require("dotenv").config();
const express = require("express");
const mysql = require("mysql2/promise");
const cors = require("cors");
const helmet = require("helmet");
const compression = require("compression");
const rateLimit = require("express-rate-limit");

const REQUIRED_ENV = ["DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME"];
const missingEnv = REQUIRED_ENV.filter((key) => !process.env[key]);
if (missingEnv.length) {
  console.error(`Missing required environment variables: ${missingEnv.join(", ")}`);
  process.exit(1);
}

const PORT = Number(process.env.PORT) || 5000;

// ---------------------------------------------------------------------------
// Domain constants
// ---------------------------------------------------------------------------

// Whitelisted reservation-category columns. Never build this list from user
// input — it is interpolated into SQL, so it must always come from a fixed set.
const CATEGORY_COLUMNS = [
  "oc_boys", "oc_girls", "sc_boys", "sc_girls", "st_boys", "st_girls",
  "bca_boys", "bca_girls", "bcb_boys", "bcb_girls", "bcc_boys", "bcc_girls",
  "bcd_boys", "bcd_girls", "bce_boys", "bce_girls", "oc_ews_boys", "oc_ews_girls"
];

const SORT_FIELDS = {
  fee: "college_fee",
  name: "institution_name",
  margin: "__margin__" // computed column, handled specially
};

const MAX_PAGE_SIZE = 100;
const DEFAULT_PAGE_SIZE = 25;
const MAX_RANK = 1000000;
const MAX_SEARCH_LEN = 80;
const META_CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes

// ---------------------------------------------------------------------------
// Database
// ---------------------------------------------------------------------------

const db = mysql.createPool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT ? Number(process.env.DB_PORT) : 3306,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  connectionLimit: Number(process.env.DB_CONNECTION_LIMIT) || 15,
  waitForConnections: true,
  queueLimit: 0,
  ssl: process.env.DB_SSL_REJECT === "false" ? false : { rejectUnauthorized: true }
});

async function safeQuery(sql, params = []) {
  const [rows] = await db.query(sql, params);
  return rows;
}

// ---------------------------------------------------------------------------
// Validation helpers
// ---------------------------------------------------------------------------

function cleanString(value, maxLen) {
  if (typeof value !== "string") return "";
  const trimmed = value.trim();
  if (!trimmed) return "";
  return trimmed.slice(0, maxLen);
}

function parsePositiveInt(value, max) {
  if (value === undefined || value === null || value === "") return null;
  const n = Number(value);
  if (!Number.isInteger(n) || n <= 0 || n > max) return null;
  return n;
}

function parsePage(value) {
  const n = Number(value);
  if (!Number.isInteger(n) || n < 1) return 1;
  return n;
}

function parsePageSize(value) {
  const n = Number(value);
  if (!Number.isInteger(n) || n < 1) return DEFAULT_PAGE_SIZE;
  return Math.min(n, MAX_PAGE_SIZE);
}

/**
 * Builds a reusable WHERE clause + params from the shared filter fields.
 * Keeping this in one place means the count query and the data query can
 * never drift out of sync with each other.
 */
function buildFilterClause(query) {
  const clauses = ["1=1"];
  const params = [];

  const district = cleanString(query.district, 100);
  const branchCode = cleanString(query.branch_code, 50);
  const type = cleanString(query.type, 50);
  const affiliation = cleanString(query.affiliation, 100);
  const search = cleanString(query.search, MAX_SEARCH_LEN);

  if (district) { clauses.push("district = ?"); params.push(district); }
  if (branchCode) { clauses.push("branch_code = ?"); params.push(branchCode); }
  if (type) { clauses.push("type = ?"); params.push(type); }
  if (affiliation) { clauses.push("affiliation = ?"); params.push(affiliation); }
  if (search.length >= 2) { clauses.push("institution_name LIKE ?"); params.push(`%${search}%`); }

  return { where: clauses.join(" AND "), params };
}

// ---------------------------------------------------------------------------
// Lightweight in-memory cache for filter/meta lookups (district lists, etc.)
// These change rarely, so hitting the DB on every page load is wasted work.
// ---------------------------------------------------------------------------

let metaCache = null;
let metaCacheAt = 0;

async function getMeta() {
  const now = Date.now();
  if (metaCache && now - metaCacheAt < META_CACHE_TTL_MS) {
    return metaCache;
  }

  const [districts, branches, types, affiliations, counters] = await Promise.all([
    safeQuery("SELECT DISTINCT district FROM colleges WHERE district IS NOT NULL AND district <> '' ORDER BY district"),
    safeQuery("SELECT DISTINCT branch_code FROM colleges WHERE branch_code IS NOT NULL AND branch_code <> '' ORDER BY branch_code"),
    safeQuery("SELECT DISTINCT type FROM colleges WHERE type IS NOT NULL AND type <> '' ORDER BY type"),
    safeQuery("SELECT DISTINCT affiliation FROM colleges WHERE affiliation IS NOT NULL AND affiliation <> '' ORDER BY affiliation"),
    safeQuery(`
      SELECT
        COUNT(*) AS totalColleges,
        COUNT(DISTINCT district) AS totalDistricts,
        COUNT(DISTINCT branch_code) AS totalBranches,
        AVG(college_fee) AS avgFee
      FROM colleges
    `)
  ]);

  metaCache = {
    districts: districts.map((r) => r.district),
    branches: branches.map((r) => r.branch_code),
    types: types.map((r) => r.type),
    affiliations: affiliations.map((r) => r.affiliation),
    stats: {
      totalColleges: counters[0]?.totalColleges || 0,
      totalDistricts: counters[0]?.totalDistricts || 0,
      totalBranches: counters[0]?.totalBranches || 0,
      averageFee: Math.round(counters[0]?.avgFee || 0)
    }
  };
  metaCacheAt = now;
  return metaCache;
}

// ---------------------------------------------------------------------------
// App setup
// ---------------------------------------------------------------------------

const app = express();
app.disable("x-powered-by");
app.use(helmet());
app.use(compression());
app.use(cors());
app.use(express.json());

const apiLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 120,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: "Too many requests. Please slow down and try again shortly." }
});
app.use("/api", apiLimiter);

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

// Single combined endpoint: dropdown options + summary stats, cached.
// Replaces four separate round trips the original client made on every load.
app.get("/api/meta", async (req, res, next) => {
  try {
    const meta = await getMeta();
    res.json(meta);
  } catch (err) {
    next(err);
  }
});

app.get("/api/colleges", async (req, res, next) => {
  try {
    const { category } = req.query;

    if (!CATEGORY_COLUMNS.includes(category)) {
      return res.status(400).json({ error: "Please choose a valid reservation category." });
    }

    const rank = parsePositiveInt(req.query.rank, MAX_RANK);
    if (rank === null) {
      return res.status(400).json({ error: `Please enter a rank between 1 and ${MAX_RANK.toLocaleString()}.` });
    }

    const page = parsePage(req.query.page);
    const pageSize = parsePageSize(req.query.pageSize);
    const offset = (page - 1) * pageSize;

    const sortKey = SORT_FIELDS[req.query.sort] ? req.query.sort : "margin";
    const order = req.query.order === "desc" ? "DESC" : "ASC";

    const { where, params } = buildFilterClause(req.query);

    // category column is validated against a fixed whitelist above, so it is
    // safe to interpolate directly; the rank value itself stays parameterized.
    const marginExpr = `(${category} - ?)`;
    const fullWhere = `${where} AND ${category} >= ? AND ${category} IS NOT NULL`;

    const orderColumn = sortKey === "margin" ? "margin" : SORT_FIELDS[sortKey];

    const countSql = `SELECT COUNT(*) AS total FROM colleges WHERE ${fullWhere}`;
    const countParams = [...params, rank];

    const dataSql = `
      SELECT *, ${marginExpr} AS margin
      FROM colleges
      WHERE ${fullWhere}
      ORDER BY ${orderColumn} ${order}
      LIMIT ? OFFSET ?
    `;
    const dataParams = [rank, ...params, rank, pageSize, offset];

    const [countRows, rows] = await Promise.all([
      safeQuery(countSql, countParams),
      safeQuery(dataSql, dataParams)
    ]);

    const total = countRows[0]?.total || 0;

    res.json({
      results: rows,
      pagination: {
        page,
        pageSize,
        total,
        totalPages: Math.max(1, Math.ceil(total / pageSize))
      }
    });
  } catch (err) {
    next(err);
  }
});

// Used by the "shortlist" panel to fetch full rows for a specific set of
// institution codes, regardless of the current filter/pagination state.
app.get("/api/colleges/lookup", async (req, res, next) => {
  try {
    const codesRaw = cleanString(req.query.codes, 2000);
    if (!codesRaw) return res.json({ results: [] });

    const codes = [...new Set(
      codesRaw.split(",").map((c) => c.trim()).filter(Boolean)
    )].slice(0, 50);

    if (!codes.length) return res.json({ results: [] });

    const placeholders = codes.map(() => "?").join(",");
    const rows = await safeQuery(
      `SELECT * FROM colleges WHERE inst_code IN (${placeholders})`,
      codes
    );
    res.json({ results: rows });
  } catch (err) {
    next(err);
  }
});

app.get("/health", (req, res) => res.json({ status: "ok" }));

app.use((req, res) => {
  res.status(404).json({ error: "That endpoint does not exist." });
});

// eslint-disable-next-line no-unused-vars
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({ error: "Something went wrong while processing your request." });
});

const server = app.listen(PORT, () => console.log(`thepha API listening on port ${PORT}`));

function shutdown() {
  console.log("Shutting down gracefully...");
  server.close(async () => {
    await db.end();
    process.exit(0);
  });
}
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
