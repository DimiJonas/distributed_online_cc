-- db_schema.sql
-- Applied every time the app starts. All statements are idempotent
-- (IF NOT EXISTS), so re-running this against an existing database
-- is a safe no-op and existing data is preserved.

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS cases (
    case_id       TEXT PRIMARY KEY,
    first_seen_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS constraints (
    constraint_id TEXT PRIMARY KEY,
    constraint_type TEXT NOT NULL,
    first_seen_at TEXT NOT NULL
);

-- One row per result received.
CREATE TABLE IF NOT EXISTS results (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id       TEXT NOT NULL REFERENCES cases(case_id),
    constraint_id TEXT NOT NULL REFERENCES constraints(constraint_id),
    result        TEXT NOT NULL CHECK (result IN ('Violated', 'TemporarilyViolated', 'Satisfied')),
    info          TEXT NOT NULL,
    created_at    TEXT NOT NULL
);

-- Supports "all results for a case" ordered by created_at without a full
-- table scan.
CREATE INDEX IF NOT EXISTS idx_results_case
    ON results (case_id, created_at);

-- Supports "all results for a specific case + constraint" ordered by
-- created_at.
CREATE INDEX IF NOT EXISTS idx_results_case_constraint
    ON results (case_id, constraint_id, created_at);