-- ============================================================================
-- Development-track slice of the real-estate job pool
-- ============================================================================
-- Storage is industry-driven: the jobs table holds EVERY posting from
-- companies whose industry is real estate / commercial real estate (see
-- TARGET_INDUSTRIES in etl/sync.py). This view slices that pool down to
-- open development-track roles.
-- Run with: psql "$DATABASE_URL" -f queries/target_roles.sql
-- or copy individual queries into any SQL client / notebook.
--
-- Note: the 'minimal' dataset variant does not include salary or seniority.
-- city is enriched from the 'full' variant for matched rows only
-- (etl.sync.add_city_from_full); upstream values can be dirty.

-- 1. A reusable view: open development-track jobs at real-estate companies.
--    ILIKE is case-insensitive. Mirror TARGET_TITLE_PATTERNS in etl/sync.py.
--    DROP + CREATE (not CREATE OR REPLACE) because Postgres won't let
--    OR REPLACE change an existing view's column list mid-order.
DROP VIEW IF EXISTS target_role_jobs;
CREATE VIEW target_role_jobs AS
SELECT
    j.id,
    j.title,
    c.name        AS company_name,
    c.industry,
    j.department,
    j.country,
    j.city,
    j.remote,
    j.employment_type,
    j.workplace_type,
    j.apply_url,
    j.posted_at,
    j.status
FROM jobs j
JOIN companies c ON c.id = j.company_id
WHERE j.status IS DISTINCT FROM 'closed'
  AND (
        j.title ILIKE '%development%'
     OR j.title ILIKE '%acquisition%'
     OR j.title ILIKE '%asset manag%'
     OR j.title ILIKE '%investment%'
     OR j.title ILIKE '%real estate%'
  );

-- 2. Newest matches first — this is what you'd check daily.
SELECT *
FROM target_role_jobs
ORDER BY posted_at DESC NULLS LAST
LIMIT 50;

-- 3. Count of open target-role jobs by company, most active hirers first.
SELECT company_name, COUNT(*) AS open_roles
FROM target_role_jobs
GROUP BY company_name
ORDER BY open_roles DESC
LIMIT 25;

-- 4. Remote-only matches.
SELECT *
FROM target_role_jobs
WHERE remote IS TRUE
ORDER BY posted_at DESC NULLS LAST;

-- 5. Matches by workplace_type (remote / onsite / hybrid / tbc, as reported
--    by each source ATS — not fully standardized, worth eyeballing distinct
--    values first with: SELECT DISTINCT workplace_type FROM jobs;).
SELECT
    workplace_type,
    COUNT(*) AS n
FROM target_role_jobs
GROUP BY workplace_type
ORDER BY n DESC;

-- 6. Jobs newly matched since your last check (swap in your last-run
--    timestamp, or use the synced_at column populated by etl/sync.py).
-- SELECT *
-- FROM target_role_jobs t
-- JOIN jobs j ON j.id = t.id
-- WHERE j.synced_at > NOW() - INTERVAL '1 day'
-- ORDER BY j.synced_at DESC;
