
-- student job profile
-- ============================================================
-- TASK 0 (Dataset 4): Student Job Profile (≈707 rows)
-- Table: proj.student_job_profile
-- NOTE: This dataset has NO placement/outcome column.
-- We still build JSON + scores + NTILE buckets, then summarize bucket trends.
-- Primary/Secondary grouping:
--   primary   = "Profile"
--   secondary = "Skill 1"
-- ============================================================

CREATE SCHEMA IF NOT EXISTS proj;
SET search_path TO proj;

-- (Optional) confirm tables in proj
SELECT tablename
FROM pg_tables
WHERE schemaname='proj'
ORDER BY tablename;

-- Confirm exact column names + types (must match CSV headers exactly)
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema='proj'
  AND table_name='student_job_profile'
ORDER BY ordinal_position;

-- ------------------------------------------------------------
-- 1) Create JSON table
-- ------------------------------------------------------------

-- drop dependent views first (safe to re-run)
DROP VIEW IF EXISTS proj.v_task0_d4_bucket_summary;
DROP VIEW IF EXISTS proj.v_task0_d4_buckets;
DROP VIEW IF EXISTS proj.v_task0_d4_scores;

-- then drop JSON table
DROP TABLE IF EXISTS proj.json_d4_profiles;

CREATE TABLE proj.json_d4_profiles (
                                       student_key TEXT PRIMARY KEY,
                                       profile     JSONB NOT NULL
);

-- ------------------------------------------------------------
-- 2) Insert rows into JSON (no LIMIT needed for 707 rows)
-- IMPORTANT: columns with spaces MUST be referenced as t."Skill 1", etc.
-- ------------------------------------------------------------

WITH src AS (
    SELECT
        t."profile"         AS profile_name,
        t."Skill 1"         AS skill_1,
        t."Skill 2"         AS skill_2,

        -- academics
        t."dsa"             AS dsa,
        t."dbms"            AS dbms,
        t."os"              AS os,
        t."cn"              AS cn,
        t."mathmetics"      AS mathmetics,        -- keep exact header spelling

        -- skills
        t."aptitute"        AS aptitute,          -- keep exact header spelling
        t."comm"            AS comm,
        t."Problem Solving" AS problem_solving,
        t."creative"        AS creative,

        -- experience
        t."hackathons"      AS hackathons,

        to_jsonb(t)         AS raw_row
    FROM proj.student_job_profile t
),
     numbered AS (
         SELECT
             *,
             row_number() OVER (
                 ORDER BY COALESCE(profile_name::text,''), COALESCE(skill_1::text,'')
                 ) AS rn
         FROM src
     )
INSERT INTO proj.json_d4_profiles(student_key, profile)
SELECT
    'd4_' || rn AS student_key,
    jsonb_build_object(
            'source',      'student_job_profile',
            'student_key', 'd4_' || rn,

            'major_group', jsonb_build_object(
                    'primary',   profile_name,
                    'secondary', skill_1
                           ),

        -- no placement label in this dataset
            'outcome', jsonb_build_object('placed', NULL),

            'academics', jsonb_build_object(
                    'dsa',        dsa,
                    'dbms',       dbms,
                    'os',         os,
                    'cn',         cn,
                    'mathmetics', mathmetics
                         ),

            'skills', jsonb_build_object(
                    'aptitute',        aptitute,
                    'comm',            comm,
                    'problem_solving', problem_solving,
                    'creative',        creative,
                    'skill_1',         skill_1,
                    'skill_2',         skill_2
                      ),

            'experience', jsonb_build_object(
                    'hackathons', hackathons
                          ),

            'raw', raw_row
    )
FROM numbered;

-- Verify JSON created
SELECT student_key, profile
FROM proj.json_d4_profiles
LIMIT 2;

-- ------------------------------------------------------------
-- 3) Scores view (extract JSON fields + compute 3 scores)
-- ------------------------------------------------------------

CREATE OR REPLACE VIEW proj.v_task0_d4_scores AS
SELECT
    student_key,
    profile->'major_group'->>'primary'   AS major_group_primary,
    profile->'major_group'->>'secondary' AS major_group_secondary,

    -- academics (cast to numbers; missing -> 0)
    COALESCE((profile->'academics'->>'dsa')::double precision, 0)        AS dsa,
    COALESCE((profile->'academics'->>'dbms')::double precision, 0)       AS dbms,
    COALESCE((profile->'academics'->>'os')::double precision, 0)         AS os,
    COALESCE((profile->'academics'->>'cn')::double precision, 0)         AS cn,
    COALESCE((profile->'academics'->>'mathmetics')::double precision, 0) AS mathmetics,

    -- skills
    COALESCE((profile->'skills'->>'aptitute')::double precision, 0)        AS aptitute,
    COALESCE((profile->'skills'->>'comm')::double precision, 0)            AS comm,
    COALESCE((profile->'skills'->>'problem_solving')::double precision, 0) AS problem_solving,
    COALESCE((profile->'skills'->>'creative')::double precision, 0)        AS creative,

    -- experience
    COALESCE((profile->'experience'->>'hackathons')::double precision, 0)  AS hackathons,

    -- Scores (NTILE will bucket, so exact scaling is fine)
    (
        0.30 * COALESCE((profile->'academics'->>'dsa')::double precision, 0) +
        0.20 * COALESCE((profile->'academics'->>'os')::double precision, 0) +
        0.20 * COALESCE((profile->'academics'->>'dbms')::double precision, 0) +
        0.15 * COALESCE((profile->'academics'->>'cn')::double precision, 0) +
        0.15 * COALESCE((profile->'academics'->>'mathmetics')::double precision, 0)
        ) AS academic_score,

    (
        0.30 * COALESCE((profile->'skills'->>'problem_solving')::double precision, 0) +
        0.25 * COALESCE((profile->'skills'->>'comm')::double precision, 0) +
        0.25 * COALESCE((profile->'skills'->>'aptitute')::double precision, 0) +
        0.20 * COALESCE((profile->'skills'->>'creative')::double precision, 0)
        ) AS skill_score,

    (
        1.00 * COALESCE((profile->'experience'->>'hackathons')::double precision, 0)
        ) AS experience_score

FROM proj.json_d4_profiles;

SELECT * FROM proj.v_task0_d4_scores LIMIT 5;

-- ------------------------------------------------------------
-- 4) Buckets (NTILE 3-way)
-- ------------------------------------------------------------

CREATE OR REPLACE VIEW proj.v_task0_d4_buckets AS
SELECT
    *,
    NTILE(3) OVER (ORDER BY academic_score)   AS acad_bucket,
    NTILE(3) OVER (ORDER BY skill_score)      AS skill_bucket,
    NTILE(3) OVER (ORDER BY experience_score) AS exp_bucket
FROM proj.v_task0_d4_scores;

SELECT student_key, major_group_primary, acad_bucket, skill_bucket, exp_bucket
FROM proj.v_task0_d4_buckets
LIMIT 10;

-- ------------------------------------------------------------
-- 5) No placement label → summary instead of lifts
-- ------------------------------------------------------------

CREATE OR REPLACE VIEW proj.v_task0_d4_bucket_summary AS
SELECT
    major_group_primary AS profile_group,
    COUNT(*) AS n_students,
    AVG(acad_bucket)::numeric(10,2)  AS avg_acad_bucket,
    AVG(skill_bucket)::numeric(10,2) AS avg_skill_bucket,
    AVG(exp_bucket)::numeric(10,2)   AS avg_exp_bucket
FROM proj.v_task0_d4_buckets
GROUP BY major_group_primary
ORDER BY n_students DESC;

SELECT * FROM proj.v_task0_d4_bucket_summary;

-- ------------------------------------------------------------
-- 6) Export helpers (DataGrip: run + Export as JSON)
-- ------------------------------------------------------------

SELECT COUNT(*) FROM proj.json_d4_profiles;

-- export this result set as JSON (you can LIMIT if you want)
SELECT profile FROM proj.json_d4_profiles;