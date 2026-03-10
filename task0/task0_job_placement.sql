
--job placement
-- ============================================================
-- TASK 0 (Dataset 2): Job Placement Dataset
-- Table assumed: proj.job_placement
-- Output JSON table: proj.json_d2_profiles
-- ============================================================

CREATE SCHEMA IF NOT EXISTS proj;
SET search_path TO proj;

-- 0) sanity: see columns
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema='proj'
  AND table_name='job_placement'
ORDER BY ordinal_position;

-- ============================================================
-- 1) JSON table
-- ============================================================

DROP TABLE IF EXISTS proj.json_d2_profiles;

CREATE TABLE proj.json_d2_profiles (
                                       student_key TEXT PRIMARY KEY,
                                       profile     JSONB NOT NULL
);

-- No student_id in this dataset, so we generate a stable key with row_number()
INSERT INTO proj.json_d2_profiles(student_key, profile)
SELECT
    'd2_' || row_number() OVER (ORDER BY undergrad_degree, degree_percentage, mba_percent, ssc_percentage, hsc_percentage) AS student_key,
    jsonb_build_object(
            'source', 'job_placement',
            'student_key', 'd2_' || row_number() OVER (ORDER BY undergrad_degree, degree_percentage, mba_percent, ssc_percentage, hsc_percentage),

        -- major grouping: undergrad degree is the closest thing to "major"
            'major_group', undergrad_degree,

            'outcome', jsonb_build_object(
                    'placed',
                    CASE
                        WHEN lower(status) IN ('placed','yes','y','1','true') THEN 1
                        ELSE 0
                        END,
                    'status', status
                       ),

            'academics', jsonb_build_object(
                    'ssc_percentage', ssc_percentage,
                    'hsc_percentage', hsc_percentage,
                    'degree_percentage', degree_percentage,
                    'mba_percent', mba_percent,
                    'ssc_board', ssc_board,
                    'hsc_board', hsc_board,
                    'hsc_subject', hsc_subject
                         ),

        -- skills proxy: employability test score
            'skills', jsonb_build_object(
                    'emp_test_percentage', emp_test_percentage
                      ),

            'experience', jsonb_build_object(
                    'work_experience', work_experience,
                    'specialisation', specialisation
                          ),

            'demographics', jsonb_build_object(
                    'gender', gender
                            ),

            'raw', to_jsonb(t)
    )
FROM proj.job_placement t;

-- verify
SELECT student_key, profile
FROM proj.json_d2_profiles
LIMIT 2;

-- ============================================================
-- 2) Scores view (extract JSON fields + compute 3 scores)
-- ============================================================

CREATE OR REPLACE VIEW proj.v_task0_d2_scores AS
SELECT
    student_key,
    profile->>'major_group' AS major_group,
    (profile->'outcome'->>'placed')::int AS placed01,

    -- academics
    COALESCE((profile->'academics'->>'ssc_percentage')::double precision, 0) AS ssc_percentage,
    COALESCE((profile->'academics'->>'hsc_percentage')::double precision, 0) AS hsc_percentage,
    COALESCE((profile->'academics'->>'degree_percentage')::double precision, 0) AS degree_percentage,
    COALESCE((profile->'academics'->>'mba_percent')::double precision, 0) AS mba_percent,

    -- skills
    COALESCE((profile->'skills'->>'emp_test_percentage')::double precision, 0) AS emp_test_percentage,

    -- experience (binary)
    CASE
        WHEN lower(COALESCE(profile->'experience'->>'work_experience','')) IN ('yes','y','true','1') THEN 1
        ELSE 0
        END AS workexp01,

    -- -------- scores --------
    -- Academics: average of percent scores
    (
        0.25 * COALESCE((profile->'academics'->>'ssc_percentage')::double precision, 0)
            + 0.25 * COALESCE((profile->'academics'->>'hsc_percentage')::double precision, 0)
            + 0.25 * COALESCE((profile->'academics'->>'degree_percentage')::double precision, 0)
            + 0.25 * COALESCE((profile->'academics'->>'mba_percent')::double precision, 0)
        ) AS academic_score,

    -- Skills: employability test
    (
        COALESCE((profile->'skills'->>'emp_test_percentage')::double precision, 0)
        ) AS skill_score,

    -- Experience: work_experience yes/no
    (
        CASE
            WHEN lower(COALESCE(profile->'experience'->>'work_experience','')) IN ('yes','y','true','1') THEN 1
            ELSE 0
            END
        ) AS experience_score

FROM proj.json_d2_profiles;

SELECT * FROM proj.v_task0_d2_scores LIMIT 5;

-- ============================================================
-- 3) Buckets (NTILE)
-- ============================================================

CREATE OR REPLACE VIEW proj.v_task0_d2_buckets AS
SELECT
    *,
    NTILE(3) OVER (ORDER BY academic_score)   AS acad_bucket,
    NTILE(3) OVER (ORDER BY skill_score)      AS skill_bucket,
    NTILE(3) OVER (ORDER BY experience_score) AS exp_bucket
FROM proj.v_task0_d2_scores;

SELECT student_key, major_group, placed01, acad_bucket, skill_bucket, exp_bucket
FROM proj.v_task0_d2_buckets
LIMIT 10;

-- ============================================================
-- 4) Lifts per major_group
-- ============================================================

CREATE OR REPLACE VIEW proj.v_task0_d2_lifts AS
WITH rates AS (
    SELECT
        major_group,
        COUNT(*) AS n_students,

        AVG(placed01::numeric) FILTER (WHERE acad_bucket=3) AS pr_acad_high,
        AVG(placed01::numeric) FILTER (WHERE acad_bucket=1) AS pr_acad_low,

        AVG(placed01::numeric) FILTER (WHERE skill_bucket=3) AS pr_skill_high,
        AVG(placed01::numeric) FILTER (WHERE skill_bucket=1) AS pr_skill_low,

        AVG(placed01::numeric) FILTER (WHERE exp_bucket=3) AS pr_exp_high,
        AVG(placed01::numeric) FILTER (WHERE exp_bucket=1) AS pr_exp_low
    FROM proj.v_task0_d2_buckets
    GROUP BY major_group
)
SELECT
    major_group,
    n_students,
    ROUND(pr_acad_high - pr_acad_low, 3)  AS acad_lift,
    ROUND(pr_skill_high - pr_skill_low, 3) AS skill_lift,
    ROUND(pr_exp_high - pr_exp_low, 3)    AS exp_lift,
    CASE
        WHEN (pr_acad_high - pr_acad_low) >= (pr_skill_high - pr_skill_low)
            AND (pr_acad_high - pr_acad_low) >= (pr_exp_high - pr_exp_low) THEN 'Academics'
        WHEN (pr_skill_high - pr_skill_low) >= (pr_exp_high - pr_exp_low) THEN 'Skills'
        ELSE 'Experience'
        END AS most_important
FROM rates
WHERE n_students >= 10
ORDER BY n_students DESC;

SELECT * FROM proj.v_task0_d2_lifts;

-- ============================================================
-- 5) JSON export helpers
-- ============================================================

SELECT COUNT(*) FROM proj.json_d2_profiles;

-- For exporting in DataGrip (Export Data -> JSON):
SELECT profile
FROM proj.json_d2_profiles;




