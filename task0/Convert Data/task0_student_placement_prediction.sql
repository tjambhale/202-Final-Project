
-- student placement prediction

-- ============================================================
-- TASK 0 (Dataset 3): Student Placement Prediction (LIMIT 2000)
-- Primary/Secondary major grouping:
--   primary   = branch
--   secondary = college_tier
-- ============================================================

CREATE SCHEMA IF NOT EXISTS proj;
SET search_path TO proj;

-- Check columns for dataset 3 table
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema='proj'
  AND table_name='student_placement_prediction'
ORDER BY ordinal_position;

-- ============================================================
-- 1) Create JSON table (LIMIT 2000 rows)
-- ============================================================

DROP TABLE IF EXISTS proj.json_d3_profiles;

CREATE TABLE proj.json_d3_profiles (
                                       student_key TEXT PRIMARY KEY,
                                       profile     JSONB NOT NULL
);

-- Insert only first 2000 rows (ordered by student_id)
WITH base AS (
    SELECT
        *,
        row_number() OVER (ORDER BY student_id) AS rn
    FROM proj.student_placement_prediction
)
INSERT INTO proj.json_d3_profiles(student_key, profile)
SELECT
    'd3_' || student_id::text AS student_key,
    jsonb_build_object(
            'source', 'student_placement_prediction',
            'student_key', 'd3_' || student_id::text,

        -- primary/secondary grouping
            'major_group', jsonb_build_object(
                    'primary', branch,
                    'secondary', college_tier
                           ),

            'outcome', jsonb_build_object(
                    'placed', CASE
                                  WHEN lower(placement_status) IN ('placed','yes','y','1','true') THEN 1
                                  ELSE 0
                END,
                    'placement_status', placement_status,
                    'salary_package_lpa', salary_package_lpa
                       ),

            'academics', jsonb_build_object(
                    'cgpa', cgpa,
                    'attendance_percentage', attendance_percentage,
                    'backlogs', backlogs,
                    'study_hours_per_day', study_hours_per_day
                         ),

            'skills', jsonb_build_object(
                    'coding_skill_score', coding_skill_score,
                    'aptitude_score', aptitude_score,
                    'communication_skill_score', communication_skill_score,
                    'logical_reasoning_score', logical_reasoning_score,
                    'mock_interview_score', mock_interview_score
                      ),

            'experience', jsonb_build_object(
                    'internships_count', internships_count,
                    'projects_count', projects_count,
                    'certifications_count', certifications_count,
                    'hackathons_participated', hackathons_participated,
                    'github_repos', github_repos,
                    'volunteer_experience', volunteer_experience,
                    'leadership_score', leadership_score,
                    'extracurricular_score', extracurricular_score
                          ),

            'demographics', jsonb_build_object(
                    'age', age,
                    'gender', gender,
                    'sleep_hours', sleep_hours
                            ),

            'raw', to_jsonb(b)
    )
FROM base b
WHERE rn <= 2000;

-- Verify JSON created
SELECT student_key, profile
FROM proj.json_d3_profiles
LIMIT 2;

-- ============================================================
-- 2) Scores view (extract JSON fields + compute 3 scores)
-- ============================================================

CREATE OR REPLACE VIEW proj.v_task0_d3_scores AS
SELECT
    student_key,

    -- use PRIMARY as the grouping key for Task 0 lift logic
    profile->'major_group'->>'primary'   AS major_group_primary,
    profile->'major_group'->>'secondary' AS major_group_secondary,

    (profile->'outcome'->>'placed')::int AS placed01,

    -- academics
    COALESCE((profile->'academics'->>'cgpa')::double precision, 0) AS cgpa,
    COALESCE((profile->'academics'->>'attendance_percentage')::double precision, 0) AS attendance_percentage,
    COALESCE((profile->'academics'->>'backlogs')::double precision, 0) AS backlogs,
    COALESCE((profile->'academics'->>'study_hours_per_day')::double precision, 0) AS study_hours_per_day,

    -- skills
    COALESCE((profile->'skills'->>'coding_skill_score')::double precision, 0) AS coding_skill_score,
    COALESCE((profile->'skills'->>'aptitude_score')::double precision, 0) AS aptitude_score,
    COALESCE((profile->'skills'->>'communication_skill_score')::double precision, 0) AS communication_skill_score,
    COALESCE((profile->'skills'->>'logical_reasoning_score')::double precision, 0) AS logical_reasoning_score,
    COALESCE((profile->'skills'->>'mock_interview_score')::double precision, 0) AS mock_interview_score,

    -- experience
    COALESCE((profile->'experience'->>'internships_count')::double precision, 0) AS internships_count,
    COALESCE((profile->'experience'->>'projects_count')::double precision, 0) AS projects_count,
    COALESCE((profile->'experience'->>'certifications_count')::double precision, 0) AS certifications_count,
    COALESCE((profile->'experience'->>'hackathons_participated')::double precision, 0) AS hackathons_participated,
    COALESCE((profile->'experience'->>'github_repos')::double precision, 0) AS github_repos,
    COALESCE((profile->'experience'->>'volunteer_experience')::double precision, 0) AS volunteer_experience,
    COALESCE((profile->'experience'->>'leadership_score')::double precision, 0) AS leadership_score,
    COALESCE((profile->'experience'->>'extracurricular_score')::double precision, 0) AS extracurricular_score,

    -- ------------------------------------------------------------
    -- Scores (no perfect “units” needed because we bucket with NTILE)
    -- ------------------------------------------------------------

    -- academics_score: reward cgpa + attendance + study, penalize backlogs
    (
        0.50 * COALESCE((profile->'academics'->>'cgpa')::double precision, 0)
            + 0.02 * COALESCE((profile->'academics'->>'attendance_percentage')::double precision, 0)
            + 0.20 * COALESCE((profile->'academics'->>'study_hours_per_day')::double precision, 0)
            - 0.60 * COALESCE((profile->'academics'->>'backlogs')::double precision, 0)
        ) AS academic_score,

    -- skill_score: mean of skill-type scores
    (
        0.25 * COALESCE((profile->'skills'->>'coding_skill_score')::double precision, 0)
            + 0.20 * COALESCE((profile->'skills'->>'aptitude_score')::double precision, 0)
            + 0.20 * COALESCE((profile->'skills'->>'logical_reasoning_score')::double precision, 0)
            + 0.20 * COALESCE((profile->'skills'->>'communication_skill_score')::double precision, 0)
            + 0.15 * COALESCE((profile->'skills'->>'mock_interview_score')::double precision, 0)
        ) AS skill_score,

    -- experience_score: “counts + leadership/extracurricular”
    (
        0.40 * COALESCE((profile->'experience'->>'internships_count')::double precision, 0)
            + 0.35 * COALESCE((profile->'experience'->>'projects_count')::double precision, 0)
            + 0.20 * COALESCE((profile->'experience'->>'certifications_count')::double precision, 0)
            + 0.10 * COALESCE((profile->'experience'->>'hackathons_participated')::double precision, 0)
            + 0.02 * COALESCE((profile->'experience'->>'github_repos')::double precision, 0)
            + 0.05 * COALESCE((profile->'experience'->>'volunteer_experience')::double precision, 0)
            + 0.02 * COALESCE((profile->'experience'->>'leadership_score')::double precision, 0)
            + 0.02 * COALESCE((profile->'experience'->>'extracurricular_score')::double precision, 0)
        ) AS experience_score

FROM proj.json_d3_profiles;

SELECT * FROM proj.v_task0_d3_scores LIMIT 5;

-- ============================================================
-- 3) Buckets (NTILE)
-- ============================================================

CREATE OR REPLACE VIEW proj.v_task0_d3_buckets AS
SELECT
    *,
    NTILE(3) OVER (ORDER BY academic_score)   AS acad_bucket,
    NTILE(3) OVER (ORDER BY skill_score)      AS skill_bucket,
    NTILE(3) OVER (ORDER BY experience_score) AS exp_bucket
FROM proj.v_task0_d3_scores;

SELECT student_key, major_group_primary, placed01, acad_bucket, skill_bucket, exp_bucket
FROM proj.v_task0_d3_buckets
LIMIT 10;

-- ============================================================
-- 4) Lifts per PRIMARY major group
-- ============================================================

CREATE OR REPLACE VIEW proj.v_task0_d3_lifts AS
WITH rates AS (
    SELECT
        major_group_primary AS major_group,
        COUNT(*) AS n_students,

        AVG(placed01::numeric) FILTER (WHERE acad_bucket=3) AS pr_acad_high,
        AVG(placed01::numeric) FILTER (WHERE acad_bucket=1) AS pr_acad_low,

        AVG(placed01::numeric) FILTER (WHERE skill_bucket=3) AS pr_skill_high,
        AVG(placed01::numeric) FILTER (WHERE skill_bucket=1) AS pr_skill_low,

        AVG(placed01::numeric) FILTER (WHERE exp_bucket=3) AS pr_exp_high,
        AVG(placed01::numeric) FILTER (WHERE exp_bucket=1) AS pr_exp_low
    FROM proj.v_task0_d3_buckets
    GROUP BY major_group_primary
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
WHERE n_students >= 20
ORDER BY n_students DESC;

SELECT * FROM proj.v_task0_d3_lifts;

-- ============================================================
-- 5) JSON export helpers
-- ============================================================

-- confirm row count in JSON table (should be 2000)
SELECT COUNT(*) FROM proj.json_d3_profiles;

-- to export from DataGrip: run this and export result as JSON
SELECT profile FROM proj.json_d3_profiles;