DROP VIEW IF EXISTS proj.v_54k_master;

DROP TABLE IF EXISTS proj.json_54k_profiles;
DROP TABLE IF EXISTS proj."54K_01_people";
DROP TABLE IF EXISTS proj."54K_02_abilities";
DROP TABLE IF EXISTS proj."54K_03_education";
DROP TABLE IF EXISTS proj."54K_04_experience";
DROP TABLE IF EXISTS proj."54K_05_person_skills";
DROP TABLE IF EXISTS proj."54K_06_skills";

CREATE SCHEMA IF NOT EXISTS proj;
SET search_path TO proj;

CREATE TABLE IF NOT EXISTS proj.master_profiles (
                                                    global_key TEXT PRIMARY KEY,
                                                    profile    JSONB NOT NULL
);

TRUNCATE TABLE proj.master_profiles;

-- D1
INSERT INTO proj.master_profiles(global_key, profile)
SELECT (profile->>'source') || ':' || (profile->>'student_key') AS global_key,
       profile
FROM proj.json_d1_profiles;

-- D2
INSERT INTO proj.master_profiles(global_key, profile)
SELECT (profile->>'source') || ':' || (profile->>'student_key') AS global_key,
       profile
FROM proj.json_d2_profiles;

-- D3
INSERT INTO proj.master_profiles(global_key, profile)
SELECT (profile->>'source') || ':' || (profile->>'student_key') AS global_key,
       profile
FROM proj.json_d3_profiles;

-- D4
INSERT INTO proj.master_profiles(global_key, profile)
SELECT (profile->>'source') || ':' || (profile->>'student_key') AS global_key,
       profile
FROM proj.json_d4_profiles;

-- sanity checks
SELECT COUNT(*) FROM proj.master_profiles;

SELECT profile->>'source' AS source, COUNT(*)
FROM proj.master_profiles
GROUP BY 1
ORDER BY 2 DESC;


CREATE OR REPLACE VIEW proj.v_task0_master_labeled AS
SELECT
    global_key,
    profile,
    profile->>'source' AS source,

    -- major string: works for both styles
    COALESCE(profile->>'major_group',
             profile->'major_group'->>'primary') AS major_group,

    (profile->'outcome'->>'placed')::int AS placed01,

    -- pull precomputed scores if present; else null
    NULLIF(profile->>'academic_score','')::double precision AS academic_score,
    NULLIF(profile->>'skill_score','')::double precision    AS skill_score,
    NULLIF(profile->>'experience_score','')::double precision AS experience_score

FROM proj.master_profiles
WHERE profile->'outcome'->>'placed' IS NOT NULL;

CREATE OR REPLACE VIEW proj.v_task0_master_scores AS
SELECT
    CASE
        WHEN student_key LIKE 'd1_%' THEN 'D1'
        WHEN student_key LIKE 'd2_%' THEN 'D2'
        WHEN student_key LIKE 'd3_%' THEN 'D3'
        WHEN student_key LIKE 'd4_%' THEN 'D4'
        ELSE 'UNK'
        END AS dataset,
    student_key AS key,
    major AS major_group,
    placed AS placed01,
    academic_score,
    skill_score,
    experience_score
FROM proj.postgres_public_task0_final_output
WHERE placed IS NOT NULL
  AND academic_score IS NOT NULL
  AND skill_score IS NOT NULL
  AND experience_score IS NOT NULL;


CREATE OR REPLACE VIEW proj.v_task0_master_buckets AS
SELECT
    *,
    NTILE(3) OVER (ORDER BY academic_score)   AS acad_bucket,
    NTILE(3) OVER (ORDER BY skill_score)      AS skill_bucket,
    NTILE(3) OVER (ORDER BY experience_score) AS exp_bucket
FROM proj.v_task0_master_scores;


CREATE OR REPLACE VIEW proj.v_task0_master_lifts AS
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
    FROM proj.v_task0_master_buckets
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
WHERE n_students >= 20
ORDER BY n_students DESC;

SELECT * FROM proj.v_task0_master_lifts;

SELECT profile FROM proj.master_profiles;
