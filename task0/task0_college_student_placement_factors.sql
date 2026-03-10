CREATE SCHEMA IF NOT EXISTS proj;
SET search_path TO proj;

-- giving us the datasets -- like what we have in proj
SELECT tablename
FROM pg_tables
WHERE schemaname='proj'
ORDER BY tablename;

-- college_student_placement_factors
-- column names and their data types
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema='proj'
  AND table_name='college_student_placement_factors'
ORDER BY ordinal_position;

-- JSON
DROP TABLE IF EXISTS proj.json_d1_profiles;

CREATE TABLE proj.json_d1_profiles (
                                       student_key TEXT PRIMARY KEY,
                                       profile JSONB NOT NULL
);

INSERT INTO proj.json_d1_profiles(student_key, profile)
SELECT
    'd1_' || row_number() OVER (ORDER BY college_id) AS student_key,
    jsonb_build_object(
            'source', 'college_student_placement_factors',
            'student_key', 'd1_' || row_number() OVER (ORDER BY college_id),
            'major_group', college_id,                 -- using college_id as group
            'outcome', jsonb_build_object(
                    'placed', CASE
                                  WHEN lower(placement) IN ('placed','yes','y','1','true') THEN 1
                                  ELSE 0
                END
                       ),
            'academics', jsonb_build_object(
                    'cgpa', cgpa,
                    'prev_sem_result', prev_sem_result,
                    'academic_performance', academic_performance,
                    'iq', iq
                         ),
            'skills', jsonb_build_object(
                    'communication_skills', communication_skills,
                    'extra_curricular_score', extra_curricular_score
                      ),
            'experience', jsonb_build_object(
                    'internship_experience', internship_experience,
                    'projects_completed', projects_completed
                          ),
            'raw', to_jsonb(t)
    )
FROM proj.college_student_placement_factors t;

-- verify
SELECT student_key, profile
FROM proj.json_d1_profiles
LIMIT 2;

-- buckets
CREATE OR REPLACE VIEW proj.v_task0_d1_scores AS
SELECT
    student_key,
    profile->>'major_group' AS major_group,
    (profile->'outcome'->>'placed')::int AS placed01,

    (profile->'academics'->>'cgpa')::double precision AS cgpa,
    (profile->'academics'->>'prev_sem_result')::double precision AS prev_sem_result,
    (profile->'academics'->>'academic_performance')::double precision AS academic_performance,
    (profile->'academics'->>'iq')::double precision AS iq,

    (profile->'skills'->>'communication_skills')::double precision AS communication_skills,
    (profile->'skills'->>'extra_curricular_score')::double precision AS extra_curricular_score,

    CASE
        WHEN lower(profile->'experience'->>'internship_experience') IN ('yes','y','true','1') THEN 1
        ELSE 0
        END AS internship01,

    (profile->'experience'->>'projects_completed')::double precision AS projects_completed,

    (0.35* (profile->'academics'->>'cgpa')::double precision
        + 0.25* (profile->'academics'->>'prev_sem_result')::double precision
        + 0.25* (profile->'academics'->>'academic_performance')::double precision
        + 0.15* (profile->'academics'->>'iq')::double precision
        ) AS academic_score,

    (0.7* (profile->'skills'->>'communication_skills')::double precision
        + 0.3* (profile->'skills'->>'extra_curricular_score')::double precision
        ) AS skill_score,

    (0.6* (CASE
               WHEN lower(profile->'experience'->>'internship_experience') IN ('yes','y','true','1') THEN 1
               ELSE 0
        END)
        + 0.4* (profile->'experience'->>'projects_completed')::double precision
        ) AS experience_score

FROM proj.json_d1_profiles;

SELECT * FROM proj.v_task0_d1_scores LIMIT 5;

-- NTILE
CREATE OR REPLACE VIEW proj.v_task0_d1_buckets AS
SELECT
    *,
    NTILE(3) OVER (ORDER BY academic_score) AS acad_bucket,
    NTILE(3) OVER (ORDER BY skill_score) AS skill_bucket,
    NTILE(3) OVER (ORDER BY experience_score) AS exp_bucket
FROM proj.v_task0_d1_scores;


-- verify
SELECT student_key, major_group, placed01, acad_bucket, skill_bucket, exp_bucket
FROM proj.v_task0_d1_buckets
LIMIT 10;


-- lift
CREATE OR REPLACE VIEW proj.v_task0_d1_lifts AS
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
    FROM proj.v_task0_d1_buckets
    GROUP BY major_group
)
SELECT
    major_group,
    n_students,
    ROUND(pr_acad_high - pr_acad_low, 3) AS acad_lift,
    ROUND(pr_skill_high - pr_skill_low, 3) AS skill_lift,
    ROUND(pr_exp_high - pr_exp_low, 3) AS exp_lift,
    CASE
        WHEN (pr_acad_high - pr_acad_low) >= (pr_skill_high - pr_skill_low)
            AND (pr_acad_high - pr_acad_low) >= (pr_exp_high - pr_exp_low) THEN 'Academics'
        WHEN (pr_skill_high - pr_skill_low) >= (pr_exp_high - pr_exp_low) THEN 'Skills'
        ELSE 'Experience'
        END AS most_important
FROM rates
WHERE n_students >= 20
ORDER BY n_students DESC;

-- results
SELECT * FROM proj.v_task0_d1_lifts;


-- JSON view
SELECT student_key, profile
FROM proj.json_d1_profiles;

-- how many we have
SELECT COUNT(*) FROM proj.college_student_placement_factors;

-- if we need it to be 1 file
SELECT profile FROM proj.json_d1_profiles LIMIT 2000;