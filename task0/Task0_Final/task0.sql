DROP TABLE IF EXISTS task0_final_output;
DROP TABLE IF EXISTS task0_final_output CASCADE;

CREATE TABLE task0_final_output AS
WITH base AS (
    SELECT
        data->'profile'->>'student_key' AS student_key,
        data->'profile'->>'source' AS source,

        CASE
            WHEN jsonb_typeof(data->'profile'->'major_group') = 'object' THEN
                CASE
                    WHEN NULLIF(data->'profile'->'major_group'->>'primary', '') ~ '^CLG[0-9]+$' THEN NULL
                    ELSE NULLIF(data->'profile'->'major_group'->>'primary', '')
                END

            WHEN jsonb_typeof(data->'profile'->'major_group') = 'string' THEN
                CASE
                    WHEN NULLIF(data->'profile'->>'major_group', '') ~ '^CLG[0-9]+$' THEN NULL
                    ELSE NULLIF(data->'profile'->>'major_group', '')
                END

            ELSE NULL
        END AS major,

        -- academics
        COALESCE(
            (data->'profile'->'academics'->>'cgpa')::numeric,
            (data->'profile'->'raw'->>'cgpa')::numeric
        ) AS cgpa,

        COALESCE(
            (data->'profile'->'raw'->>'iq')::numeric,
            NULL
        ) AS iq,

        COALESCE(
            (data->'profile'->'raw'->>'prev_sem_result')::numeric,
            NULL
        ) AS prev_sem_result,

        COALESCE(
            (data->'profile'->'raw'->>'academic_performance')::numeric,
            NULL
        ) AS academic_performance,

        COALESCE(
            (data->'profile'->'academics'->>'attendance_percentage')::numeric,
            NULL
        ) AS attendance_percentage,

        -- skills
        COALESCE(
            (data->'profile'->'skills'->>'communication_skill_score')::numeric,
            (data->'profile'->'raw'->>'communication_skill_score')::numeric,
            (data->'profile'->'raw'->>'communication_skills')::numeric
        ) AS communication_skill_score,

        COALESCE(
            (data->'profile'->'skills'->>'coding_skill_score')::numeric,
            (data->'profile'->'raw'->>'coding_skill_score')::numeric
        ) AS coding_skill_score,

        COALESCE(
            (data->'profile'->'skills'->>'logical_reasoning_score')::numeric,
            (data->'profile'->'raw'->>'logical_reasoning_score')::numeric
        ) AS logical_reasoning_score,

        COALESCE(
            (data->'profile'->'raw'->>'extra_curricular_score')::numeric,
            (data->'profile'->'experience'->>'extracurricular_score')::numeric,
            (data->'profile'->'raw'->>'extracurricular_score')::numeric
        ) AS extracurricular_score,

        -- experience
        COALESCE(
            (data->'profile'->'experience'->>'projects_count')::numeric,
            (data->'profile'->'experience'->>'projects_completed')::numeric,
            (data->'profile'->'raw'->>'projects_count')::numeric,
            (data->'profile'->'raw'->>'projects_completed')::numeric
        ) AS projects_count,

        COALESCE(
            (data->'profile'->'experience'->>'internships_count')::numeric,
            CASE
                WHEN data->'profile'->'experience'->>'internship_experience' = 'Yes' THEN 1
                WHEN data->'profile'->'experience'->>'internship_experience' = 'No' THEN 0
                WHEN data->'profile'->'raw'->>'internship_experience' = 'Yes' THEN 1
                WHEN data->'profile'->'raw'->>'internship_experience' = 'No' THEN 0
                ELSE NULL
            END
        ) AS internships_value,

        COALESCE(
            (data->'profile'->'experience'->>'certifications_count')::numeric,
            (data->'profile'->'raw'->>'certifications_count')::numeric
        ) AS certifications_count,

        -- outcome
        COALESCE(
            (data->'profile'->'outcome'->>'placed')::int,
            CASE
                WHEN data->'profile'->'raw'->>'placement' = 'Yes' THEN 1
                WHEN data->'profile'->'raw'->>'placement_status' = 'Placed' THEN 1
                WHEN data->'profile'->'raw'->>'placement_status' = 'Not Placed' THEN 0
                ELSE 0
            END
        ) AS placed
    FROM final_students
),

scores AS (
    SELECT
        *,
        CASE
            WHEN cgpa IS NULL
             AND iq IS NULL
             AND prev_sem_result IS NULL
             AND academic_performance IS NULL
             AND attendance_percentage IS NULL
            THEN NULL
            ELSE (
                COALESCE(cgpa, 0)
                + COALESCE(LEAST(GREATEST(iq / 14.0, 0), 10), 0)
                + COALESCE(prev_sem_result, 0)
                + COALESCE(academic_performance, 0)
                + COALESCE(attendance_percentage / 10.0, 0)
            )
            /
            NULLIF(
                (CASE WHEN cgpa IS NOT NULL THEN 1 ELSE 0 END) +
                (CASE WHEN iq IS NOT NULL THEN 1 ELSE 0 END) +
                (CASE WHEN prev_sem_result IS NOT NULL THEN 1 ELSE 0 END) +
                (CASE WHEN academic_performance IS NOT NULL THEN 1 ELSE 0 END) +
                (CASE WHEN attendance_percentage IS NOT NULL THEN 1 ELSE 0 END),
                0
            )
        END AS academic_score,

        CASE
            WHEN communication_skill_score IS NULL
             AND coding_skill_score IS NULL
             AND logical_reasoning_score IS NULL
             AND extracurricular_score IS NULL
            THEN NULL
            ELSE (
                COALESCE(communication_skill_score, 0)
                + COALESCE(coding_skill_score / 10.0, 0)
                + COALESCE(logical_reasoning_score / 10.0, 0)
                + COALESCE(extracurricular_score / 10.0, 0)
            )
            /
            NULLIF(
                (CASE WHEN communication_skill_score IS NOT NULL THEN 1 ELSE 0 END) +
                (CASE WHEN coding_skill_score IS NOT NULL THEN 1 ELSE 0 END) +
                (CASE WHEN logical_reasoning_score IS NOT NULL THEN 1 ELSE 0 END) +
                (CASE WHEN extracurricular_score IS NOT NULL THEN 1 ELSE 0 END),
                0
            )
        END AS skill_score,

        CASE
            WHEN projects_count IS NULL
             AND internships_value IS NULL
             AND certifications_count IS NULL
            THEN NULL
            ELSE (
                COALESCE(projects_count, 0)
                + COALESCE(internships_value, 0)
                + COALESCE(certifications_count, 0)
            )
            /
            NULLIF(
                (CASE WHEN projects_count IS NOT NULL THEN 1 ELSE 0 END) +
                (CASE WHEN internships_value IS NOT NULL THEN 1 ELSE 0 END) +
                (CASE WHEN certifications_count IS NOT NULL THEN 1 ELSE 0 END),
                0
            )
        END AS experience_score
    FROM base
),

ranked AS (
    SELECT
        *,
        CASE
            WHEN academic_score IS NOT NULL
            THEN NTILE(3) OVER (ORDER BY academic_score NULLS LAST)
        END AS academic_tile_all,

        CASE
            WHEN skill_score IS NOT NULL
            THEN NTILE(3) OVER (ORDER BY skill_score NULLS LAST)
        END AS skill_tile_all,

        CASE
            WHEN experience_score IS NOT NULL
            THEN NTILE(3) OVER (ORDER BY experience_score NULLS LAST)
        END AS experience_tile_all,

        CASE
            WHEN major IS NOT NULL AND academic_score IS NOT NULL
            THEN NTILE(3) OVER (PARTITION BY major ORDER BY academic_score NULLS LAST)
        END AS academic_tile_major,

        CASE
            WHEN major IS NOT NULL AND skill_score IS NOT NULL
            THEN NTILE(3) OVER (PARTITION BY major ORDER BY skill_score NULLS LAST)
        END AS skill_tile_major,

        CASE
            WHEN major IS NOT NULL AND experience_score IS NOT NULL
            THEN NTILE(3) OVER (PARTITION BY major ORDER BY experience_score NULLS LAST)
        END AS experience_tile_major
    FROM scores
),

major_rates AS (
    SELECT
        major,
        AVG(CASE WHEN academic_tile_major = 3 THEN placed::numeric END) AS placement_rate_high_academic,
        AVG(CASE WHEN academic_tile_major = 1 THEN placed::numeric END) AS placement_rate_low_academic,
        AVG(CASE WHEN skill_tile_major = 3 THEN placed::numeric END) AS placement_rate_high_skill,
        AVG(CASE WHEN skill_tile_major = 1 THEN placed::numeric END) AS placement_rate_low_skill,
        AVG(CASE WHEN experience_tile_major = 3 THEN placed::numeric END) AS placement_rate_high_experience,
        AVG(CASE WHEN experience_tile_major = 1 THEN placed::numeric END) AS placement_rate_low_experience
    FROM ranked
    WHERE major IS NOT NULL
    GROUP BY major
),

major_importance AS (
    SELECT
        major,
        placement_rate_high_academic,
        placement_rate_low_academic,
        placement_rate_high_academic - placement_rate_low_academic AS academic_lift,
        placement_rate_high_skill,
        placement_rate_low_skill,
        placement_rate_high_skill - placement_rate_low_skill AS skill_lift,
        placement_rate_high_experience,
        placement_rate_low_experience,
        placement_rate_high_experience - placement_rate_low_experience AS experience_lift,
        CASE
            WHEN (placement_rate_high_academic - placement_rate_low_academic) >=
                 (placement_rate_high_skill - placement_rate_low_skill)
             AND (placement_rate_high_academic - placement_rate_low_academic) >=
                 (placement_rate_high_experience - placement_rate_low_experience)
            THEN 'Academics'
            WHEN (placement_rate_high_skill - placement_rate_low_skill) >=
                 (placement_rate_high_academic - placement_rate_low_academic)
             AND (placement_rate_high_skill - placement_rate_low_skill) >=
                 (placement_rate_high_experience - placement_rate_low_experience)
            THEN 'Skills'
            ELSE 'Experience'
        END AS most_important_category
    FROM major_rates
)

SELECT
    r.student_key,
    r.major,
    r.academic_score,
    r.skill_score,
    r.experience_score,
    CASE
        WHEN r.academic_score IS NULL THEN NULL
        WHEN r.academic_tile_all = 1 THEN 'Low'
        WHEN r.academic_tile_all = 2 THEN 'Medium'
        WHEN r.academic_tile_all = 3 THEN 'High'
    END AS academic_bucket,
    CASE
        WHEN r.skill_score IS NULL THEN NULL
        WHEN r.skill_tile_all = 1 THEN 'Low'
        WHEN r.skill_tile_all = 2 THEN 'Medium'
        WHEN r.skill_tile_all = 3 THEN 'High'
    END AS skill_bucket,
    CASE
        WHEN r.experience_score IS NULL THEN NULL
        WHEN r.experience_tile_all = 1 THEN 'Low'
        WHEN r.experience_tile_all = 2 THEN 'Medium'
        WHEN r.experience_tile_all = 3 THEN 'High'
    END AS experience_bucket,
    r.placed,
    m.placement_rate_high_academic,
    m.placement_rate_low_academic,
    m.academic_lift,
    m.placement_rate_high_skill,
    m.placement_rate_low_skill,
    m.skill_lift,
    m.placement_rate_high_experience,
    m.placement_rate_low_experience,
    m.experience_lift,
    CASE
        WHEN r.major IS NOT NULL THEN m.most_important_category
        ELSE NULL
    END AS most_important_category
FROM ranked r
LEFT JOIN major_importance m
    ON r.major = m.major
ORDER BY r.major NULLS LAST, r.student_key;

SELECT *
FROM task0_final_output;