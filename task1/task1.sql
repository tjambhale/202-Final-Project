SET search_path TO proj;

-- STEP 1: Unnest all 20 resumes from the single JSON array cell
CREATE OR REPLACE VIEW proj.v_task1_raw AS
SELECT
    resume->>'student_key'                          AS student_key,
    resume->'college'->>'major'                     AS major,
    resume->'college'->>'name'                      AS college,
    (resume->'academics'->>'cgpa')::numeric         AS cgpa,

    jsonb_array_length(COALESCE(resume->'academics'->'relevant_coursework', '[]')) AS course_count,
    jsonb_array_length(COALESCE(resume->'academics'->'honors_or_awards',    '[]')) AS honor_count,
    jsonb_array_length(COALESCE(resume->'skills'->'technical',              '[]')) AS tech_skill_count,
    jsonb_array_length(COALESCE(resume->'skills'->'soft',                   '[]')) AS soft_skill_count,
    jsonb_array_length(COALESCE(resume->'experience'->'projects',           '[]')) AS project_count,
    jsonb_array_length(COALESCE(resume->'experience'->'leadership_and_activities','[]')) AS leadership_count,

    CASE
        WHEN jsonb_array_length(COALESCE(resume->'experience'->'work','[]')) > 0
            THEN GREATEST(
                (DATE_PART('year',  (resume->'experience'->'work'->0->>'end_date')::date)
                    - DATE_PART('year',  (resume->'experience'->'work'->0->>'start_date')::date)) * 12
                    + DATE_PART('month', (resume->'experience'->'work'->0->>'end_date')::date)
                    - DATE_PART('month', (resume->'experience'->'work'->0->>'start_date')::date), 1)
        ELSE 0
        END AS work_months

FROM (
         SELECT jsonb_array_elements(c1::jsonb) AS resume
         FROM proj.resumes_original
         WHERE c0 = 'resumes'
     ) expanded;


-- STEP 2: Compute 3 scores
CREATE OR REPLACE VIEW proj.v_task1_scores AS
SELECT
    student_key,
    major,
    college,
    cgpa,
    ROUND((
              0.60 * LEAST((cgpa / 4.0) * 10.0, 10) +
              0.25 * LEAST(course_count * 2.0,   10) +
              0.15 * LEAST(honor_count  * 3.0,   10)
              )::numeric, 4) AS academic_score,

    ROUND((
              0.70 * LEAST(tech_skill_count * 1.5, 10) +
              0.30 * LEAST(soft_skill_count * 1.5, 10)
              )::numeric, 4) AS skill_score,

    ROUND((
              0.40 * LEAST(project_count    * 2.0, 10) +
              0.35 * LEAST(work_months      / 2.0, 10) +
              0.25 * LEAST(leadership_count * 3.0, 10)
              )::numeric, 4) AS experience_score
FROM proj.v_task1_raw;

-- STEP 3: NTILE(3) buckets across all 20 students
CREATE OR REPLACE VIEW proj.v_task1_buckets AS
SELECT
    *,
    CASE NTILE(3) OVER (ORDER BY academic_score)
        WHEN 1 THEN 'Low' WHEN 2 THEN 'Medium' ELSE 'High'
        END AS academic_bucket,
    CASE NTILE(3) OVER (ORDER BY skill_score)
        WHEN 1 THEN 'Low' WHEN 2 THEN 'Medium' ELSE 'High'
        END AS skill_bucket,
    CASE NTILE(3) OVER (ORDER BY experience_score)
        WHEN 1 THEN 'Low' WHEN 2 THEN 'Medium' ELSE 'High'
        END AS experience_bucket
FROM proj.v_task1_scores;

-- STEP 4: Get major lifts from task0
CREATE OR REPLACE VIEW proj.v_task1_major_lifts AS
SELECT
    major,
    AVG(academic_lift)     AS academic_lift,
    AVG(skill_lift)        AS skill_lift,
    AVG(experience_lift)   AS experience_lift,
    MODE() WITHIN GROUP (ORDER BY most_important_category) AS most_important_category
FROM proj.postgres_public_task0_final_output
WHERE major IS NOT NULL
GROUP BY major;

-- STEP 5: Final output — 20 rows, one per student
CREATE OR REPLACE VIEW proj.v_task1_final AS
WITH mapped AS (
    SELECT
        b.*,
        CASE b.major
            WHEN 'Computer Science'              THEN 'CSE'
            WHEN 'Computer Science: Game Design' THEN 'CSE'
            WHEN 'Computer Engineering'          THEN 'CSE'
            WHEN 'Software Engineering'          THEN 'Software Engineer'
            WHEN 'Data Science'                  THEN 'IT'
            WHEN 'Information Systems'           THEN 'IT'
            WHEN 'Electrical Engineering'        THEN 'EEE'
            WHEN 'Mechanical Engineering'        THEN 'Mechanical'
            WHEN 'Economics'                     THEN 'Comm&Mgmt'
            WHEN 'Mathematics'                   THEN 'Sci&Tech'
            WHEN 'Statistics'                    THEN 'Sci&Tech'
            WHEN 'Design'                        THEN 'UI/UX'
            WHEN 'Cognitive Science (HCI)'       THEN 'UI/UX'
            WHEN 'Biomedical Engineering'        THEN 'ECE'
            ELSE b.major
            END AS task0_major
    FROM proj.v_task1_buckets b
),
     joined AS (
         SELECT
             m.*,
             l.academic_lift,
             l.skill_lift,
             l.experience_lift,
             l.most_important_category,

             -- FOCUS AREA: GPA < 3.0 with Low academics overrides everything
             CASE
                 WHEN m.cgpa < 3.0 AND m.academic_bucket = 'Low'
                     THEN 'Academics'
                 WHEN COALESCE(l.experience_lift,0) >= COALESCE(l.skill_lift,0)
                     AND COALESCE(l.experience_lift,0) >= COALESCE(l.academic_lift,0)
                     THEN 'Experience'
                 WHEN COALESCE(l.skill_lift,0) >= COALESCE(l.academic_lift,0)
                     THEN 'Skills'
                 ELSE 'Academics'
                 END AS focus_area,

             -- FOCUS BUCKET: match focus area's bucket
             CASE
                 WHEN m.cgpa < 3.0 AND m.academic_bucket = 'Low'
                     THEN m.academic_bucket
                 WHEN COALESCE(l.experience_lift,0) >= COALESCE(l.skill_lift,0)
                     AND COALESCE(l.experience_lift,0) >= COALESCE(l.academic_lift,0)
                     THEN m.experience_bucket
                 WHEN COALESCE(l.skill_lift,0) >= COALESCE(l.academic_lift,0)
                     THEN m.skill_bucket
                 ELSE m.academic_bucket
                 END AS focus_bucket

         FROM mapped m
                  LEFT JOIN proj.v_task1_major_lifts l ON l.major = m.task0_major
     )
SELECT
    student_key,
    college,
    major,
    ROUND(cgpa, 2)                           AS cgpa,
    ROUND(academic_score::numeric,   2)      AS academic_score,
    ROUND(skill_score::numeric,      2)      AS skill_score,
    ROUND(experience_score::numeric, 2)      AS experience_score,
    academic_bucket,
    skill_bucket,
    experience_bucket,
    most_important_category,
    focus_area,

    -- RECOMMENDATION string
    CASE
        -- GPA override recommendation
        WHEN cgpa < 3.0 AND academic_bucket = 'Low' THEN
            'URGENT — GPA is ' || ROUND(cgpa,2)::text || ' (below 3.0) and Academic score is Low. ' ||
            'Raise your GPA first — this is a hard filter for most employers. ' ||
            'Attend office hours, retake low-grade courses, and reduce extracurriculars temporarily.'

        WHEN focus_bucket = 'Low' THEN
            'URGENT — ' || focus_area || ' is your weakest area AND has the highest placement lift for ' ||
            major || ' majors (lift: ' ||
            ROUND(CASE focus_area
                      WHEN 'Experience' THEN COALESCE(experience_lift,0)
                      WHEN 'Skills'     THEN COALESCE(skill_lift,0)
                      ELSE                   COALESCE(academic_lift,0)
                      END::numeric, 3)::text || '). ' ||
            CASE focus_area
                WHEN 'Experience' THEN 'Get an internship and complete 1-2 more strong projects.'
                WHEN 'Skills'     THEN 'Add 2-3 in-demand technical skills and build a portfolio.'
                ELSE                   'Raise GPA above 3.7 and pursue honors or research.'
                END

        WHEN focus_bucket = 'Medium' THEN
            'GROW — Push ' || focus_area || ' from Medium to High. ' ||
            'This is the highest-leverage move for ' || major || ' placement (lift: ' ||
            ROUND(CASE focus_area
                      WHEN 'Experience' THEN COALESCE(experience_lift,0)
                      WHEN 'Skills'     THEN COALESCE(skill_lift,0)
                      ELSE                   COALESCE(academic_lift,0)
                      END::numeric, 3)::text || '). ' ||
            CASE focus_area
                WHEN 'Experience' THEN 'Take on a leadership role and add one more substantial project.'
                WHEN 'Skills'     THEN 'Deepen expertise in your top skills and earn a certification.'
                ELSE                   'Target 3.7+ GPA and take advanced electives in your focus area.'
                END

        ELSE
            'MAINTAIN — ' || focus_area || ' is already High. ' ||
            'Keep it strong and shift focus to your next weakest area to stay well-rounded for ' || major || ' roles.'
        END AS recommendation

FROM joined
ORDER BY student_key;

-- final students
SELECT * FROM proj.v_task1_final;