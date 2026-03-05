
-- 54K RESUMES


-- confirm the join key exists + spelling
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema='proj'
  AND table_name IN ('54k_01_people','54k_02_abilities','54k_03_education','54k_04_experience','54k_05_person_skills')
  AND lower(column_name) IN ('personid','person_id','id')
ORDER BY table_name, column_name;

-- check row counts
SELECT 'people' AS tbl, COUNT(*) FROM proj."54k_01_people"
UNION ALL SELECT 'abilities', COUNT(*) FROM proj."54K_02_abilities"
UNION ALL SELECT 'education', COUNT(*) FROM proj."54K_03_education"
UNION ALL SELECT 'experience', COUNT(*) FROM proj."54K_04_experience"
UNION ALL SELECT 'person_skills', COUNT(*) FROM proj."54K_05_person_skills";



CREATE OR REPLACE VIEW proj.v_54k_master AS
WITH
-- 02 abilities (many rows per person)
abilities AS (
    SELECT
        person_id,
        jsonb_agg(to_jsonb(a) ORDER BY a.*) AS abilities
    FROM proj."54K_02_abilities" a
    GROUP BY person_id
),

-- 03 education (many rows per person)
education AS (
    SELECT
        person_id,
        jsonb_agg(to_jsonb(e) ORDER BY e.*) AS education
    FROM proj."54K_03_education" e
    GROUP BY person_id
),

-- 04 experience (many rows per person)
experience AS (
    SELECT
        person_id,
        jsonb_agg(to_jsonb(x) ORDER BY x.*) AS experience
    FROM proj."54K_04_experience" x
    GROUP BY person_id
),

-- 05 person_skills (many rows per person)
person_skills AS (
    SELECT
        person_id,
        jsonb_agg(to_jsonb(s) ORDER BY s.*) AS person_skills
    FROM proj."54K_05_person_skills" s
    GROUP BY person_id
)

SELECT
    p.person_id,
    to_jsonb(p)                         AS people_row,
    COALESCE(ab.abilities, '[]'::jsonb) AS abilities,
    COALESCE(ed.education, '[]'::jsonb) AS education,
    COALESCE(ex.experience, '[]'::jsonb)AS experience,
    COALESCE(ps.person_skills,'[]'::jsonb) AS person_skills
FROM proj."54k_01_people" p
         LEFT JOIN abilities     ab ON ab.person_id = p.person_id
         LEFT JOIN education     ed ON ed.person_id = p.person_id
         LEFT JOIN experience    ex ON ex.person_id = p.person_id
         LEFT JOIN person_skills ps ON ps.person_id = p.person_id;


SELECT COUNT(*) FROM proj.v_54k_master;           -- should equal #people (or close)
SELECT * FROM proj.v_54k_master LIMIT 5;          -- spot-check
SELECT person_id, jsonb_array_length(experience) AS n_exp
FROM proj.v_54k_master
ORDER BY n_exp DESC
LIMIT 10;

DROP TABLE IF EXISTS proj.json_54k_profiles;

CREATE TABLE proj.json_54k_profiles (
                                        personid   TEXT PRIMARY KEY,
                                        profile    JSONB NOT NULL
);

INSERT INTO proj.json_54k_profiles(personid, profile)
SELECT
    person_id::text,
    jsonb_build_object(
            'source', '54k_resumes',
            'personid', person_id::text,
            'people', people_row,
            'abilities', abilities,
            'education', education,
            'experience', experience,
            'person_skills', person_skills
    )
FROM proj.v_54k_master;

-- verify
SELECT personid, profile
FROM proj.json_54k_profiles
LIMIT 2;

SELECT profile FROM proj.json_54k_profiles LIMIT 2000;