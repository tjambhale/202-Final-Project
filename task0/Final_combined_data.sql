
--- got rid of the postgres project part

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
SELECT COUNT(*) AS total_rows
FROM proj.master_profiles;

SELECT profile->>'source' AS source, COUNT(*) AS count
FROM proj.master_profiles
GROUP BY 1
ORDER BY 2 DESC;

SELECT global_key, profile
FROM proj.master_profiles
ORDER BY profile->>'source', global_key;

SELECT profile
FROM proj.master_profiles
ORDER BY profile->>'source', global_key;
