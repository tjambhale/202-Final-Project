use("proj");

/* =========================================================
   TASK 2 - MongoDB Setup / Build Script
   Assumes these collections already exist:
   - resumes_original
   - resumes_v1
   - resumes_v2
   - resumes_v3
   - task1_profiles
   ========================================================= */

/* =========================
   1. Build resume_versions
   ========================= */

db.resume_versions.drop();

db.resumes_original.aggregate([
  {
    $addFields: {
      version_label: "original",
      version_order: 0,
      source_file: "resumes-original.json"
    }
  },
  {
    $project: {
      _id: 0,
      student_key: 1,
      timestamp: { $toDate: "$timestamp" },
      version_label: 1,
      version_order: 1,
      source_file: 1,
      college: 1,
      academics: 1,
      skills: 1,
      experience: 1
    }
  },
  {
    $unionWith: {
      coll: "resumes_v1",
      pipeline: [
        {
          $addFields: {
            version_label: "v1",
            version_order: 1,
            source_file: "resumes-version1.json"
          }
        },
        {
          $project: {
            _id: 0,
            student_key: 1,
            timestamp: { $toDate: "$timestamp" },
            version_label: 1,
            version_order: 1,
            source_file: 1,
            college: 1,
            academics: 1,
            skills: 1,
            experience: 1
          }
        }
      ]
    }
  },
  {
    $unionWith: {
      coll: "resumes_v2",
      pipeline: [
        {
          $addFields: {
            version_label: "v2",
            version_order: 2,
            source_file: "resumes-version2.json"
          }
        },
        {
          $project: {
            _id: 0,
            student_key: 1,
            timestamp: { $toDate: "$timestamp" },
            version_label: 1,
            version_order: 1,
            source_file: 1,
            college: 1,
            academics: 1,
            skills: 1,
            experience: 1
          }
        }
      ]
    }
  },
  {
    $unionWith: {
      coll: "resumes_v3",
      pipeline: [
        {
          $addFields: {
            version_label: "v3",
            version_order: 3,
            source_file: "resumes-version3.json"
          }
        },
        {
          $project: {
            _id: 0,
            student_key: 1,
            timestamp: { $toDate: "$timestamp" },
            version_label: 1,
            version_order: 1,
            source_file: 1,
            college: 1,
            academics: 1,
            skills: 1,
            experience: 1
          }
        }
      ]
    }
  },
  {
    $merge: {
      into: "resume_versions",
      whenMatched: "replace",
      whenNotMatched: "insert"
    }
  }
]);

/* =========================
   2. Create indexes
   ========================= */

db.resume_versions.createIndex(
  { student_key: 1, version_order: 1 },
  { unique: true }
);

db.resume_versions.createIndex({ student_key: 1, timestamp: 1 });

db.task1_profiles.createIndex(
  { student_key: 1 },
  { unique: true }
);

/* =========================
   3. Helper functions
   ========================= */

function safeLen(arr) {
  return Array.isArray(arr) ? arr.length : 0;
}

function monthsBetween(startDate, endDate) {
  if (!startDate || !endDate) return 0;

  const s = new Date(startDate);
  const e = new Date(endDate);

  let months =
    (e.getFullYear() - s.getFullYear()) * 12 +
    (e.getMonth() - s.getMonth());

  return Math.max(months, 1);
}

function round4(x) {
  return Math.round(x * 10000) / 10000;
}

function norm(s) {
  return String(s || "").trim().toLowerCase();
}

function setDiff(oldArr, newArr) {
  const oldSet = new Set((oldArr || []).map(norm));
  const newSet = new Set((newArr || []).map(norm));

  return {
    added: [...newSet].filter(x => !oldSet.has(x)),
    removed: [...oldSet].filter(x => !newSet.has(x))
  };
}

function objNameSet(arr, key) {
  return (arr || []).map(x => x?.[key]).filter(Boolean);
}

/* =========================
   4. Scoring functions
   Mirrors Task 1 logic
   ========================= */

function academicScore(doc) {
  const cgpa = Number(doc?.academics?.cgpa || 0);
  const courseCount = safeLen(doc?.academics?.relevant_coursework);
  const honorCount = safeLen(doc?.academics?.honors_or_awards);

  return round4(
    0.60 * Math.min((cgpa / 4.0) * 10.0, 10) +
    0.25 * Math.min(courseCount * 2.0, 10) +
    0.15 * Math.min(honorCount * 3.0, 10)
  );
}

function skillScore(doc) {
  const techCount = safeLen(doc?.skills?.technical);
  const softCount = safeLen(doc?.skills?.soft);

  return round4(
    0.70 * Math.min(techCount * 1.5, 10) +
    0.30 * Math.min(softCount * 1.5, 10)
  );
}

function experienceScore(doc) {
  const projectCount = safeLen(doc?.experience?.projects);
  const leadershipCount = safeLen(doc?.experience?.leadership_and_activities);

  let workMonths = 0;
  if (Array.isArray(doc?.experience?.work) && doc.experience.work.length > 0) {
    const w = doc.experience.work[0];
    workMonths = monthsBetween(w.start_date, w.end_date);
  }

  return round4(
    0.40 * Math.min(projectCount * 2.0, 10) +
    0.35 * Math.min(workMonths / 2.0, 10) +
    0.25 * Math.min(leadershipCount * 3.0, 10)
  );
}

/* =========================
   5. Add scores to versions
   ========================= */

db.resume_versions.find().forEach(doc => {
  db.resume_versions.updateOne(
    { _id: doc._id },
    {
      $set: {
        academic_score: academicScore(doc),
        skill_score: skillScore(doc),
        experience_score: experienceScore(doc)
      }
    }
  );
});

/* =========================
   6. Compare versions
   ========================= */

function compareResumeVersions(oldDoc, newDoc) {
  const oldCourses = objNameSet(oldDoc?.academics?.relevant_coursework, "course");
  const newCourses = objNameSet(newDoc?.academics?.relevant_coursework, "course");

  const oldAwards = objNameSet(oldDoc?.academics?.honors_or_awards, "title");
  const newAwards = objNameSet(newDoc?.academics?.honors_or_awards, "title");

  const oldProjects = objNameSet(oldDoc?.experience?.projects, "name");
  const newProjects = objNameSet(newDoc?.experience?.projects, "name");

  const oldWork = objNameSet(oldDoc?.experience?.work, "title");
  const newWork = objNameSet(newDoc?.experience?.work, "title");

  const oldActs = objNameSet(oldDoc?.experience?.leadership_and_activities, "activity");
  const newActs = objNameSet(newDoc?.experience?.leadership_and_activities, "activity");

  return {
    academics: {
      cgpa_old: Number(oldDoc?.academics?.cgpa || 0),
      cgpa_new: Number(newDoc?.academics?.cgpa || 0),
      cgpa_delta: round4(
        Number(newDoc?.academics?.cgpa || 0) - Number(oldDoc?.academics?.cgpa || 0)
      ),
      coursework: setDiff(oldCourses, newCourses),
      awards: setDiff(oldAwards, newAwards)
    },
    skills: {
      technical: setDiff(oldDoc?.skills?.technical, newDoc?.skills?.technical),
      soft: setDiff(oldDoc?.skills?.soft, newDoc?.skills?.soft)
    },
    experience: {
      projects: setDiff(oldProjects, newProjects),
      work_titles: setDiff(oldWork, newWork),
      activities: setDiff(oldActs, newActs)
    },
    score_delta: {
      academic: round4((newDoc.academic_score || 0) - (oldDoc.academic_score || 0)),
      skill: round4((newDoc.skill_score || 0) - (oldDoc.skill_score || 0)),
      experience: round4((newDoc.experience_score || 0) - (oldDoc.experience_score || 0))
    }
  };
}

function getFocusDelta(focusArea, diff) {
  if (focusArea === "Academics") return diff.score_delta.academic;
  if (focusArea === "Skills") return diff.score_delta.skill;
  if (focusArea === "Experience") return diff.score_delta.experience;
  return null;
}

/* =========================
   7. Build change logs
   Includes:
   - step comparisons
   - overall original -> v3
   ========================= */

db.resume_change_logs.drop();

const students = db.resume_versions.distinct("student_key");

students.forEach(studentKey => {
  const versions = db.resume_versions
    .find({ student_key: studentKey })
    .sort({ version_order: 1 })
    .toArray();

  const profile = db.task1_profiles.findOne({ student_key: studentKey });

  /* Step-by-step comparisons:
     original -> v1
     v1 -> v2
     v2 -> v3
  */
  for (let i = 1; i < versions.length; i++) {
    const prev = versions[i - 1];
    const curr = versions[i];
    const diff = compareResumeVersions(prev, curr);

    const focusArea = profile?.focus_area || null;
    const focusDelta = getFocusDelta(focusArea, diff);

    db.resume_change_logs.insertOne({
      student_key: studentKey,
      major: curr?.college?.major || profile?.major || null,
      comparison_type: "step",
      compared_versions: {
        from: prev.version_label,
        to: curr.version_label
      },
      compared_timestamps: {
        from: prev.timestamp,
        to: curr.timestamp
      },
      focus_area: focusArea,
      most_important_category: profile?.most_important_category || null,
      score_before: {
        academic: prev.academic_score,
        skill: prev.skill_score,
        experience: prev.experience_score
      },
      score_after: {
        academic: curr.academic_score,
        skill: curr.skill_score,
        experience: curr.experience_score
      },
      score_delta: diff.score_delta,
      improved_in_focus_area: focusDelta !== null ? focusDelta > 0 : null,
      focus_area_delta: focusDelta,
      changes: diff
    });
  }

  /* Overall comparison:
     original -> v3
  */
  const originalDoc = versions.find(v => v.version_label === "original");
  const latestDoc = versions.find(v => v.version_label === "v3");

  if (originalDoc && latestDoc) {
    const diff = compareResumeVersions(originalDoc, latestDoc);

    const focusArea = profile?.focus_area || null;
    const focusDelta = getFocusDelta(focusArea, diff);

    db.resume_change_logs.insertOne({
      student_key: studentKey,
      major: latestDoc?.college?.major || profile?.major || null,
      comparison_type: "overall",
      compared_versions: {
        from: "original",
        to: "v3"
      },
      compared_timestamps: {
        from: originalDoc.timestamp,
        to: latestDoc.timestamp
      },
      focus_area: focusArea,
      most_important_category: profile?.most_important_category || null,
      score_before: {
        academic: originalDoc.academic_score,
        skill: originalDoc.skill_score,
        experience: originalDoc.experience_score
      },
      score_after: {
        academic: latestDoc.academic_score,
        skill: latestDoc.skill_score,
        experience: latestDoc.experience_score
      },
      score_delta: diff.score_delta,
      improved_in_focus_area: focusDelta !== null ? focusDelta > 0 : null,
      focus_area_delta: focusDelta,
      changes: diff
    });
  }
});

/* =========================
   8. Custom any-two-version comparison
   Returns object only
   ========================= */

function getResumeComparison(studentKey, fromVersion, toVersion) {
  const profile = db.task1_profiles.findOne({ student_key: studentKey });

  const fromDoc = db.resume_versions.findOne({
    student_key: studentKey,
    version_label: fromVersion
  });

  const toDoc = db.resume_versions.findOne({
    student_key: studentKey,
    version_label: toVersion
  });

  if (!fromDoc || !toDoc) {
    return null;
  }

  const diff = compareResumeVersions(fromDoc, toDoc);

  const focusArea = profile?.focus_area || null;
  const focusDelta = getFocusDelta(focusArea, diff);

  return {
    student_key: studentKey,
    compared_versions: {
      from: fromVersion,
      to: toVersion
    },
    compared_timestamps: {
      from: fromDoc.timestamp,
      to: toDoc.timestamp
    },
    focus_area: focusArea,
    score_before: {
      academic: fromDoc.academic_score,
      skill: fromDoc.skill_score,
      experience: fromDoc.experience_score
    },
    score_after: {
      academic: toDoc.academic_score,
      skill: toDoc.skill_score,
      experience: toDoc.experience_score
    },
    score_delta: diff.score_delta,
    improved_in_focus_area: focusDelta !== null ? focusDelta > 0 : null,
    focus_area_delta: focusDelta,
    changes: diff
  };
}

/* =========================
   9. User-facing helper
   Prints legend only when error occurs
   ========================= */

function showVersionDiff(studentKey, fromVersion, toVersion) {
  const result = getResumeComparison(studentKey, fromVersion, toVersion);

  if (!result) {
    print("Unable to compare the requested versions.");
    print("Version legend: original = first uploaded resume, v1 = first update, v2 = second update, v3 = most recent resume.");
    print("Please make sure the student_key exists and both version names are valid.");
    return;
  }

  printjson(result);
}

/* =========================
   10. Optional sanity checks
   Uncomment if needed
   ========================= */

// print("task1_profiles count: " + db.task1_profiles.countDocuments());
// print("resume_versions count: " + db.resume_versions.countDocuments());
// print("resume_change_logs count: " + db.resume_change_logs.countDocuments());
