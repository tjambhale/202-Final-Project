use("proj");

db.resume_change_logs.find(
  { student_key: "u_0019" },
  {
    _id: 0,
    student_key: 1,
    comparison_type: 1,
    compared_versions: 1,
    focus_area: 1,
    focus_area_delta: 1,
    improved_in_focus_area: 1,
    score_before: 1,
    score_after: 1,
    score_delta: 1,
    "changes.academics": 1,
    "changes.skills": 1,
    "changes.experience": 1
  }
).sort({ "compared_timestamps.from": 1 });
