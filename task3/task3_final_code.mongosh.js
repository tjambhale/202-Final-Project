const TASK0_KEYWORDS = [
  "internship", "projects", "cgpa", "gpa", "academic", "performance",
  "communication", "extracurricular", "leadership"
];

function escRe(s) {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function regexFromKeywords(words) {
  return words.map(w => escRe(w.toLowerCase())).join("|");
}

function targetKeywords(target) {
  return (target || "")
    .toLowerCase()
    .split(/\s+/)
    .map(x => x.trim())
    .filter(x => x.length >= 3);
}

function runTailor(studentKey, target, opts = {}) {
  const bulletsPerItem = opts.bulletsPerItem ?? 2;
  const maxItemsTotal = opts.maxItemsTotal ?? 8;

  const tkw = targetKeywords(target);
  if (tkw.length === 0) {
    print("ERROR: target string too short. Add more keywords.");
    return;
  }

  const targetRe = regexFromKeywords(tkw);
  const task0Re = regexFromKeywords(TASK0_KEYWORDS);

  const out = db.master_resumes.aggregate([
    { $match: { student_key: studentKey } },

    {
      $addFields: {
        all_items: {
          $concatArrays: [
            {
              $map: {
                input: { $ifNull: ["$experience", []] },
                as: "e",
                in: {
                  section: "experience",
                  title: "$$e.role_title",
                  org: "$$e.company",
                  start_date: "$$e.start_date",
                  end_date: "$$e.end_date",
                  bullets: { $ifNull: ["$$e.bullets", []] }
                }
              }
            },
            {
              $map: {
                input: { $ifNull: ["$projects", []] },
                as: "p",
                in: {
                  section: "projects",
                  title: "$$p.project_name",
                  org: "$$p.category",
                  start_date: "$$p.start_date",
                  end_date: "$$p.end_date",
                  bullets: { $ifNull: ["$$p.bullets", []] }
                }
              }
            },
            {
              $map: {
                input: { $ifNull: ["$research", []] },
                as: "r",
                in: {
                  section: "research",
                  title: "$$r.role_title",
                  org: "$$r.institution",
                  start_date: "$$r.start_date",
                  end_date: "$$r.end_date",
                  bullets: { $ifNull: ["$$r.bullets", []] }
                }
              }
            }
          ]
        }
      }
    },

    { $unwind: "$all_items" },
    { $unwind: "$all_items.bullets" },

    {
      $addFields: {
        combined_text: {
          $toLower: {
            $concat: [
              { $ifNull: ["$all_items.title", ""] }, " ",
              { $ifNull: ["$all_items.org", ""] }, " ",
              { $ifNull: ["$all_items.bullets", ""] }
            ]
          }
        }
      }
    },

    {
      $addFields: {
        bullet_text: "$all_items.bullets",
        targetHits: { $size: { $regexFindAll: { input: "$combined_text", regex: targetRe } } },
        task0Hits: { $size: { $regexFindAll: { input: "$combined_text", regex: task0Re } } }
      }
    },

    { $match: { targetHits: { $gt: 0 } } },

    { $addFields: { score: { $add: [{ $multiply: ["$targetHits", 2] }, "$task0Hits"] } } },
    { $sort: { score: -1 } },

    {
      $group: {
        _id: {
          section: "$all_items.section",
          title: "$all_items.title",
          org: "$all_items.org",
          start_date: "$all_items.start_date",
          end_date: "$all_items.end_date"
        },
        bullets: { $push: "$bullet_text" },
        bestScore: { $max: "$score" },
        education: { $first: "$education" },
        skills: { $first: "$skills" }
      }
    },

    {
      $project: {
        _id: 0,
        section: "$_id.section",
        title: "$_id.title",
        org: "$_id.org",
        start_date: "$_id.start_date",
        end_date: "$_id.end_date",
        bullets: { $slice: ["$bullets", bulletsPerItem] },
        education: 1,
        skills: 1,
        bestScore: 1
      }
    },

    { $sort: { bestScore: -1 } },
    { $limit: maxItemsTotal },

    {
      $group: {
        _id: studentKey,
        education: { $first: "$education" },
        skills: { $first: "$skills" },
        items: {
          $push: {
            section: "$section",
            title: "$title",
            org: "$org",
            start_date: "$start_date",
            end_date: "$end_date",
            bullets: "$bullets"
          }
        }
      }
    },

    {
      $project: {
        _id: 0,
        student_key: "$_id",
        target: target,
        resume: {
          education: "$education",
          skills: "$skills",
          experience: { $filter: { input: "$items", as: "it", cond: { $eq: ["$$it.section", "experience"] } } },
          projects: { $filter: { input: "$items", as: "it", cond: { $eq: ["$$it.section", "projects"] } } },
          research: { $filter: { input: "$items", as: "it", cond: { $eq: ["$$it.section", "research"] } } }
        }
      }
    }
  ]).toArray();

  if (!out.length) {
    print("No matching bullets found. Try broader target keywords.");
    return;
  }

  printjson(out[0]);
}

