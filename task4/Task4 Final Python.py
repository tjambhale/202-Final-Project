from sentence_transformers import SentenceTransformer
import chromadb
import psycopg2
import json
import pandas as pd

# ChromaDB
# Creates or opens a persistent local Chroma fatabase folder
    # Keeps the vector collections saved on disk instead of only in memory
chroma_client = chromadb.PersistentClient(path='./chroma_db')
# Collection that contains detailed profile text with resume information
# Contains more information
collection_rich = chroma_client.get_or_create_collection(
    name='student_profiles_rich',
    metadata={'hnsw:space': 'cosine'}
)
# Collection for shorter or more general profile summaries
collection_broad = chroma_client.get_or_create_collection(
    name='student_profiles_broad',
    metadata={'hnsw:space': 'cosine'}
)

# Connect to Postgres
# !!!Enter your own dbname, user, password, host, and port!!!
# !!!Enter inside quotation marks ''
conn = psycopg2.connect(
    dbname='postgres',
    user='postgres',
    password='123456',
    host='localhost',
    port=5432
)
# Curser object used to execute SQL commands
cur = conn.cursor()

# Model
model = SentenceTransformer('all-MiniLM-L6-v2')

# Load original resume files
# !!!!!!! PASTE OWN FILE PATH FOR 'resumes_original.json'!!!!!!!
with open('', 'r') as f:
    resumes = json.load(f)['resumes']   # must extract nested 'resumes' key
print(f'Resumes loaded: {len(resumes)}')

# Load scored profiles for resumes from Task 1
# !!!!!!! PASTE OWN FILE PATH FOR 'task1_final_output.json'!!!!!!!
with open('', 'r') as f:
    task1 = json.load(f)
print(f'Task1 profiles loaded: {len(task1)}')

# Load Task 0 output
# !!!!!!! PASTE OWN FILE PATH FOR 'postgres_public_task0_final_output.json'!!!!!!!
with open('', 'r') as f:
    task0_all = json.load(f)
print(f'Task0 records loaded: {len(task0)}')

# Keep only Task 0 output
major_lifts = {}
for r in task0:
    if r['major'] and r['major'] not in major_lifts:
        major_lifts[r['major']] = {
            'academic_lift':   r['academic_lift']   if r['academic_lift']   is not None else 0,
            'skill_lift':      r['skill_lift']       if r['skill_lift']       is not None else 0,
            'experience_lift': r['experience_lift']  if r['experience_lift']  is not None else 0
        }

# Map Task 1 major names to Task 0 major names
task0 = [
    r for r in task0_all
    if r['major']
    and r['academic_bucket']
    and r['skill_bucket']
    and r['experience_bucket']
    and r['academic_score'] is not None
    and r['skill_score'] is not None
    and r['experience_score'] is not None
]
print(f'Task0 usable records: {len(task0)}')

# Build lookup of lift values by Task 0 major
MAJOR_MAP = {
    'Computer Science':              'CSE',
    'Computer Science: Game Design': 'CSE',
    'Computer Engineering':          'CSE',
    'Software Engineering':          'Software Engineer',
    'Data Science':                  'IT',
    'Information Systems':           'IT',
    'Electrical Engineering':        'EEE',
    'Mechanical Engineering':        'Mechanical',
    'Economics':                     'Comm&Mgmt',
    'Mathematics':                   'Sci&Tech',
    'Statistics':                    'Sci&Tech',
    'Design':                        'UI/UX',
    'Cognitive Science (HCI)':       'UI/UX',
    'Biomedical Engineering':        'ECE',
}

# Combine Task 1 scores with Mapped Task 0 lift values
major_lifts = {}
for r in task0:
    if r['major'] not in major_lifts:
        major_lifts[r['major']] = {
            'academic_lift':   r['academic_lift']   if r['academic_lift']   is not None else 0,
            'skill_lift':      r['skill_lift']       if r['skill_lift']       is not None else 0,
            'experience_lift': r['experience_lift']  if r['experience_lift']  is not None else 0
        }

# Build lookup of lift values by Task 0 major
task1_profiles = []
for s in task1:
    task0_major = MAJOR_MAP.get(s['major'], s['major'])
    lifts = major_lifts.get(task0_major, {
        'academic_lift': 0, 'skill_lift': 0, 'experience_lift': 0
    })
    # Combine Task 1 scores with mapped Task 0 lift values
    task1_profiles.append({
        'student_key':       s['student_key'],
        'cgpa':              s['cgpa'],
        'major':             s['major'],
        'task0_major':       task0_major,
        'academic_score':    s['academic_score'],
        'skill_score':       s['skill_score'],
        'experience_score':  s['experience_score'],
        'academic_lift':     lifts['academic_lift'],
        'skill_lift':        lifts['skill_lift'],
        'experience_lift':   lifts['experience_lift'],
    })

# Collect all scores from Task 0 and Task 1 for shared normalization
all_acad  = [r['academic_score']   for r in task0] + [p['academic_score']   for p in task1_profiles]
all_skill = [r['skill_score']      for r in task0] + [p['skill_score']      for p in task1_profiles]
all_exp   = [r['experience_score'] for r in task0] + [p['experience_score'] for p in task1_profiles]

# Store min/max ranges for later normalization
NORM = {
    'acad_min':  min(all_acad),   'acad_max':  max(all_acad),
    'skill_min': min(all_skill),  'skill_max': max(all_skill),
    'exp_min':   min(all_exp),    'exp_max':   max(all_exp),
}

# # Convert one resume dict into a single text string for embedding
def resume_to_text(resume: dict) -> str:
    major      = resume.get('college', {}).get('major', '')
    cgpa       = resume.get('academics', {}).get('cgpa', '')
    courses    = ', '.join([
        c.get('course', '')
        for c in resume.get('academics', {}).get('relevant_coursework', [])
    ])
    awards     = ', '.join([
        a.get('title', '')
        for a in resume.get('academics', {}).get('honors_or_awards', [])
    ])
    tech       = ', '.join(resume.get('skills', {}).get('technical', []))
    soft       = ', '.join(resume.get('skills', {}).get('soft', []))
    projects   = ', '.join([
        p.get('name', '')
        for p in resume.get('experience', {}).get('projects', [])
    ])
    work       = ', '.join([
        w.get('title', '')
        for w in resume.get('experience', {}).get('work', [])
    ])
    activities = ', '.join([
        a.get('activity', '')
        for a in resume.get('experience', {}).get('leadership_and_activities', [])
    ])
    return (
        f'Major: {major}. CGPA: {cgpa}. '
        f'Courses: {courses}. Awards: {awards}. '
        f'Technical skills: {tech}. Soft skills: {soft}. '
        f'Projects: {projects}. Work: {work}. Activities: {activities}.'
    )

# # Scale a score to the 0–1 range
def normalize_score(value: float, min_val: float, max_val: float) -> float:
    if max_val == min_val:
        return 0.0
    return round((value - min_val) / (max_val - min_val), 6)

# Convert structured profile scores into text for embedding
def profile_to_text(major: str, academic_score: float,
                    skill_score: float, experience_score: float,
                    academic_bucket: str, skill_bucket: str,
                    experience_bucket: str) -> str:
    acad_n  = normalize_score(academic_score,   NORM['acad_min'],  NORM['acad_max'])
    skill_n = normalize_score(skill_score,       NORM['skill_min'], NORM['skill_max'])
    exp_n   = normalize_score(experience_score,  NORM['exp_min'],   NORM['exp_max'])
    return (
        f'Major: {major}. '
        f'Academic score: {acad_n:.3f} ({academic_bucket}). '
        f'Skill score: {skill_n:.3f} ({skill_bucket}). '
        f'Experience score: {exp_n:.3f} ({experience_bucket}).'
    )

# Compute placement prediction from normalized scores and lift weights
# Returns placement_prediction as a percentage (0–100).
def compute_placement_prediction(academic_score, skill_score, experience_score,
                                  academic_lift, skill_lift, experience_lift) -> float:
    # Normalize scores to 0–1
    acad_n  = normalize_score(academic_score,   NORM['acad_min'],  NORM['acad_max'])
    skill_n = normalize_score(skill_score,       NORM['skill_min'], NORM['skill_max'])
    exp_n   = normalize_score(experience_score,  NORM['exp_min'],   NORM['exp_max'])

    # Clamp negative lifts to 0
    al = max(academic_lift,   0)
    sl = max(skill_lift,      0)
    el = max(experience_lift, 0)
    total_lift = al + sl + el

    # Use equal-weight average if all lifts are 0
    if total_lift == 0:
        return round(((acad_n + skill_n + exp_n) / 3.0) * 100, 1)

    # Compute lift-weighted percentage
    raw = (acad_n * al) + (skill_n * sl) + (exp_n * el)
    return round((raw / total_lift) * 100, 1)

# Build rich-text vector collection from 20 resumes
print('Building collection_rich (20 resume-text vectors)...')
texts_rich      = [resume_to_text(r) for r in resumes]
ids_rich        = [r['student_key'] for r in resumes]
embeddings_rich = model.encode(texts_rich).tolist()
metadatas_rich  = [
    {
        'student_key': r['student_key'],
        'major':       r['college']['major'],
        'cgpa':        str(r['academics']['cgpa']),
        'source':      'task1_resume'
    }
    for r in resumes
]

# Store rich resume embeddings in ChromaDB
collection_rich.add(
    ids=ids_rich,
    embeddings=embeddings_rich,
    metadatas=metadatas_rich,
    documents=texts_rich
)

print(f'collection_rich loaded: {collection_rich.count()} documents')

# Build broad vector collection from Task 0 score profiles
print('\nBuilding collection_broad (2,000 score-profile vectors)...')

texts_broad, ids_broad, embeddings_broad, metadatas_broad = [], [], [], []

BATCH_SIZE = 200
for i, r in enumerate(task0):
    # Convert each Task 0 record into normalized profile text
    text = profile_to_text(
        r['major'],
        r['academic_score'],
        r['skill_score'],
        r['experience_score'],
        r['academic_bucket'],
        r['skill_bucket'],
        r['experience_bucket']
    )
    # Compute structured placement prediction
    pp = compute_placement_prediction(
        r['academic_score'],   r['skill_score'],   r['experience_score'],
        r['academic_lift'] or 0, r['skill_lift'] or 0, r['experience_lift'] or 0
    )
    texts_broad.append(text)
    ids_broad.append(r['student_key'])
    metadatas_broad.append({
        'student_key':          r['student_key'],
        'major':                r['major'],
        'placed':               str(r['placed']),
        'academic_bucket':      r['academic_bucket'],
        'skill_bucket':         r['skill_bucket'],
        'experience_bucket':    r['experience_bucket'],
        'placement_prediction': str(pp),
        'source':               'task0_profile'
    })

# Add broad profiles to ChromaDB in batches
for start in range(0, len(texts_broad), BATCH_SIZE):
    batch_texts = texts_broad[start:start + BATCH_SIZE]
    batch_embs  = model.encode(batch_texts).tolist()
    collection_broad.add(
        ids=ids_broad[start:start + BATCH_SIZE],
        embeddings=batch_embs,
        metadatas=metadatas_broad[start:start + BATCH_SIZE],
        documents=batch_texts
    )
    print(f'  Embedded {min(start + BATCH_SIZE, len(texts_broad))}/{len(texts_broad)}')

print(f'collection_broad loaded: {collection_broad.count()} documents')


# Populate PostgreSQL table with all Task 1 and Task 0 students
print('\nPopulating PostgreSQL task3_students...')

# Insert task1 students
for profile in task1_profiles:
    pp = compute_placement_prediction(
        profile['academic_score'],   profile['skill_score'],   profile['experience_score'],
        profile['academic_lift'],    profile['skill_lift'],    profile['experience_lift']
    )
    cur.execute('''
        INSERT INTO proj.task3_students
        (student_key, cgpa, major, academic_score, skill_score,
         experience_score, academic_lift, skill_lift, experience_lift,
         placement_prediction)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (student_key) DO UPDATE SET
            placement_prediction = EXCLUDED.placement_prediction
    ''', (
        profile['student_key'], profile['cgpa'],
        profile['major'],       profile['academic_score'],
        profile['skill_score'], profile['experience_score'],
        profile['academic_lift'], profile['skill_lift'],
        profile['experience_lift'], pp
    ))

# Insert task0 students
for r in task0:
    pp = compute_placement_prediction(
        r['academic_score'],      r['skill_score'],      r['experience_score'],
        r['academic_lift'] or 0,  r['skill_lift'] or 0,  r['experience_lift'] or 0
    )
    cur.execute('''
        INSERT INTO proj.task3_students
        (student_key, cgpa, major, academic_score, skill_score,
         experience_score, academic_lift, skill_lift, experience_lift,
         placement_prediction)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (student_key) DO UPDATE SET
            placement_prediction = EXCLUDED.placement_prediction
    ''', (
        r['student_key'], None,
        r['major'],       r['academic_score'],
        r['skill_score'], r['experience_score'],
        r['academic_lift'] or 0, r['skill_lift'] or 0,
        r['experience_lift'] or 0, pp
    ))

# Save all inserts to PostgreSQL
conn.commit()
print('PostgreSQL task3_students populated')

# Find the most similar resumes from the 20 rich-text profiles
def find_rich_similar(query_resume: dict, k: int = 5) -> list:
    query_text = resume_to_text(query_resume)
    query_emb  = model.encode([query_text]).tolist()
    student_key = query_resume['student_key']

    # Query ChromaDB and exclude the same student
    results = collection_rich.query(
        query_embeddings=query_emb,
        n_results=k + 1,                          # fetch one extra to account for self
        where={'student_key': {'$ne': student_key}},  # exclude self
        include=['distances', 'metadatas']
    )
    
    # Return top k matches with key metadata
    return [
        {
            'student_key': results['ids'][0][i],
            'distance':    round(results['distances'][0][i], 4),
            'major':       results['metadatas'][0][i]['major'],
            'cgpa':        results['metadatas'][0][i]['cgpa'],
            'collection':  'rich (resume text)',
        }
        for i in range(min(k, len(results['ids'][0])))  # cap at k
    ]

# Find the most similar Task 0 profiles from the broad collection
def find_broad_similar(academic_score: float, skill_score: float,
                        experience_score: float,
                        academic_bucket: str, skill_bucket: str,
                        experience_bucket: str, major: str,
                        k: int = 5, filter_major: bool = True) -> list:
    # Build query text using the same format as stored profiles
    query_text = profile_to_text(
        MAJOR_MAP.get(major, major),
        academic_score, skill_score, experience_score,
        academic_bucket, skill_bucket, experience_bucket
    )
    query_emb = model.encode([query_text]).tolist()

    # Optionally restrict search to the same mapped major
    where = {'major': MAJOR_MAP.get(major, major)} if filter_major else None

    # Query ChromaDB for nearest score-profile matches
    results = collection_broad.query(
        query_embeddings=query_emb,
        n_results=k,
        where=where,
        include=['distances', 'metadatas']
    )
    
    # Return matches with profile metadata
    return [
        {
            'student_key':       results['ids'][0][i],
            'distance':          round(results['distances'][0][i], 4),
            'major':             results['metadatas'][0][i]['major'],
            'placed':            results['metadatas'][0][i]['placed'],
            'academic_bucket':   results['metadatas'][0][i]['academic_bucket'],
            'skill_bucket':      results['metadatas'][0][i]['skill_bucket'],
            'experience_bucket': results['metadatas'][0][i]['experience_bucket'],
            'placement_prediction': results['metadatas'][0][i]['placement_prediction'],
            'collection':        'broad (score profile)',
        }
        for i in range(len(results['ids'][0]))
    ]

# Find students with higher placement prediction than the query student
def find_stronger_peers(query_pp: float, query_major: str = None,
                         limit: int = 3) -> list:
    if query_major:
        task0_major = MAJOR_MAP.get(query_major, query_major)
        # Search only within the same mapped major
        cur.execute('''
            SELECT student_key, cgpa, major, academic_score,
                   skill_score, experience_score, placement_prediction
            FROM proj.task3_students
            WHERE placement_prediction > %s
              AND major = %s
            ORDER BY placement_prediction DESC
            LIMIT %s
        ''', (query_pp, task0_major, limit))
    else:
        # Search across all students
        cur.execute('''
            SELECT student_key, cgpa, major, academic_score,
                   skill_score, experience_score, placement_prediction
            FROM proj.task3_students
            WHERE placement_prediction > %s
            ORDER BY placement_prediction DESC
            LIMIT %s
        ''', (query_pp, limit))
    rows = cur.fetchall()
    cols = ['student_key', 'cgpa', 'major', 'academic_score',
            'skill_score', 'experience_score', 'placement_prediction']
    
    # Convert SQL rows into dictionaries
    return [dict(zip(cols, row)) for row in rows]

# Compute percentile rank based on placement prediction
def compute_placement_percentile(query_pp: float) -> dict:
    # Count total students and how many are below the query score
    cur.execute('''
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN placement_prediction < %s THEN 1 ELSE 0 END) AS below
        FROM proj.task3_students
    ''', (query_pp,))
    row = cur.fetchone()
    total, below = row[0], row[1]
    percentile = round((below / total) * 100, 1) if total > 0 else 0
    return {'percentile': percentile, 'below': below, 'total': total}

# Run the full Task 3 comparison workflow for one student
def run_task3(query_resume: dict, query_profile: dict):
    sk    = query_profile['student_key']
    major = query_profile['major']
    cgpa  = query_profile['cgpa']

    # Get Task 1 bucket labels for the query student
    t1_record    = next((s for s in task1 if s['student_key'] == sk), None)
    acad_bucket  = t1_record['academic_bucket']   if t1_record else 'Medium'
    skill_bucket = t1_record['skill_bucket']      if t1_record else 'Medium'
    exp_bucket   = t1_record['experience_bucket'] if t1_record else 'Medium'

    # Compute the student's placement prediction
    query_pp = compute_placement_prediction(
        query_profile['academic_score'],   query_profile['skill_score'],
        query_profile['experience_score'], query_profile['academic_lift'],
        query_profile['skill_lift'],       query_profile['experience_lift']
    )

    # Print the query student's summary
    print(f'Student Key — {sk} | {major} | CGPA: {cgpa}')
    print(f'Scores: acad={query_profile["academic_score"]}  '
          f'skill={query_profile["skill_score"]}  '
          f'exp={query_profile["experience_score"]}')
    print(f'Buckets: {acad_bucket} / {skill_bucket} / {exp_bucket}')
    print(f'placement_prediction: {query_pp}%')

    # Output 1 - ChromaDB
    # Find top 5 similar students from the rich resume-text collection
    rich_results = find_rich_similar(query_resume, k=5)
    rich_keys    = tuple(r['student_key'] for r in rich_results)
    # Fetch structured scores for those matched students
    cur.execute('''
        SELECT student_key, academic_score, skill_score,
               experience_score, placement_prediction
        FROM proj.task3_students WHERE student_key IN %s
    ''', (rich_keys,))
    rich_scores = {row[0]: row[1:] for row in cur.fetchall()}

    # Build Output 1 table
    df1 = pd.DataFrame(rich_results)
    df1['academic_score']       = df1['student_key'].map(lambda k: rich_scores.get(k, (None,)*4)[0])
    df1['skill_score']          = df1['student_key'].map(lambda k: rich_scores.get(k, (None,)*4)[1])
    df1['experience_score']     = df1['student_key'].map(lambda k: rich_scores.get(k, (None,)*4)[2])
    df1['placement_prediction'] = df1['student_key'].map(
        lambda k: f'{rich_scores.get(k, (None,)*4)[3]}%'
        if rich_scores.get(k, (None,)*4)[3] is not None else 'N/A'
    )
    df1 = df1.sort_values('distance').reset_index(drop=True)

    # Print output
    print('\nOUTPUT 1 — Top 5 Similar Resume Peers (collection_rich)')
    print('Question: "Which of the 20 detailed resume peers is most similar to me?"')
    print('Method:   Full resume text embedding (semantic similarity)')
    print(df1[['student_key', 'distance', 'major', 'cgpa',
               'academic_score', 'skill_score',
               'experience_score', 'placement_prediction']].to_string(index=False))

    # Output 2
    # Find stronger peers from PostgreSQL
    stronger   = find_stronger_peers(query_pp, query_major=major, limit=3)
    percentile = compute_placement_percentile(query_pp)

    # Print
    print(f'\nOUTPUT 2 — Top 3 Stronger Comparison Students (PostgreSQL)')
    print(f'Question: "Who has better placement odds than me in '
          f'{MAJOR_MAP.get(major, major)}?"')
    print(f'NOTE: Queried across ALL 2,020 students — not filtered from Output 1.')
    print(f'Your placement percentile: top '
          f'{100 - percentile["percentile"]:.1f}% '
          f'(outperform {percentile["below"]} of {percentile["total"]} students)')

    # Build stronger-peer comparison table
    if stronger:
        df2 = pd.DataFrame(stronger)
        df2['placement_prediction'] = df2['placement_prediction'].apply(
            lambda x: f'{float(x):.1f}%' if x is not None else 'N/A'
        )
        print(df2[['student_key', 'major', 'academic_score',
                   'skill_score', 'experience_score',
                   'placement_prediction']].to_string(index=False))

        # Compare the query student to the top stronger peer
        top = stronger[0]
        print('\nIMPROVEMENT DIRECTION (vs top stronger peer):')
        for col, label in [
            ('academic_score',   'Academic Score '),
            ('skill_score',      'Skill Score    '),
            ('experience_score', 'Experience Score')
        ]:
            mine   = query_profile.get(col, 0)
            theirs = float(top[col])
            # Normalize scores before comparing gaps
            if col == 'academic_score':
                mine_n   = normalize_score(mine,   NORM['acad_min'],  NORM['acad_max'])
                theirs_n = normalize_score(theirs, NORM['acad_min'],  NORM['acad_max'])
            elif col == 'skill_score':
                mine_n   = normalize_score(mine,   NORM['skill_min'], NORM['skill_max'])
                theirs_n = normalize_score(theirs, NORM['skill_min'], NORM['skill_max'])
            else:
                mine_n   = normalize_score(mine,   NORM['exp_min'],   NORM['exp_max'])
                theirs_n = normalize_score(theirs, NORM['exp_min'],   NORM['exp_max'])
            # Print normalized score gap
            diff  = round((theirs_n - mine_n) * 100, 1)
            arrow = '▲' if diff > 0 else '✓'
            print(f'  {arrow}  {label}: you={mine_n*100:.1f}%  |  '
                  f'peer={theirs_n*100:.1f}%  |  gap={diff:+.1f}%')
    else:
        # Case where no stronger peer exists
        print('You already outperform all students in this major by placement_prediction!')

# Demonstration
# student_key = u_0007
query_resume_7  = next(r for r in resumes if r['student_key'] == 'u_0007')
query_profile_7 = next(p for p in task1_profiles if p['student_key'] == 'u_0007')
run_task3(query_resume_7, query_profile_7)

# Demonstration
# student_key = u_0020
query_resume_20  = next(r for r in resumes if r['student_key'] == 'u_0020')
query_profile_20 = next(p for p in task1_profiles if p['student_key'] == 'u_0020')
run_task3(query_resume_20, query_profile_20)