from openai import AzureOpenAI
from flask import Flask, request, render_template, redirect, url_for, flash, jsonify, send_file, make_response
import os
import sqlite3
import tempfile
import PyPDF2
import docx
import uuid
from werkzeug.utils import secure_filename
import json
from datetime import datetime, timedelta
from jinja2 import Template
import pdfkit
import pymssql
# Import the generate_cv function from cv_generator.py
from cv_generator import generate_cv
from collections import defaultdict
from flask_mail import Mail, Message

# Create a function to establish database connection
def get_db_connection():
    try:
        conn = pymssql.connect(
            server='cv-analysis-server.database.windows.net',
            user='cv-analysis-admin',
            password='Bloesemstraat14a.',
            database='cv-analysis-db'
        )
        
        # Test the connection
        cursor = conn.cursor()
        cursor.execute("SELECT 1 AS test_column")  # Give the column a name
        result = cursor.fetchone()
        if result and result[0] == 1:
            print("Database connection successful")
        else:
            print("Database connection test failed")
        cursor.close()
        
        return conn
    except Exception as e:
        print(f"Database connection error: {str(e)}")
        import traceback
        print(traceback.format_exc())
        raise

# Initialize Azure OpenAI client


# Initialize Flask app
app = Flask(__name__)
app.secret_key = "your_secret_key"
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max upload size
ALLOWED_EXTENSIONS = {'pdf', 'docx', 'doc', 'txt'}

# Create uploads folder if it doesn't exist
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# Initialize database and normalize data
def initialize_database():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Perform database initialization
        # Check if candidates table exists first
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'candidates')
            BEGIN
                CREATE TABLE candidates (
                    id VARCHAR(255) PRIMARY KEY,
                    name NVARCHAR(255),
                    email NVARCHAR(255),
                    phone NVARCHAR(50),
                    cv_text NVARCHAR(MAX),
                    wensen NVARCHAR(MAX),
                    ambities NVARCHAR(MAX),
                    professional_role NVARCHAR(255)
                )
            END
        """)
        
        # Create work_experience table if it doesn't exist
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'work_experience')
            BEGIN
                CREATE TABLE work_experience (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    candidate_id VARCHAR(255),
                    company NVARCHAR(255),
                    position NVARCHAR(255),
                    start_date NVARCHAR(50),
                    end_date NVARCHAR(50),
                    description NVARCHAR(MAX),
                    FOREIGN KEY (candidate_id) REFERENCES candidates (id)
                )
            END
        """)
        
        # Create skills table if it doesn't exist
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'skills')
            BEGIN
                CREATE TABLE skills (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    candidate_id VARCHAR(255),
                    skill_name NVARCHAR(255),
                    years_experience FLOAT,
                    is_starred BIT DEFAULT 0,
                    FOREIGN KEY (candidate_id) REFERENCES candidates (id)
                )
            END
        """)
        
        # Create certificates table if it doesn't exist
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'certificates')
            BEGIN
                CREATE TABLE certificates (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    candidate_id VARCHAR(255),
                    name NVARCHAR(255),
                    issuer NVARCHAR(255),
                    date_obtained NVARCHAR(50),
                    expiry_date NVARCHAR(50),
                    description NVARCHAR(MAX),
                    FOREIGN KEY (candidate_id) REFERENCES candidates (id)
                )
            END
        """)
        
        # Create search_history table if it doesn't exist
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'search_history')
            BEGIN
                CREATE TABLE search_history (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    search_term NVARCHAR(255),
                    search_timestamp DATETIME DEFAULT GETDATE(),
                    results_count INT
                )
            END
        """)
        
        # Add timestamp columns if they don't exist
        cursor.execute("""
            IF NOT EXISTS (
                SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'candidates' AND COLUMN_NAME = 'created_at'
            )
            BEGIN
                ALTER TABLE candidates 
                ADD created_at DATETIME DEFAULT GETDATE(),
                    updated_at DATETIME DEFAULT GETDATE()
            END
        """)
        
        # Add professional_role column if it doesn't exist
        cursor.execute("""
            IF NOT EXISTS (
                SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'candidates' AND COLUMN_NAME = 'professional_role'
            )
            BEGIN
                ALTER TABLE candidates 
                ADD professional_role NVARCHAR(255) NULL
            END
        """)
        
        # Normalize existing data
        cursor.execute("""
            UPDATE skills
            SET skill_name = REPLACE(skill_name, N'–', '-')
            WHERE skill_name LIKE N'%–%'
        """)
        
        cursor.execute("""
            UPDATE certificates
            SET name = REPLACE(name, N'–', '-')
            WHERE name LIKE N'%–%'
        """)
        
        conn.commit()
    finally:
        conn.close()

# Replace the multiple init calls with a single initialization
initialize_database()

# Initialize at app startup
mail = Mail(app)

# Configure Flask-Mail with Gmail settings
app.config['MAIL_SERVER'] = 'smtp.gmail.com'
app.config['MAIL_PORT'] = 587
app.config['MAIL_USE_TLS'] = True
app.config['MAIL_USERNAME'] = 'doeielies@gmail.com'
app.config['MAIL_PASSWORD'] = 'gkeo qhis eicz hezj'  
app.config['MAIL_DEFAULT_SENDER'] = 'doeielies@gmail.com'

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def extract_text_from_file(file_path):
    """Extract text from PDF, DOCX, or TXT files"""
    file_extension = file_path.split('.')[-1].lower()
    
    if file_extension == 'pdf':
        text = ""
        with open(file_path, 'rb') as file:
            pdf_reader = PyPDF2.PdfReader(file)
            for page in pdf_reader.pages:
                text += page.extract_text() + "\n"
        return text
    
    elif file_extension in ['docx', 'doc']:
        doc = docx.Document(file_path)
        return "\n".join([paragraph.text for paragraph in doc.paragraphs])
    
    elif file_extension == 'txt':
        with open(file_path, 'r', encoding='utf-8') as file:
            return file.read()
    
    return ""

def get_cv_analysis_prompt(cv_text):
    return f"""Please analyze this CV text and extract the following information in JSON format. Determine the candidate's primary professional role (e.g., Data Engineer, Cloud Consultant) based on their recent experience and overall profile. For each Job listed in the CV, include the full job description exactly as it appears in the CV under work_experience; do not summarize it. Extract skills and add them to the skills section. For each certificate, analyze what technologies or skills it represents and add those to the skills section.

    {{
        "name": "candidate's full name",
        "email": "email address",
        "phone": "phone number",
        "professional_role": "primary job title or role (e.g., Data Engineer, Cloud Consultant)",
        "work_experience": [
            {{
                "company": "company name",
                "position": "job title",
                "start_date": "start date",
                "end_date": "end date or 'Present'",
                "description": "full job description as written in the CV, do not summarize"
            }}
        ],
        "skills": [
            {{
                "skill_name": "name of skill",
                "years_experience": "number of years (decimal)"
            }}
        ],
        "certificates": [
            {{
                "name": "EXACT name of certification/training (do not translate or modify)",
                "issuer": "issuing organization/institution",
                "date_obtained": "date obtained (if available)",
                "expiry_date": "expiration date (if applicable)",
                "description": "brief description or additional details",
                "related_skills": ["list of skills this certificate represents"]
            }}
        ]
    }}
    1. All dates are in YYYY-MM format when possible
    2. Years of experience are numbers (can be decimals)
    3. Include all relevant certifications, professional qualifications, and training programs
    4. If certain fields are not available, use empty strings or null
    5. For ongoing certifications, use 'Present' for expiry_date
    6. Analyze each certification and add corresponding skills to the skills section. For example:
       - DP-600 certification should add "Microsoft Fabric" and "Data Analytics" to skills
       - AWS Solutions Architect should add "AWS", "Cloud Architecture" to skills
       - CISSP should add "Information Security", "Cybersecurity" to skills
    7. When adding certificate-based skills, set the years_experience to match the time since certification obtained
    8. Ensure no duplicate skills are added (merge and take the highest years of experience if found in multiple sources)
    Please ensure for certificates:
    1. Keep the EXACT original name of the certification (do not translate)
    2. Do not use generic terms like "zelfstandig werken" as certificate names
    3. For Microsoft certifications, use the exact code (e.g., "AZ-900", "DP-600")
    4. For professional certifications, use the exact name (e.g., "PRINCE2 Foundation", "Scrum Master")

    Example of good certificate entries:
    - "Microsoft Azure Data Engineer Associate (DP-203)"
    - "AWS Solutions Architect Professional"
    - "PRINCE2 Foundation"
    - "Certified ScrumMaster (CSM)"

    CV Text:
    {cv_text}
    """

def process_cv_response(gpt_response):
    """Process the GPT response and merge any duplicate skills"""
    data = json.loads(gpt_response)
    
    # Filter out any certificates that look like generic skills
    generic_terms = {'zelfstandig werken', 'communicatie', 'teamwork', 'leadership'}
    data['certificates'] = [
        cert for cert in data.get('certificates', [])
        if cert.get('name', '').lower() not in generic_terms
    ]
    
    # Create a dictionary to track skills and their maximum years of experience
    skills_dict = {}
    
    # Process existing skills
    for skill in data.get('skills', []):
        skill_name = skill['skill_name'].lower()
        years = float(skill.get('years_experience', 0))
        skills_dict[skill_name] = max(skills_dict.get(skill_name, 0), years)
    
    # Process skills from certificates
    for cert in data.get('certificates', []):
        cert_date = cert.get('date_obtained', '')
        if cert_date:
            try:
                # Calculate years of experience based on certification date
                cert_date = datetime.strptime(cert_date, '%Y-%m')
                years_since_cert = (datetime.now() - cert_date).days / 365.25
                
                # Add related skills with years since certification
                for skill in cert.get('related_skills', []):
                    skill_name = skill.lower()
                    skills_dict[skill_name] = max(skills_dict.get(skill_name, 0), years_since_cert)
            except ValueError:
                # Handle invalid date format
                pass
    
    # Convert back to list format
    data['skills'] = [
        {'skill_name': skill, 'years_experience': round(years, 1)}
        for skill, years in skills_dict.items()
    ]
    
    return data

def analyze_cv_with_ai(cv_text):
    """Use Azure OpenAI to extract information from CV"""
    prompt = get_cv_analysis_prompt(cv_text)
    
    response = client.chat.completions.create(
        model="gpt-4",  # Use the appropriate model available in your Azure OpenAI deployments
        messages=[
            {"role": "system", "content": "You are a helpful assistant that extracts structured information from CVs. Use Dutch terms only. Try to extract as many skills as possible. Voeg Standaard skills toe als deze niet in de CV staan. Zoals Analytische vaardigheden, communicatievaardigheden, Nederlands, Engels, zelfstandig werken, etc. Kunstmatige Intelligentie is AI/Artificial Intelligence."},
            {"role": "user", "content": prompt}
        ],
        response_format={"type": "json_object"}
    )
    
    return response.choices[0].message.content

def save_to_database(cv_data):
    """Save the extracted CV data to the database"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    candidate_id = str(uuid.uuid4())
    
    # Helper function to validate certificate
    def is_valid_certificate(cert_name):
        if not cert_name:
            return False
        
        # List of terms that indicate generic skills rather than certificates
        generic_terms = {
            'zelfstandig werken', 'communicatie', 'teamwork', 'leadership',
            'werken', 'skill', 'competentie', 'vaardigheid'
        }
        
        return (
            cert_name.lower() not in generic_terms and
            len(cert_name) > 3 and  # Avoid very short names
            any(char.isalnum() for char in cert_name)  # Must contain at least one alphanumeric character
        )
    
    # Insert candidate information, including professional_role
    cursor.execute(
        """INSERT INTO candidates 
           (id, name, email, phone, cv_text, wensen, ambities, professional_role) 
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
        (candidate_id, cv_data.get('name', ''), cv_data.get('email', ''), 
         cv_data.get('phone', ''), cv_data.get('cv_text', ''), 
         cv_data.get('wensen', ''), cv_data.get('ambities', ''),
         cv_data.get('professional_role', ''))
    )
    
    # Insert work experience
    for exp in cv_data.get('work_experience', []):
        cursor.execute(
            "INSERT INTO work_experience (candidate_id, company, position, start_date, end_date, description) VALUES (%s, %s, %s, %s, %s, %s)",
            (candidate_id, exp.get('company', ''), exp.get('position', ''), 
             exp.get('start_date', ''), exp.get('end_date', ''), exp.get('description', ''))
        )
    
    # Insert skills with normalized names
    for skill in cv_data.get('skills', []):
        cursor.execute(
            "INSERT INTO skills (candidate_id, skill_name, years_experience) VALUES (%s, %s, %s)",
            (candidate_id, skill.get('skill_name', ''), skill.get('years_experience', 0))
        )
    
    # Insert certificates/trainings with validation
    for cert in cv_data.get('certificates', []):
        cert_name = cert.get('name', '').strip()
        if is_valid_certificate(cert_name):
            cursor.execute("""
                INSERT INTO certificates (candidate_id, name, issuer, date_obtained, 
                                       expiry_date, description)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                candidate_id,
                cert_name,
                cert.get('issuer', ''),
                cert.get('date_obtained', ''),
                cert.get('expiry_date', ''),
                cert.get('description', '')
            ))
    
    conn.commit()
    conn.close()
    
    return candidate_id

@app.route('/')
def index():
    return redirect(url_for('dashboard'))

@app.route('/dashboard')
def dashboard():
    conn = get_db_connection()
    # Create a cursor with dictionary results for this specific function
    cursor = conn.cursor(as_dict=True)

    # Get total candidates
    cursor.execute("SELECT COUNT(*) AS count FROM candidates")
    total_candidates = cursor.fetchone()['count']

    # Get unique skills count
    cursor.execute("SELECT COUNT(DISTINCT skill_name) AS count FROM skills")
    unique_skills = cursor.fetchone()['count']
    
    # Get most experienced skill (highest average years)
    cursor.execute("""
        SELECT TOP 1 skill_name, AVG(years_experience) as avg_exp
        FROM skills
        GROUP BY skill_name
        HAVING COUNT(*) > 2  -- Only consider skills that appear multiple times
        ORDER BY avg_exp DESC
    """)
    most_exp_skill = cursor.fetchone()
    most_experienced_skill = {
        'name': most_exp_skill['skill_name'] if most_exp_skill else 'None',
        'years': round(most_exp_skill['avg_exp'], 1) if most_exp_skill else 0
    }
    
    # Get candidate with most skills
    cursor.execute("""
        SELECT TOP 1 c.name, COUNT(s.id) as skill_count
        FROM candidates c
        JOIN skills s ON c.id = s.candidate_id
        GROUP BY c.id, c.name
        ORDER BY skill_count DESC
    """)
    most_skilled = cursor.fetchone()
    most_skilled_candidate = {
        'name': most_skilled['name'] if most_skilled else 'None',
        'count': most_skilled['skill_count'] if most_skilled else 0
    }

    # Get top skills
    cursor.execute("""
        SELECT TOP 6 skill_name, COUNT(*) as count
        FROM skills
        GROUP BY skill_name
        ORDER BY count DESC
    """)
    skills_data = cursor.fetchall()
    
    # Calculate percentages for skills
    max_count = max([skill['count'] for skill in skills_data]) if skills_data else 1
    top_skills = [
        {
            'name': skill['skill_name'],
            'count': skill['count'],
            'percentage': (skill['count'] / max_count) * 100
        }
        for skill in skills_data
    ]
    
    # Get top candidates with their most frequent skill
    cursor.execute("""
        SELECT 
            c.id, 
            c.name, 
            c.email,
            (SELECT STRING_AGG(CONCAT(s2.skill_name, ':', CAST(s2.years_experience AS VARCHAR)), ',') 
             FROM skills s2 
             WHERE s2.candidate_id = c.id) as skills
        FROM candidates c
    """)
    candidates = cursor.fetchall()
    
    # Process to get top skill for each candidate
    candidate_top_skills = {}
    for row in candidates:
        if row['id'] not in candidate_top_skills and row['skills']:
            candidate_top_skills[row['id']] = {
                'name': row['name'],
                'top_skill': row['skills'].split(',')[0] if row['skills'] else 'None'
            }
    
    # Get top 15 candidates (or all if less than 15)
    top_candidates = list(candidate_top_skills.values())[:15]
    
    # Get recent activities
    recent_activities = []
    
    # Get most recent candidates
    cursor.execute("""
        SELECT TOP 4 id, name
        FROM candidates
        ORDER BY id DESC
    """)
    recent_candidates = cursor.fetchall()
    
    # Create activity entries for recent candidates
    for candidate in recent_candidates:
        recent_activities.append({
            'icon': 'file-earmark-person',
            'title': 'Nieuw kandidaatprofiel aangemaakt',
            'description': f"{candidate['name']} is toegevoegd aan de database",
            'time': 'Recent'
        })

    # Calculate candidates added this month
    current_month = datetime.now().month
    current_year = datetime.now().year
    cursor.execute("""
        SELECT COUNT(*) as count FROM candidates
        WHERE MONTH(created_at) = %s AND YEAR(created_at) = %s
    """, (current_month, current_year))
    candidates_added_this_month = cursor.fetchone()['count']

    conn.close()

    return render_template('dashboard.html',
                         total_candidates=total_candidates,
                         unique_skills=unique_skills,
                         most_experienced_skill=most_experienced_skill,
                         most_skilled_candidate=most_skilled_candidate,
                         top_skills=top_skills,
                         top_candidates=top_candidates,
                         recent_activities=recent_activities,
                         candidates_added_this_month=candidates_added_this_month)

@app.route('/upload')
def upload_page():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'files[]' not in request.files:
        flash('No file part')
        return redirect(request.url)
    
    files = request.files.getlist('files[]')
    
    if not files or files[0].filename == '':
        flash('No selected file')
        return redirect(request.url)
    
    processed_count = 0
    for file in files:
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)
            
            try:
                # Extract text from the file
                cv_text = extract_text_from_file(file_path)
                print(f"Extracted text from {filename}: {len(cv_text)} characters")
                
                # Analyze the CV with Azure OpenAI
                response = analyze_cv_with_ai(cv_text)
                print(f"Received AI analysis for {filename}")
                
                processed_data = process_cv_response(response)
                processed_data['cv_text'] = cv_text  # Add the original text for reference
                
                # Save to database - wrap in try/except to catch specific database errors
                try:
                    candidate_id = save_to_database(processed_data)
                    print(f"Saved to database with ID: {candidate_id}")
                    processed_count += 1
                except Exception as db_error:
                    print(f"Database error for {filename}: {str(db_error)}")
                    import traceback
                    print(traceback.format_exc())
                    flash(f'Database error processing {filename}: {str(db_error)}')
                
            except Exception as e:
                print(f"Error processing {filename}: {str(e)}")
                import traceback
                print(traceback.format_exc())
                flash(f'Error processing {filename}: {str(e)}')
            
            # Clean up the uploaded file
            os.remove(file_path)
    
    if processed_count > 0:
        flash(f'Successfully processed {processed_count} CV(s)')
    else:
        flash('Files were processed but no candidates were saved to the database')
    
    return redirect(url_for('view_candidates'))

@app.route('/candidates')
def view_candidates():
    search_query = request.args.get('q', '')
    search_terms = [term.strip() for term in search_query.split() if term.strip()]
    
    conn = get_db_connection()
    cursor = conn.cursor(as_dict=True)
    
    # Get total count of all candidates
    cursor.execute("SELECT COUNT(*) as count FROM candidates")
    total_candidates = cursor.fetchone()['count']
    
    # Get average skills per candidate
    cursor.execute("""
        SELECT CAST(COUNT(s.id) AS FLOAT) / NULLIF(COUNT(DISTINCT s.candidate_id), 0) as avg_skills
        FROM skills s
    """)
    result = cursor.fetchone()
    avg_skills_per_candidate = round(result['avg_skills'], 1) if result and result['avg_skills'] else 0
    
    # Calculate candidates added this month
    current_month = datetime.now().month
    current_year = datetime.now().year
    cursor.execute("""
        SELECT COUNT(*) as count FROM candidates
        WHERE MONTH(created_at) = %s AND YEAR(created_at) = %s
    """, (current_month, current_year))
    candidates_added_this_month = cursor.fetchone()['count']
    
    # Fetch candidates based on search query
    if search_terms:
        # Build a query that finds candidates matching ALL search terms
        query_parts = []
        params = []
        
        for term in search_terms:
            term_param = f'%{term}%'
            query_parts.append("""
                (
                    LOWER(c.name) LIKE LOWER(%s) OR 
                    LOWER(c.email) LIKE LOWER(%s) OR 
                    EXISTS (
                        SELECT 1 FROM skills s 
                        WHERE s.candidate_id = c.id AND LOWER(s.skill_name) LIKE LOWER(%s)
                    )
                )
            """)
            params.extend([term_param, term_param, term_param])
        
        # Join all conditions with AND to require all terms
        query = f"""
            SELECT DISTINCT c.id, c.name, c.email, c.phone 
            FROM candidates c
            WHERE {" AND ".join(query_parts)}
            ORDER BY c.name
        """
        
        cursor.execute(query, params)
    else:
        cursor.execute("SELECT id, name, email, phone FROM candidates ORDER BY name")
    
    candidates = cursor.fetchall()
    
    # For each candidate, fetch their top skills
    for candidate in candidates:
        cursor.execute("""
            SELECT TOP 5 skill_name, years_experience 
            FROM skills 
            WHERE candidate_id = %s 
            ORDER BY is_starred DESC, years_experience DESC
        """, (candidate['id'],))
        candidate['skills'] = cursor.fetchall()
        
        # Check if assignments table exists 
        cursor.execute("""
            SELECT CASE WHEN EXISTS(
                SELECT * FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME = 'assignments'
            ) THEN 1 ELSE 0 END AS table_exists
        """)
        
        if cursor.fetchone()['table_exists'] == 1:
            # Table exists, get real availability data
            cursor.execute("""
                SELECT 
                    CASE 
                        WHEN EXISTS (
                            SELECT 1 FROM assignments 
                            WHERE consultant_id = %s 
                            AND status = 'Active'
                            AND end_date >= GETDATE()
                        ) THEN (
                            SELECT MIN(end_date)
                            FROM assignments 
                            WHERE consultant_id = %s 
                            AND status = 'Active'
                            AND end_date >= GETDATE()
                        )
                        ELSE NULL
                    END as next_available_date,
                    (
                        SELECT SUM(hours_per_week)
                        FROM assignments 
                        WHERE consultant_id = %s 
                        AND status = 'Active'
                        AND end_date >= GETDATE()
                    ) as current_hours
                """, (candidate['id'], candidate['id'], candidate['id']))
            
            availability_info = cursor.fetchone()
            
            # Fetch availability preferences
            cursor.execute("""
                SELECT max_hours_per_week
                FROM availability_preferences
                WHERE consultant_id = %s
            """, (candidate['id'],))
            prefs = cursor.fetchone()
            max_hours = prefs['max_hours_per_week'] if prefs else 40
            
            # Set availability status
            if not availability_info['next_available_date'] or not availability_info['current_hours']:
                candidate['availability_status'] = 'available'
                candidate['next_available'] = None
            elif availability_info['current_hours'] < max_hours:
                candidate['availability_status'] = 'partially'
                candidate['next_available'] = availability_info['next_available_date']
            else:
                candidate['availability_status'] = 'busy'
                candidate['next_available'] = availability_info['next_available_date']
        else:
            # Table doesn't exist, use default
            candidate['availability_status'] = 'available'
            candidate['next_available'] = None
    
    # Get all unique skills for the datalist
    cursor.execute("""
        SELECT DISTINCT skill_name
        FROM skills
        ORDER BY skill_name
    """)
    all_skills = [row['skill_name'] for row in cursor.fetchall()]
    
    conn.close()
    
    return render_template('candidates.html',
                         candidates=candidates,
                         total_candidates=total_candidates,
                         avg_skills_per_candidate=avg_skills_per_candidate,
                         search_query=search_query,
                         all_skills=all_skills,
                         candidates_added_this_month=candidates_added_this_month)

@app.route('/candidate/<candidate_id>')
def candidate_details(candidate_id):
    conn = get_db_connection()
    cursor = conn.cursor(as_dict=True)
    
    # Fetch candidate including professional_role, wensen, and ambities
    cursor.execute("""
        SELECT id, name, email, phone, cv_text, wensen, ambities, professional_role, updated_at
        FROM candidates 
        WHERE id = %s
    """, (candidate_id,))
    
    candidate = cursor.fetchone()
    
    if not candidate:
        flash('Candidate not found', 'error')
        return redirect(url_for('view_candidates'))

    # Fetch other candidate data (skills, work_experience, certificates, etc.)
    cursor.execute("SELECT * FROM work_experience WHERE candidate_id = %s ORDER BY start_date DESC", (candidate_id,))
    work_experience = cursor.fetchall()
    
    cursor.execute("SELECT * FROM skills WHERE candidate_id = %s ORDER BY is_starred DESC, years_experience DESC", (candidate_id,))
    skills = cursor.fetchall()
    
    cursor.execute("SELECT * FROM certificates WHERE candidate_id = %s ORDER BY date_obtained DESC", (candidate_id,))
    certificates = cursor.fetchall()
    
    conn.close()
    
    return render_template('candidate_details.html', 
                          candidate=candidate, 
                          work_experience=work_experience, 
                          skills=skills,
                          certificates=certificates)

@app.route('/candidate/delete/<string:candidate_id>', methods=['POST'])
def delete_candidate(candidate_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Delete certificates first
    cursor.execute("DELETE FROM certificates WHERE candidate_id = %s", (candidate_id,))
    
    # Delete skills
    cursor.execute("DELETE FROM skills WHERE candidate_id = %s", (candidate_id,))
    
    # Delete work experience
    cursor.execute("DELETE FROM work_experience WHERE candidate_id = %s", (candidate_id,))
    
    # Finally delete candidate
    cursor.execute("DELETE FROM candidates WHERE id = %s", (candidate_id,))
    
    conn.commit()
    conn.close()
    
    flash('Candidate deleted successfully')
    return redirect(url_for('view_candidates'))

@app.route('/candidate/edit/<candidate_id>', methods=['GET', 'POST'])
def edit_candidate(candidate_id):
    if request.method == 'POST':
        # Get form data
        name = request.form['name']
        email = request.form['email']
        phone = request.form['phone']
        wensen = request.form.get('wensen', '')
        ambities = request.form.get('ambities', '')
        professional_role = request.form.get('professional_role', '')
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Update the query to include wensen, ambities, and professional_role
        cursor.execute("""
            UPDATE candidates 
            SET name = %s, email = %s, phone = %s, wensen = %s, ambities = %s, 
                professional_role = %s, updated_at = GETDATE() 
            WHERE id = %s
        """, (name, email, phone, wensen, ambities, professional_role, candidate_id))
        
        # Process work experience updates
        work_exp_count = int(request.form.get('work_exp_count', 0))
        
        # Get existing work experience IDs to track what needs to be deleted
        cursor.execute("SELECT id FROM work_experience WHERE candidate_id = %s", (candidate_id,))
        existing_exp_ids = [row[0] for row in cursor.fetchall()]
        processed_exp_ids = []
        
        # Process each work experience entry
        for i in range(work_exp_count):
            company = request.form.get(f'company_{i}', '')
            position = request.form.get(f'position_{i}', '')
            start_date = request.form.get(f'start_date_{i}', '')
            end_date = request.form.get(f'end_date_{i}', '')
            description = request.form.get(f'description_{i}', '')
            
            # Get work experience ID if it's an existing entry - using TOP instead of LIMIT for SQL Server
            exp_id_query = "SELECT TOP 1 id FROM work_experience WHERE candidate_id = %s AND company = %s AND position = %s"
            cursor.execute(exp_id_query, (candidate_id, company, position))
            result = cursor.fetchone()
            exp_id = result[0] if result else None
            
            if exp_id:
                # Update existing entry
                processed_exp_ids.append(exp_id)
                cursor.execute("""
                    UPDATE work_experience
                    SET company = %s, position = %s, start_date = %s, end_date = %s, description = %s
                    WHERE id = %s AND candidate_id = %s
                """, (company, position, start_date, end_date, description, exp_id, candidate_id))
            else:
                # Insert new entry
                cursor.execute("""
                    INSERT INTO work_experience (candidate_id, company, position, start_date, end_date, description)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (candidate_id, company, position, start_date, end_date, description))
        
        # Delete work experience entries that weren't processed
        for exp_id in existing_exp_ids:
            if exp_id not in processed_exp_ids:
                cursor.execute("DELETE FROM work_experience WHERE id = %s", (exp_id,))
        
        # Process skills updates
        skills_count = int(request.form.get('skills_count', 0))
        
        # Get existing skill IDs to track what needs to be deleted
        cursor.execute("SELECT id FROM skills WHERE candidate_id = %s", (candidate_id,))
        existing_skill_ids = [row[0] for row in cursor.fetchall()]
        processed_skill_ids = []
        
        # Process each skill entry
        for i in range(skills_count):
            skill_name = request.form.get(f'skill_name_{i}', '')
            years_experience = request.form.get(f'years_experience_{i}', 0)
            
            # Get skill ID if it's an existing entry - using TOP instead of LIMIT for SQL Server
            skill_id_query = "SELECT TOP 1 id FROM skills WHERE candidate_id = %s AND skill_name = %s"
            cursor.execute(skill_id_query, (candidate_id, skill_name))
            result = cursor.fetchone()
            skill_id = result[0] if result else None
            
            if skill_id:
                # Update existing skill
                processed_skill_ids.append(skill_id)
                cursor.execute("""
                    UPDATE skills
                    SET skill_name = %s, years_experience = %s
                    WHERE id = %s AND candidate_id = %s
                """, (skill_name, years_experience, skill_id, candidate_id))
            else:
                # Insert new skill
                cursor.execute("""
                    INSERT INTO skills (candidate_id, skill_name, years_experience, is_starred)
                    VALUES (%s, %s, %s, %s)
                """, (candidate_id, skill_name, years_experience, False))
        
        # Delete skill entries that weren't processed
        for skill_id in existing_skill_ids:
            if skill_id not in processed_skill_ids:
                cursor.execute("DELETE FROM skills WHERE id = %s", (skill_id,))
        
        # Process certificates
        certificates_count = int(request.form.get('certificates_count', 0))
        
        # Get existing certificate IDs
        cursor.execute("SELECT id FROM certificates WHERE candidate_id = %s", (candidate_id,))
        existing_cert_ids = [row[0] for row in cursor.fetchall()]
        processed_cert_ids = []
        
        # Process each certificate
        for i in range(certificates_count):
            cert_name = request.form.get(f'cert_name_{i}', '')
            cert_issuer = request.form.get(f'cert_issuer_{i}', '')
            cert_date_obtained = request.form.get(f'cert_date_obtained_{i}', '')
            cert_expiry_date = request.form.get(f'cert_expiry_date_{i}', '')
            cert_description = request.form.get(f'cert_description_{i}', '')
            
            # Skip empty certificates
            if not cert_name.strip():
                continue
                
            # Get certificate ID if it's an existing entry - using TOP instead of LIMIT for SQL Server
            cert_id_query = "SELECT TOP 1 id FROM certificates WHERE candidate_id = %s AND name = %s"
            cursor.execute(cert_id_query, (candidate_id, cert_name))
            result = cursor.fetchone()
            cert_id = result[0] if result else None
            
            if cert_id:
                # Update existing certificate
                processed_cert_ids.append(cert_id)
                cursor.execute("""
                    UPDATE certificates
                    SET name = %s, issuer = %s, date_obtained = %s, expiry_date = %s, description = %s
                    WHERE id = %s AND candidate_id = %s
                """, (cert_name, cert_issuer, cert_date_obtained, cert_expiry_date, cert_description, cert_id, candidate_id))
            else:
                # Insert new certificate
                cursor.execute("""
                    INSERT INTO certificates (candidate_id, name, issuer, date_obtained, expiry_date, description)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (candidate_id, cert_name, cert_issuer, cert_date_obtained, cert_expiry_date, cert_description))
        
        # Delete certificate entries that weren't processed
        for cert_id in existing_cert_ids:
            if cert_id not in processed_cert_ids:
                cursor.execute("DELETE FROM certificates WHERE id = %s", (cert_id,))
        
        conn.commit()
        conn.close()
        
        flash('Candidate updated successfully!', 'success')
        return redirect(url_for('candidate_details', candidate_id=candidate_id))
    
    # For GET requests
    conn = get_db_connection()
    cursor = conn.cursor(as_dict=True)
    
    # Fetch the candidate data including professional_role
    cursor.execute("""
        SELECT id, name, email, phone, cv_text, wensen, ambities, professional_role, updated_at
        FROM candidates 
        WHERE id = %s
    """, (candidate_id,))
    
    candidate = cursor.fetchone()

    if not candidate:
        flash('Candidate not found', 'error')
        return redirect(url_for('view_candidates'))
    
    # Fetch skills - make sure to keep this part
    cursor.execute("""
        SELECT id, skill_name, years_experience, is_starred
        FROM skills
        WHERE candidate_id = %s
        ORDER BY is_starred DESC, years_experience DESC
    """, (candidate_id,))
    
    skills = cursor.fetchall()
    
    # Fetch work experience - make sure to keep this part
    cursor.execute("""
        SELECT id, company, position, start_date, end_date, description
        FROM work_experience
        WHERE candidate_id = %s
        ORDER BY start_date DESC
    """, (candidate_id,))
    
    work_experience = cursor.fetchall()
    
    # Fetch certificates - make sure to keep this part
    cursor.execute("""
        SELECT id, name, issuer, date_obtained, expiry_date, description
        FROM certificates
        WHERE candidate_id = %s
        ORDER BY date_obtained DESC
    """, (candidate_id,))
    
    certificates = cursor.fetchall()
    
    conn.close()
    
    # Make sure the edit_candidate.html template can handle the professional_role
    # You'll need to add an input field for it in the template.
    return render_template('edit_candidate.html', 
                          candidate=candidate, 
                          skills=skills, 
                          work_experience=work_experience, 
                          certificates=certificates)

@app.route('/vacancy-match', methods=['GET', 'POST'])
def vacancy_match():
    # Initialize empty skills structure
    vacancy_skills = {
        'required_skills': [],
        'nice_to_have_skills': []
    }
    
    if request.method == 'POST':
        # Check if it's an AJAX request
        if request.is_json:
            data = request.get_json()
            vacancy_text = data.get('vacancy_text', '')
            vacancy_skills = {
                'required_skills': data.get('required_skills', []),
                'nice_to_have_skills': data.get('nice_to_have_skills', [])
            }
            
            # Get all candidates and their skills from the database
            conn = get_db_connection()
            cursor = conn.cursor(as_dict=True)  # Use dictionary cursor
            
            cursor.execute("""
                SELECT 
                    c.id, 
                    c.name, 
                    c.email,
                    (SELECT STRING_AGG(CONCAT(s2.skill_name, ':', CAST(s2.years_experience AS VARCHAR)), ',') 
                     FROM skills s2 
                     WHERE s2.candidate_id = c.id) as skills
                FROM candidates c
            """)
            candidates = cursor.fetchall()
            
            # Calculate match percentage for each candidate
            matches = []
            for candidate in candidates:
                candidate_skills = {}
                if candidate['skills']:
                    for skill_entry in candidate['skills'].split(','):
                        if ':' in skill_entry:
                            skill_name, years = skill_entry.split(':', 1)
                            try:
                                candidate_skills[skill_name] = float(years)
                            except ValueError:
                                candidate_skills[skill_name] = 0
                
                required_match_score = 0
                required_total = len(vacancy_skills['required_skills'])
                nice_to_have_match_score = 0
                nice_to_have_total = len(vacancy_skills['nice_to_have_skills'])
                
                details = []
                
                # Check required skills
                for req_skill in vacancy_skills['required_skills']:
                    skill_name = req_skill['skill_name']
                    req_years = req_skill['years_experience']
                    
                    best_match = None
                    best_score = 0
                    best_years = 0
                    
                    for cand_skill, cand_years in candidate_skills.items():
                        similarity = skill_similarity(skill_name, cand_skill)
                        if similarity > 0.5 and similarity > best_score:  # Threshold of 0.5 for similarity
                            best_match = cand_skill
                            best_score = similarity
                            best_years = cand_years
                    
                    if best_match:
                        if best_years >= req_years:
                            required_match_score += 1
                            details.append({
                                'skill_name': skill_name,
                                'type': 'Required',
                                'match': True,
                                'reason': f"Kandidaat heeft {best_years} ervaring met {best_match}, gevraagd: {req_years} jaar."
                            })
                        else:
                            required_match_score += 0.5
                            details.append({
                                'skill_name': skill_name,
                                'type': 'Required',
                                'match': False,
                                'reason': f"Kandidaat heeft  {best_years} ervaring met {best_match}, gevraagd: {req_years} jaar."
                            })
                    else:
                        details.append({
                            'skill_name': skill_name,
                            'type': 'Required',
                            'match': False,
                            'reason': "Vaardigheid niet gevonden in kandidaat's profiel."
                        })
                
                # Check nice-to-have skills
                for nice_skill in vacancy_skills['nice_to_have_skills']:
                    skill_name = nice_skill['skill_name']
                    req_years = nice_skill['years_experience']
                    
                    match_found = False
                    for cand_skill, cand_years in candidate_skills.items():
                        # Simple matching for now, can be enhanced with fuzzy matching
                        if skill_name.lower() in cand_skill.lower() or cand_skill.lower() in skill_name.lower():
                            match_found = True
                            if cand_years >= req_years:
                                nice_to_have_match_score += 1
                                details.append({
                                    'skill_name': skill_name,
                                    'type': 'Nice to Have',
                                    'match': True,
                                    'reason': f"Kandidaat heeft {cand_years} jaar ervaring, gevraagd: {req_years} jaar."
                                })
                            else:
                                nice_to_have_match_score += 0.5
                                details.append({
                                    'skill_name': skill_name,
                                    'type': 'Nice to Have',
                                    'match': False,
                                    'reason': f"Kandidaat heeft {cand_years} jaar ervaring, preferred {req_years} jaar."
                                })
                            break
                    
                    if not match_found:
                        details.append({
                            'skill_name': skill_name,
                            'type': 'Nice to Have',
                            'match': False,
                            'reason': "Vaardigheid niet gevonden in kandidaat's profiel."
                        })
                
                # Calculate percentages
                required_percentage = (required_match_score / required_total * 100) if required_total > 0 else 0
                nice_to_have_percentage = (nice_to_have_match_score / nice_to_have_total * 100) if nice_to_have_total > 0 else 0
                
                # Calculate total match percentage with weighted formula
                required_weight = 0.7  # 70% weight for required skills
                nice_to_have_weight = 0.3  # 30% weight for nice-to-have skills
                
                # If there are no nice-to-have skills, give 100% weight to required skills
                if nice_to_have_total == 0:
                    match_percentage = required_percentage
                # If there are no required skills, give 100% weight to nice-to-have skills
                elif required_total == 0:
                    match_percentage = nice_to_have_percentage
                # Otherwise, use the weighted formula
                else:
                    match_percentage = (required_percentage * required_weight) + (nice_to_have_percentage * nice_to_have_weight)
                
                # Round to nearest integer
                match_percentage = round(match_percentage)
                required_percentage = round(required_percentage)
                nice_to_have_percentage = round(nice_to_have_percentage)
                
                # Add the match to the results
                matches.append({
                    'candidate_id': candidate['id'],
                    'name': candidate['name'],
                    'email': candidate['email'],
                    'match_percentage': match_percentage,
                    'required_match': required_percentage,
                    'nice_to_have_match': nice_to_have_percentage,
                    'details': details
                })
            
            conn.close()
            
            # Sort matches by percentage
            matches.sort(key=lambda x: x['match_percentage'], reverse=True)
            
            # Return JSON data instead of HTML
            return jsonify({
                'matches': matches
            })
        
        # Handle regular form submission
        vacancy_text = request.form.get('vacancy_text')
        
        # Extract skills from vacancy using Azure OpenAI
        prompt = f"""
        Extract required skills and experience from this job vacancy.
        Format the output as JSON with the following structure:
        {{
            "required_skills": [
                {{
                    "skill_name": "skill name",
                    "years_experience": minimum years required (number, 0 if not specified)
                }}
            ],
            "nice_to_have_skills": [
                {{
                    "skill_name": "skill name",
                    "years_experience": minimum years preferred (number, 0 if not specified)
                }}
            ]
        }}
        
        Vacancy Text:
        {vacancy_text}
        """
        
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a helpful assistant that extracts structured information from job vacancies. Try to extract the most important skills and experience from the vacancy and translate to Dutch."},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"}
        )
        
        vacancy_skills = json.loads(response.choices[0].message.content)
        
        # Get all candidates and their skills from the database
        conn = get_db_connection()
        cursor = conn.cursor(as_dict=True)
        
        cursor.execute("""
            SELECT 
                c.id, 
                c.name, 
                c.email,
                (SELECT STRING_AGG(CONCAT(s2.skill_name, ':', CAST(s2.years_experience AS VARCHAR)), ',') 
                 FROM skills s2 
                 WHERE s2.candidate_id = c.id) as skills
            FROM candidates c
        """)
        candidates = cursor.fetchall()
        
        # Calculate match percentage for each candidate
        matches = []
        for candidate in candidates:
            candidate_skills = {}
            if candidate['skills']:
                for skill_entry in candidate['skills'].split(','):
                    if ':' in skill_entry:
                        skill_name, years = skill_entry.split(':', 1)
                        try:
                            candidate_skills[skill_name] = float(years)
                        except ValueError:
                            candidate_skills[skill_name] = 0
            
            required_match_score = 0
            required_total = len(vacancy_skills['required_skills'])
            nice_to_have_match_score = 0
            nice_to_have_total = len(vacancy_skills['nice_to_have_skills'])
            
            details = []
            
            # Check required skills
            for req_skill in vacancy_skills['required_skills']:
                skill_name = req_skill['skill_name']
                req_years = req_skill['years_experience']
                
                best_match = None
                best_score = 0
                best_years = 0
                
                for cand_skill, cand_years in candidate_skills.items():
                    similarity = skill_similarity(skill_name, cand_skill)
                    if similarity > 0.5 and similarity > best_score:  # Threshold of 0.5 for similarity
                        best_match = cand_skill
                        best_score = similarity
                        best_years = cand_years
                
                if best_match:
                    if best_years >= req_years:
                        required_match_score += 1
                        details.append({
                            'skill_name': skill_name,
                            'type': 'Required',
                            'match': True,
                            'reason': f"Kandidaat heeft {best_years} jaar ervaring met {best_match}, gevraagd: {req_years} jaar."
                        })
                    else:
                        required_match_score += 0.5
                        details.append({
                            'skill_name': skill_name,
                            'type': 'Required',
                            'match': False,
                            'reason': f"Kandidaat heeft {best_years} jaar ervaring met {best_match}, gevraagd: {req_years} jaar."
                        })
                else:
                    details.append({
                        'skill_name': skill_name,
                        'type': 'Required',
                        'match': False,
                        'reason': "Vaardigheid niet gevonden in kandidaat's profiel."
                    })
            
            # Check nice-to-have skills
            for nice_skill in vacancy_skills['nice_to_have_skills']:
                skill_name = nice_skill['skill_name']
                req_years = nice_skill['years_experience']
                
                match_found = False
                for cand_skill, cand_years in candidate_skills.items():
                    # Simple matching for now, can be enhanced with fuzzy matching
                    if skill_name.lower() in cand_skill.lower() or cand_skill.lower() in skill_name.lower():
                        match_found = True
                        if cand_years >= req_years:
                            nice_to_have_match_score += 1
                            details.append({
                                'skill_name': skill_name,
                                'type': 'Nice to Have',
                                'match': True,
                                'reason': f"Kandidaat heeft {cand_years} jaar ervaring, gevraagd: {req_years} jaar."
                            })
                        else:
                            nice_to_have_match_score += 0.5
                            details.append({
                                'skill_name': skill_name,
                                'type': 'Nice to Have',
                                'match': False,
                                'reason': f"Kandidaat heeft {cand_years} jaar ervaring, preferred {req_years} jaar."
                            })
                        break
                
                if not match_found:
                    details.append({
                        'skill_name': skill_name,
                        'type': 'Nice to Have',
                        'match': False,
                        'reason': "Skill not found in candidate's profile."
                    })
            
            # Calculate percentages
            required_percentage = (required_match_score / required_total * 100) if required_total > 0 else 0
            nice_to_have_percentage = (nice_to_have_match_score / nice_to_have_total * 100) if nice_to_have_total > 0 else 0
            
            # Calculate total match percentage with weighted formula
            required_weight = 0.7  # 70% weight for required skills
            nice_to_have_weight = 0.3  # 30% weight for nice-to-have skills
            
            # If there are no nice-to-have skills, give 100% weight to required skills
            if nice_to_have_total == 0:
                match_percentage = required_percentage
            # If there are no required skills, give 100% weight to nice-to-have skills
            elif required_total == 0:
                match_percentage = nice_to_have_percentage
            # Otherwise, use the weighted formula
            else:
                match_percentage = (required_percentage * required_weight) + (nice_to_have_percentage * nice_to_have_weight)
            
            # Round to nearest integer
            match_percentage = round(match_percentage)
            required_percentage = round(required_percentage)
            nice_to_have_percentage = round(nice_to_have_percentage)
            
            # Add the match to the results
            matches.append({
                'candidate_id': candidate['id'],
                'name': candidate['name'],
                'email': candidate['email'],
                'match_percentage': match_percentage,
                'required_match': required_percentage,
                'nice_to_have_match': nice_to_have_percentage,
                'details': details
            })
        
        conn.close()
        
        # Sort matches by percentage
        matches.sort(key=lambda x: x['match_percentage'], reverse=True)
        
        # Return full page for form submissions
        return render_template(
            'vacancy_match.html',
            vacancy_text=vacancy_text,
            vacancy_skills=vacancy_skills,
            matches=matches,
            show_results=True
        )
    
    return render_template('vacancy_match.html', show_results=False)

def skill_similarity(skill1, skill2):
    """Calculate similarity between two skill names"""
    # Normalize strings: lowercase and remove all spaces and dashes
    def normalize_string(s):
        return ''.join(c for c in s.lower() if c.isalnum())
    
    s1 = normalize_string(skill1)
    s2 = normalize_string(skill2)
    
    # Direct match after normalization
    if s1 == s2:
        return 1.0
    
    # Check for substring match after normalization
    if s1 in s2 or s2 in s1:
        return 0.9
    
    # For skills that might have been split by spaces, hyphens, or en-dashes
    words1 = set(skill1.lower().replace('–', '-').replace('-', ' ').split())
    words2 = set(skill2.lower().replace('–', '-').replace('-', ' ').split())
    common_words = words1.intersection(words2)
    
    if common_words:
        return len(common_words) / max(len(words1), len(words2))
    
    return 0.0

@app.route('/skill-search', methods=['GET', 'POST'])
def skill_search():
    skill = None
    candidates = []
    
    if request.method == 'POST':
        skill = request.form.get('skill', '').strip()
    elif request.method == 'GET':
        skill = request.args.get('skill', '').strip()
    
    if skill:
        # Connect to the database
        conn = get_db_connection()
        cursor = conn.cursor(as_dict=True)  # Use dictionary cursor
        
        # Get candidates with the specified skill
        cursor.execute("""
            SELECT 
                c.id, 
                c.name, 
                c.email, 
                c.phone,
                s.years_experience
            FROM candidates c
            JOIN skills s ON c.id = s.candidate_id
            WHERE LOWER(s.skill_name) = LOWER(%s)
            ORDER BY s.years_experience DESC
        """, (skill,))
        
        skill_candidates = cursor.fetchall()
        
        # Save the search to search_history
        cursor.execute("""
            INSERT INTO search_history (search_term, results_count)
            VALUES (%s, %s)
        """, (skill, len(skill_candidates)))
        conn.commit()
        
        # For each candidate, get their other skills
        for candidate in skill_candidates:
            cursor.execute("""
                SELECT skill_name, years_experience
                FROM skills
                WHERE candidate_id = %s AND LOWER(skill_name) != LOWER(%s)
                ORDER BY years_experience DESC
            """, (candidate['id'], skill))
            
            other_skills = cursor.fetchall()
            candidate['other_skills'] = other_skills
            candidates.append(candidate)
        
        conn.close()
    
    return render_template('skill_search.html', skill=skill, candidates=candidates)

def get_candidate_by_id(candidate_id):
    conn = get_db_connection()
    cursor = conn.cursor(as_dict=True)  # Use dictionary cursor
    
    # Get candidate info
    cursor.execute('''
        SELECT c.*, 
            (SELECT STRING_AGG(s2.skill_name, ',') 
             FROM skills s2 
             WHERE s2.candidate_id = c.id) as skills_list
        FROM candidates c
        WHERE c.id = %s
    ''', (candidate_id,))
    
    candidate = cursor.fetchone()
    
    if candidate:
        # Get skills separately to maintain the full skill information
        cursor.execute('''
            SELECT skill_name, years_experience
            FROM skills 
            WHERE candidate_id = %s
        ''', (candidate_id,))
        skills = cursor.fetchall()
        
        # No need to convert candidate to dict
        candidate['skills'] = [
            {
                'name': skill['skill_name'],
                'years': skill['years_experience']
            } for skill in skills
        ]
        
        # Get work experience
        cursor.execute('''
            SELECT company, position, start_date, end_date, description
            FROM work_experience
            WHERE candidate_id = %s
            ORDER BY start_date DESC
        ''', (candidate_id,))
        work_experience = cursor.fetchall()
        candidate['work_experience'] = work_experience  # Already dictionaries
        
        # Get certificates
        cursor.execute('''
            SELECT name, issuer, date_obtained, expiry_date, description
            FROM certificates
            WHERE candidate_id = %s
            ORDER BY date_obtained DESC
        ''', (candidate_id,))
        certificates = cursor.fetchall()
        candidate['certificates'] = certificates  # Already dictionaries
        
        conn.close()
        return candidate
    
    conn.close()
    return None

@app.route('/write-cover-letter/<string:candidate_id>')
def write_cover_letter(candidate_id):
    candidate = get_candidate_by_id(candidate_id)
    if not candidate:
        flash('Kandidaat niet gevonden', 'error')
        return redirect(url_for('vacancy_match'))
        
    return render_template(
        'cover_letter_writer.html',
        candidate=candidate,
        vacancy_text=request.args.get('vacancy_text', '')
    )

@app.route('/generate-cover-letter', methods=['POST'])
def generate_cover_letter():
    data = request.get_json()
    vacancy_text = data['vacancy_text']
    candidate_id = data['candidate_id']
    language = data['language']
    
    # Get candidate information
    candidate = get_candidate_by_id(candidate_id)
    
    # Create prompt for Azure OpenAI
    prompt = f"""
    Please write a professional cover letter in {'Dutch' if language == 'nl' else 'English'} for the following job:
    
    Vacancy:
    {vacancy_text}
    
    Candidate Information:
    Name: {candidate['name']}
    Skills: {', '.join(skill['name'] for skill in candidate['skills'])}
    Work Experience: {', '.join(exp['position'] + ' at ' + exp['company'] for exp in candidate['work_experience'])}
    
    Please write a personalized cover letter that:
    1. Matches the candidate's experience with the job requirements
    2. Highlights relevant skills and experience
    3. Uses a professional but engaging tone
    4. Is structured with proper paragraphs
    5. Is in {'Dutch' if language == 'nl' else 'English'}
    """
    
    # Use your existing Azure OpenAI client
    response = client.chat.completions.create(
        model="gpt-4",  # or whatever model you're using
        messages=[
            {"role": "system", "content": "You are a professional cover letter writer."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.7,
        max_tokens=1000
    )
    
    cover_letter = response.choices[0].message.content
    
    return jsonify({'cover_letter': cover_letter})

@app.route('/generate-candidate-cv/<string:candidate_id>')
def generate_candidate_cv(candidate_id):
    try:
        # Call the generate_cv function from cv_generator.py
        from cv_generator import generate_cv
        
        # Generate the CV
        html_content = generate_cv(candidate_id)
        
        # Create a unique filename
        filename = f"cv_{candidate_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}.html"
        
        # Create a response with the HTML content
        response = make_response(html_content)
        response.headers["Content-Disposition"] = f"attachment; filename={filename}"
        response.headers["Content-Type"] = "text/html"
        
        return response
    except Exception as e:
        flash(f'Error generating CV: {str(e)}', 'error')
        return redirect(url_for('view_candidates', candidate_id=candidate_id))

@app.route('/view_cv/<candidate_id>')
def view_cv(candidate_id):
    try:
        # Generate the CV HTML
        html_content = generate_cv(candidate_id)
        
        # Return the HTML content
        return html_content
    except Exception as e:
        import traceback
        print(f"Error generating CV: {str(e)}")
        print(traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@app.route('/outdated-candidates')
def outdated_candidates():
    conn = get_db_connection()
    cursor = conn.cursor(as_dict=True)
    
    # Get candidates not updated in last 6 months
    cursor.execute("""
        SELECT 
            c.id, c.name, c.email, c.phone, c.updated_at,
            DATEDIFF(month, c.updated_at, GETDATE()) as months_since_update
        FROM candidates c
        WHERE DATEDIFF(month, c.updated_at, GETDATE()) >= 6
        ORDER BY c.updated_at ASC
    """)
    
    candidates = cursor.fetchall()
    
    # For each candidate, fetch their top skills
    for candidate in candidates:
        cursor.execute("""
            SELECT TOP 5 skill_name, years_experience 
            FROM skills 
            WHERE candidate_id = %s 
            ORDER BY years_experience DESC
        """, (candidate['id'],))
        candidate['skills'] = cursor.fetchall()
    
    conn.close()
    
    return render_template('outdated_candidates.html', 
                         candidates=candidates)

def cleanup_certificates():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Delete generic certificates
    cursor.execute("""
        DELETE FROM certificates 
        WHERE LOWER(name) IN ('zelfstandig werken', 'communicatie', 'teamwork', 'leadership')
        OR LEN(name) <= 3
    """)
    
    conn.commit()
    conn.close()

# Run this once to clean up the database
cleanup_certificates()

@app.route('/add-candidate-skill', methods=['POST'])
def add_candidate_skill():
    try:
        data = request.json
        candidate_id = data['candidate_id']
        # Escape single quotes in skill_name to prevent SQL injection
        skill_name = data['skill_name'].replace("'", "''")
        years_experience = float(data['years_experience'])
        is_starred = data.get('is_starred', 0)  # Default to 0 if not provided
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Using f-string with proper escaping
        query = f"""
            INSERT INTO [cv-analysis-db].dbo.skills 
            (candidate_id, skill_name, years_experience, is_starred) 
            VALUES 
            ('{candidate_id}', N'{skill_name}', {years_experience}, {is_starred})
        """
        
        cursor.execute(query)
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({'success': True})
        
    except Exception as e:
        print(f"Error adding skill: {e}")
        print(f"Input data: candidate_id={data.get('candidate_id')}, "
              f"skill_name={data.get('skill_name')}, "
              f"years_experience={data.get('years_experience')}, "
              f"is_starred={data.get('is_starred', 0)}")
        # Print the actual query for debugging
        print(f"Executed query: {query}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/search-stats')
def search_stats():
    period = request.args.get('period', 'month')
    conn = get_db_connection()
    cursor = conn.cursor(as_dict=True)
    
    # Calculate date range based on period
    now = datetime.now()
    if period == 'week':
        start_date = now - timedelta(days=7)
    elif period == 'month':
        start_date = now - timedelta(days=30)
    else:  # 'all'
        start_date = datetime.min
    
    # Get total searches for the period
    cursor.execute("""
        SELECT COUNT(*) as total
        FROM search_history
        WHERE search_timestamp >= %s
    """, (start_date,))
    total_searches = cursor.fetchone()['total']
    
    # Get unique skills count
    cursor.execute("""
        SELECT COUNT(DISTINCT search_term) as unique_count
        FROM search_history
        WHERE search_timestamp >= %s
    """, (start_date,))
    unique_skills = cursor.fetchone()['unique_count']
    
    # Get average results
    cursor.execute("""
        SELECT AVG(CAST(results_count AS FLOAT)) as avg_results
        FROM search_history
        WHERE search_timestamp >= %s
    """, (start_date,))
    avg_results = cursor.fetchone()['avg_results'] or 0
    
    # Get top searches with trends
    cursor.execute("""
        WITH TopSearches AS (
            SELECT 
                search_term,
                COUNT(*) as search_count,
                MAX(search_timestamp) as last_searched,
                AVG(CAST(results_count AS FLOAT)) as avg_results
            FROM search_history
            WHERE search_timestamp >= %s
            GROUP BY search_term
            ORDER BY search_count DESC, last_searched DESC
            OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY
        )
        SELECT *,
            (
                SELECT AVG(CAST(results_count AS FLOAT))
                FROM search_history h2
                WHERE h2.search_term = TopSearches.search_term
                AND h2.search_timestamp >= DATEADD(day, -7, GETDATE())
            ) as recent_avg
        FROM TopSearches
    """, (start_date,))
    
    top_searches = cursor.fetchall()
    
    # Calculate trends (% change in average results over the last week)
    for search in top_searches:
        if search['recent_avg'] and search['avg_results']:
            search['trend'] = round(((search['recent_avg'] - search['avg_results']) / search['avg_results']) * 100)
        else:
            search['trend'] = 0
    
    conn.close()
    
    return render_template('search_stats.html',
                         total_searches=total_searches,
                         unique_skills=unique_skills,
                         avg_results=avg_results,
                         top_searches=top_searches,
                         period=period)

@app.route('/toggle-skill-star', methods=['POST'])
def toggle_skill_star():
    try:
        data = request.json
        skill_id = data['skill_id']
        is_starred = data['is_starred']
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = f"""
            UPDATE [cv-analysis-db].dbo.skills 
            SET is_starred = {1 if is_starred else 0}
            WHERE id = {skill_id}
        """
        
        cursor.execute(query)
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({'success': True})
        
    except Exception as e:
        print(f"Error toggling skill star: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/add-candidate', methods=['POST'])
def add_candidate():
    # ... existing code ...
    
    # Add wensen and ambities to the INSERT statement
    cursor.execute("""
        INSERT INTO candidates (id, name, email, phone, cv_text, wensen, ambities)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (candidate_id, name, email, phone, cv_text, wensen, ambities))
    
    # ... existing code ...

@app.route('/request-assignment-change/<string:candidate_id>', methods=['POST'])
def request_assignment_change(candidate_id):
    candidate = get_candidate_by_id(candidate_id)
    if not candidate:
        flash('Kandidaat niet gevonden', 'error')
        return redirect(url_for('view_candidates'))
        
    change_reason = request.form.get('change_reason')
    if not change_reason:
        flash('Voer een reden in voor de opdrachtwisseling', 'error')
        return redirect(url_for('candidate_details', candidate_id=candidate_id))
    
    # Update candidate record to mark assignment change request
    update_candidate_assignment_request(candidate_id, change_reason)
    
    # Send email notification
    subject = f'Opdrachtwisseling verzoek: {candidate["name"]}'
    body = f"""
    Hallo,
    
    {candidate["name"]} heeft een verzoek ingediend om van opdracht te wisselen.
    
    Reden: {change_reason}
    
    Contact informatie:
    Email: {candidate["email"]}
    Telefoon: {candidate["phone"]}
    
    Je kunt het profiel bekijken via: {url_for('candidate_details', candidate_id=candidate_id, _external=True)}
    """
    
    if send_email(subject, 'ralph.lemaire@wortell.nl', body):
        flash('Je verzoek is ingediend en er is een email verzonden.', 'success')
    else:
        flash('Je verzoek is ingediend, maar er kon geen email worden verzonden.', 'warning')
    
    return redirect(url_for('candidate_details', candidate_id=candidate_id))

def update_candidate_assignment_request(candidate_id, change_reason):
    """
    Update candidate record to mark assignment change request.
    This will store the change reason and timestamp in the database.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # First, check if we need to alter the table to add the columns if they don't exist
        # This is a safe operation to perform in case the columns don't exist yet
        try:
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sys.columns 
                    WHERE object_id = OBJECT_ID('candidates') AND name = 'change_request_reason')
                BEGIN
                    ALTER TABLE candidates ADD change_request_reason NVARCHAR(MAX), 
                    change_request_date DATETIME
                END
            """)
            conn.commit()
        except Exception as e:
            print(f"Error checking/adding columns: {str(e)}")
            # Continue anyway, the columns might already exist
            
        # Now update the candidate record
        cursor.execute("""
            UPDATE candidates 
            SET change_request_reason = %s, 
                change_request_date = GETDATE(),
                updated_at = GETDATE()
            WHERE id = %s
        """, (change_reason, candidate_id))
        
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"Error updating candidate assignment request: {str(e)}")
        return False

@app.route('/test-email')
def test_email():
    try:
        msg = Message('Test Email from Flask App', 
                     recipients=['ralph.lemaire@wortell.nl'])
        msg.body = 'This is a test email from your Flask application.'
        mail.send(msg)
        return 'Email sent successfully!'
    except Exception as e:
        return f'Error sending email: {str(e)}'

def send_email(subject, recipient, body):
    import smtplib
    from email.mime.text import MIMEText
    
    sender = os.getenv('EMAIL_SENDER')
    password = os.getenv('EMAIL_PASSWORD')
    
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = "Wortell-Kandidaat-Portal <ralph@rlemaire.com>"
    msg['To'] = recipient
    
    try:
        # When using SMTP_SSL, you don't need to call starttls()
        server = smtplib.SMTP_SSL('smtp.strato.de', 465)
        server.login(sender, password)
        server.send_message(msg)
        server.quit()
        return True
    except Exception as e:
        print(f"Error sending email: {str(e)}")
        return False

if __name__ == '__main__':
    app.run(debug=True)
