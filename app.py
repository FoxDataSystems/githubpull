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
from datetime import datetime
from jinja2 import Template
import pdfkit
import pymssql
# Import the generate_cv function from cv_generator.py
from cv_generator import generate_cv

# Create a function to establish database connection
def get_db_connection():
    try:
        conn = pymssql.connect(
            server='cv-analysis-server.database.windows.net',
            user='cv-analysis-admin',
            password='',
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
client = AzureOpenAI(
      
    api_version="2024-05-01-preview",
    azure_endpoint="https://rl-test-ai.openai.azure.com/"
)

# Initialize Flask app
app = Flask(__name__)
app.secret_key = "your_secret_key"
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max upload size
ALLOWED_EXTENSIONS = {'pdf', 'docx', 'doc', 'txt'}

# Create uploads folder if it doesn't exist
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# Database setup
def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Check if candidates table exists first
    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'candidates')
    BEGIN
        CREATE TABLE candidates (
            id VARCHAR(255) PRIMARY KEY,
            name NVARCHAR(255),
            email NVARCHAR(255),
            phone NVARCHAR(50),
            cv_text NVARCHAR(MAX)
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
    
    conn.commit()
    conn.close()

# Initialize database
init_db()

def normalize_existing_data():
    """Normalize existing certificate and skill names in the database"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Update skills
    cursor.execute("""
        UPDATE skills
        SET skill_name = REPLACE(skill_name, N'–', '-')
        WHERE skill_name LIKE N'%–%'
    """)
    
    # Update certificates
    cursor.execute("""
        UPDATE certificates
        SET name = REPLACE(name, N'–', '-')
        WHERE name LIKE N'%–%'
    """)
    
    conn.commit()
    conn.close()

# Add this line after init_db() call to normalize existing data
init_db()
normalize_existing_data()

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
    return f"""Please analyze this CV text and extract the following information in JSON format. For each certificate, analyze what technologies or skills it represents and add those to the skills section.

    {{
        "name": "candidate's full name",
        "email": "email address",
        "phone": "phone number",
        "work_experience": [
            {{
                "company": "company name",
                "position": "job title",
                "start_date": "start date",
                "end_date": "end date or 'Present'",
                "description": "job description"
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
    4. For Microsoft certifications, use the exact code (e.g., "AZ-900", "DP-600")
    5. For professional certifications, use the exact name (e.g., "PRINCE2 Foundation", "Scrum Master")

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
    
    # Insert candidate information
    cursor.execute(
        "INSERT INTO candidates (id, name, email, phone, cv_text) VALUES (%s, %s, %s, %s, %s)",
        (candidate_id, cv_data.get('name', ''), cv_data.get('email', ''), 
         cv_data.get('phone', ''), cv_data.get('cv_text', ''))
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

    conn.close()

    return render_template('dashboard.html',
                         total_candidates=total_candidates,
                         unique_skills=unique_skills,
                         most_experienced_skill=most_experienced_skill,
                         most_skilled_candidate=most_skilled_candidate,
                         top_skills=top_skills,
                         top_candidates=top_candidates,
                         recent_activities=recent_activities)

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
    conn = get_db_connection()
    cursor = conn.cursor(as_dict=True)
    
    # Unified search query
    search_query = request.args.get('q', '').strip()
    
    if search_query:
        # Split search terms and remove empty strings
        search_terms = [term.strip() for term in search_query.split() if term.strip()]
        
        if search_terms:
            # Create a subquery for each search term (for both skills and certificates)
            conditions = []
            params = []
            
            for term in search_terms:
                # Add condition using normalized comparison (removing spaces and hyphens)
                conditions.append("""
                    EXISTS (
                        SELECT 1 
                        FROM skills s 
                        WHERE s.candidate_id = c.id 
                        AND REPLACE(REPLACE(LOWER(s.skill_name), ' ', ''), '-', '') LIKE REPLACE(REPLACE(LOWER(%s), ' ', ''), '-', '')
                    )
                    OR
                    EXISTS (
                        SELECT 1 
                        FROM certificates cert 
                        WHERE cert.candidate_id = c.id 
                        AND REPLACE(REPLACE(LOWER(cert.name), ' ', ''), '-', '') LIKE REPLACE(REPLACE(LOWER(%s), ' ', ''), '-', '')
                    )
                """)
                params.extend([f'%{term}%', f'%{term}%'])  # Add parameters for both skills and certificates
            
            # Combine all conditions with AND
            combined_query = " AND ".join(f"({condition})" for condition in conditions)
            
            # Search by name, email, or all specified skills/certificates
            cursor.execute(f"""
                SELECT DISTINCT c.id, c.name, c.email, c.phone
                FROM candidates c
                WHERE 
                    LOWER(c.name) LIKE LOWER(%s) OR 
                    LOWER(c.email) LIKE LOWER(%s) OR
                    ({combined_query})
                ORDER BY c.name
            """, ['%' + search_query + '%', '%' + search_query + '%'] + params)
    else:
        # Show all candidates if no search
        cursor.execute("SELECT id, name, email, phone FROM candidates ORDER BY name")
    
    candidates = cursor.fetchall()
    
    # For each candidate, fetch their top skills and certificates
    for candidate in candidates:
        # Fetch skills
        cursor.execute("""
            SELECT TOP 5 skill_name, years_experience 
            FROM skills 
            WHERE candidate_id = %s 
            ORDER BY years_experience DESC
        """, (candidate['id'],))
        candidate['skills'] = cursor.fetchall()
        
        # Fetch certificates
        cursor.execute("""
            SELECT TOP 3 name, issuer, date_obtained
            FROM certificates 
            WHERE candidate_id = %s 
            ORDER BY date_obtained DESC
        """, (candidate['id'],))
        candidate['certificates'] = cursor.fetchall()
    
    # Get all unique skills and certificates for the datalist
    cursor.execute("""
        SELECT DISTINCT skill_name as name, 'skill' as type
        FROM skills
        UNION
        SELECT DISTINCT name, 'certificate' as type
        FROM certificates
        ORDER BY name
    """)
    all_searchable = cursor.fetchall()
    
    conn.close()
    
    return render_template('candidates.html', 
                          candidates=candidates, 
                          search_query=search_query,
                          all_searchable=all_searchable)

@app.route('/candidate/<candidate_id>')
def view_candidate(candidate_id):
    conn = get_db_connection()
    cursor = conn.cursor(as_dict=True)  # Set cursor to return dictionaries
    
    # Get candidate info
    cursor.execute("SELECT * FROM candidates WHERE id = %s", (candidate_id,))
    candidate = cursor.fetchone()
    
    # Get work experience
    cursor.execute("SELECT * FROM work_experience WHERE candidate_id = %s ORDER BY start_date DESC", (candidate_id,))
    work_experience = cursor.fetchall()
    
    # Get skills
    cursor.execute("SELECT * FROM skills WHERE candidate_id = %s ORDER BY years_experience DESC", (candidate_id,))
    skills = cursor.fetchall()
    
    # Get certificates
    cursor.execute("SELECT * FROM certificates WHERE candidate_id = %s ORDER BY date_obtained DESC", (candidate_id,))
    certificates = cursor.fetchall()
    
    conn.close()
    
    return render_template('candidate_details.html', 
                          candidate=candidate, 
                          work_experience=work_experience, 
                          skills=skills,
                          certificates=certificates)

@app.route('/candidate/delete/<candidate_id>', methods=['POST'])
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

@app.route('/edit_candidate/<candidate_id>', methods=['GET', 'POST'])
def edit_candidate(candidate_id):
    if request.method == 'POST':
        # Update candidate information
        name = request.form.get('name')
        email = request.form.get('email')
        phone = request.form.get('phone')
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Update candidate with new updated_at timestamp
        cursor.execute(
            """UPDATE candidates 
               SET name = %s, email = %s, phone = %s, updated_at = %s 
               WHERE id = %s""",
            (name, email, phone, datetime.now(), candidate_id)
        )
        
        # Handle work experience updates
        # First, delete all existing work experience
        cursor.execute("DELETE FROM work_experience WHERE candidate_id = %s", (candidate_id,))
        
        # Then add the updated ones
        work_exp_count = int(request.form.get('work_exp_count', 0))
        for i in range(work_exp_count):
            company = request.form.get(f'company_{i}')
            position = request.form.get(f'position_{i}')
            start_date = request.form.get(f'start_date_{i}')
            end_date = request.form.get(f'end_date_{i}')
            description = request.form.get(f'description_{i}')
            
            if company and position:  # Only add if essential fields are present
                cursor.execute(
                    "INSERT INTO work_experience (candidate_id, company, position, start_date, end_date, description) VALUES (%s, %s, %s, %s, %s, %s)",
                    (candidate_id, company, position, start_date, end_date, description)
                )
        
        # Handle skills updates
        # First, delete existing skills
        cursor.execute("DELETE FROM skills WHERE candidate_id = %s", (candidate_id,))
        
        # Insert updated skills
        skills_count = int(request.form.get('skills_count', 0))
        for i in range(skills_count):
            skill_name = request.form.get(f'skill_name_{i}')
            years_experience = request.form.get(f'years_experience_{i}')
            
            if skill_name:  # Only add if skill name is present
                try:
                    years_exp = float(years_experience) if years_experience else 0
                except ValueError:
                    years_exp = 0
                    
                cursor.execute(
                    "INSERT INTO skills (candidate_id, skill_name, years_experience) VALUES (%s, %s, %s)",
                    (candidate_id, skill_name, years_exp)
                )
        
        # Process certificates
        certificates_count = int(request.form.get('certificates_count', 0))
        
        # First, delete existing certificates
        cursor.execute("DELETE FROM certificates WHERE candidate_id = %s", (candidate_id,))
        
        # Insert updated certificates
        for i in range(certificates_count):
            name = request.form.get(f'cert_name_{i}')
            if name:  # Only insert if there's at least a name
                cursor.execute("""
                    INSERT INTO certificates (candidate_id, name, issuer, date_obtained, 
                                           expiry_date, description)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    candidate_id,
                    name,
                    request.form.get(f'cert_issuer_{i}', ''),
                    request.form.get(f'cert_date_obtained_{i}', ''),
                    request.form.get(f'cert_expiry_date_{i}', ''),
                    request.form.get(f'cert_description_{i}', '')
                ))
        
        conn.commit()
        conn.close()
        
        flash('Candidate information updated successfully')
        return redirect(url_for('view_candidate', candidate_id=candidate_id))
    
    # GET request - show edit form
    conn = get_db_connection()
    cursor = conn.cursor(as_dict=True)  # Use dictionary cursor for GET
    
    # Get candidate info
    cursor.execute("SELECT * FROM candidates WHERE id = %s", (candidate_id,))
    candidate = cursor.fetchone()
    
    if not candidate:
        flash('Candidate not found', 'error')
        return redirect(url_for('view_candidates'))
    
    # Get work experience
    cursor.execute("SELECT * FROM work_experience WHERE candidate_id = %s ORDER BY start_date DESC", (candidate_id,))
    work_experience = cursor.fetchall()
    
    # Get skills
    cursor.execute("SELECT * FROM skills WHERE candidate_id = %s ORDER BY years_experience DESC", (candidate_id,))
    skills = cursor.fetchall()
    
    # Get certificates
    cursor.execute("SELECT * FROM certificates WHERE candidate_id = %s ORDER BY date_obtained DESC", (candidate_id,))
    certificates = cursor.fetchall()
    
    conn.close()
    
    # Add debug output to check what data is being retrieved
    print(f"Editing candidate: {candidate['name']} with ID: {candidate['id']}")
    print(f"Skills count: {len(skills)}")
    print(f"Work experience count: {len(work_experience)}")
    print(f"Certificates count: {len(certificates)}")
    
    return render_template('edit_candidate.html', 
                          candidate=candidate, 
                          work_experience=work_experience, 
                          skills=skills,
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
                                'reason': f"Candidate has {best_years} years of {best_match}, required {req_years} years."
                            })
                        else:
                            required_match_score += 0.5
                            details.append({
                                'skill_name': skill_name,
                                'type': 'Required',
                                'match': False,
                                'reason': f"Candidate has {best_years} years of {best_match}, required {req_years} years."
                            })
                    else:
                        details.append({
                            'skill_name': skill_name,
                            'type': 'Required',
                            'match': False,
                            'reason': "Skill not found in candidate's profile."
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
                                    'reason': f"Candidate has {cand_years} years, preferred {req_years} years."
                                })
                            else:
                                nice_to_have_match_score += 0.5
                                details.append({
                                    'skill_name': skill_name,
                                    'type': 'Nice to Have',
                                    'match': False,
                                    'reason': f"Candidate has {cand_years} years, preferred {req_years} years."
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
                            'reason': f"Candidate has {best_years} years of {best_match}, required {req_years} years."
                        })
                    else:
                        required_match_score += 0.5
                        details.append({
                            'skill_name': skill_name,
                            'type': 'Required',
                            'match': False,
                            'reason': f"Candidate has {best_years} years of {best_match}, required {req_years} years."
                        })
                else:
                    details.append({
                        'skill_name': skill_name,
                        'type': 'Required',
                        'match': False,
                        'reason': "Skill not found in candidate's profile."
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
                                'reason': f"Candidate has {cand_years} years, preferred {req_years} years."
                            })
                        else:
                            nice_to_have_match_score += 0.5
                            details.append({
                                'skill_name': skill_name,
                                'type': 'Nice to Have',
                                'match': False,
                                'reason': f"Candidate has {cand_years} years, preferred {req_years} years."
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
        
        # For each candidate, get their other skills
        for candidate in skill_candidates:
            cursor.execute("""
                SELECT skill_name, years_experience
                FROM skills
                WHERE candidate_id = %s AND LOWER(skill_name) != LOWER(%s)
                ORDER BY years_experience DESC
            """, (candidate['id'], skill))
            
            other_skills = cursor.fetchall()
            
            # No need to convert to dictionary
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
        return redirect(url_for('view_candidate', candidate_id=candidate_id))

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

if __name__ == '__main__':
    app.run(debug=True)
