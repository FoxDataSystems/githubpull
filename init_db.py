import sqlite3

def init_db():
    conn = sqlite3.connect('cv_database.db')
    cursor = conn.cursor()

    # Create candidates table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS candidates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        email TEXT,
        phone TEXT,
        cv_text TEXT,
        file_path TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # Create skills table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS skills (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        candidate_id INTEGER,
        skill_name TEXT,
        years_experience REAL,
        FOREIGN KEY (candidate_id) REFERENCES candidates (id)
    )
    ''')

    # Create work_experience table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS work_experience (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        candidate_id INTEGER,
        company TEXT,
        position TEXT,
        start_date TEXT,
        end_date TEXT,
        description TEXT,
        FOREIGN KEY (candidate_id) REFERENCES candidates (id)
    )
    ''')

    # Create certificates table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS certificates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        candidate_id INTEGER,
        name TEXT,
        issuer TEXT,
        date_obtained TEXT,
        expiry_date TEXT,
        description TEXT,
        FOREIGN KEY (candidate_id) REFERENCES candidates (id)
    )
    ''')

    # Create search_history table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS search_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        search_term TEXT NOT NULL,
        search_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        results_count INTEGER
    )
    ''')

    conn.commit()
    conn.close()

if __name__ == '__main__':
    init_db()
    print("Database initialized successfully!") 