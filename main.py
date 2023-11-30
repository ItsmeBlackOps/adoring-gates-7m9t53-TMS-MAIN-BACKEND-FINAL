import warnings
import psycopg2
from dateutil import parser as date_parser
import pytz
from flask import Flask, request, jsonify
from prometheus_flask_exporter import PrometheusMetrics
from flask_cors import CORS
import re
from fuzzywuzzy import process
from psycopg2 import sql
from psycopg2.extras import execute_values


warnings.filterwarnings("ignore")

app = Flask(__name__)
CORS(app)


def extract_date_time(text_data):
    # Define a pattern to extract the time zone
    timezone_pattern = r"\((.*?)\)"
    timezone_match = re.search(timezone_pattern, text_data)
    timezone_str = timezone_match.group(1).strip() if timezone_match else None

    # Extract the date and time string (excluding time zone)
    date_time_pattern = r"Data and Time of Interview \(Mention time zone\):\s*(.*?)\s*Duration"
    date_time_match = re.search(date_time_pattern, text_data)
    date_time_str = date_time_match.group(
        1).strip() if date_time_match else None

    if date_time_str and timezone_str:
        # Combine date and time with timezone
        date_time_with_timezone = f"{date_time_str} ({timezone_str})"

        # Use dateutil.parser to parse the date and time string
        try:
            parsed_date_time = date_parser.parse(
                date_time_with_timezone, fuzzy=True)
            parsed_date_time = parsed_date_time.astimezone(pytz.UTC)
            date_time = parsed_date_time.isoformat() + 'Z'
        except ValueError:
            date_time = None
    else:
        date_time = None

    return date_time


def extract_candidate_details(text_data):
    details = {}

    # Patterns for extracting each detail
    patterns = {
        "Candidate Name": r"Candidate Name:\s*(.*)",
        "Birth date": r"Birth date:\s*(.*)",
        "Gender": r"Gender:\s*(.*)",
        "Education": r"Education:\s*(.*)",
        "University": r"University:\s*(.*)",
        "Total Experience in Years": r"Total Experience in Years:\s*(.*)",
        "State": r"State:\s*(.*)",
        "Technology": r"Technology:\s*(.*)",
        "End Client": r"End Client:\s*(.*)",
        "Interview Round": r"Interview Round 1st 2nd  3rd  or Final round:\s*(.*)",
        "Job Title in JD": r"Job Title in JD:\s*(.*)",
        "Email ID": r"Email ID:\s*(.*)",
        "Contact Number": r"Personal Contact Number:\s*(.*)",
        "Date and Time of Interview": r"Date and Time of Interview \(Mention time zone\):\s*(.*)",
        "Duration": r"Duration:\s*(.*)",
        "Previous Support by/Preferred by Candidate": r"Previous Support by/Preferred by Candidate:\s*(.*)",
        "Subject": r"Subject:\s*(.*)"
    }

    # Extracting each detail using the corresponding pattern
    for key, pattern in patterns.items():
        match = re.search(pattern, text_data)
        if match:
            details[key] = match.group(1).strip()
    return details


def get_or_create_candidate(cursor, data):
    # This function would need to check if the candidate exists and either get the ID or create a new candidate
    # For demonstration, let's assume we are creating a new candidate
    cursor.execute("""
        INSERT INTO candidates (candidate_name, candidate_phone, candidate_email, gender_id, age, education, university, technology, total_experience_year, state_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (candidate_email) DO UPDATE SET
        candidate_phone = EXCLUDED.candidate_phone,
        age = EXCLUDED.age,
        education = EXCLUDED.education,
        university = EXCLUDED.university,
        technology = EXCLUDED.technology,
        total_experience_year = EXCLUDED.total_experience_year,
        state_id = EXCLUDED.state_id
        RETURNING candidate_id;
    """, (
        data['Candidate Name'],
        data['Contact Number'],
        data['Email ID'],
        data['Gender'],
        data['Birth date'],
        data['Education'],
        data['University'],
        data['Technology'],
        data['Total Experience in Years'],
        data['State']
    ))
    return cursor.fetchone()[0]


def get_task_type_id(cursor, task_type_name):
    cursor.execute(
        "SELECT task_type_id FROM task_types WHERE task_type_name = %s;", (task_type_name,))
    result = cursor.fetchone()
    return result[0] if result else None


def get_gender_id(cursor, gender_name, all_genders):
    cursor.execute("SELECT gender_id, gender_name FROM gender;")
    genders = cursor.fetchall()

    # If exact match not found, use fuzzy matching to find the closest one
    if not genders:
        return None

    # Create a dictionary for gender_name to gender_id mapping
    gender_dict = {name: g_id for g_id, name in genders}
    all_genders = list(gender_dict.keys())

    # Use fuzzy matching to find the closest gender name
    closest_match = process.extractOne(
        gender_name, [g[1] for g in genders], score_cutoff=80)
    return closest_match[2] if closest_match else None


def get_state_id(cursor, state_name):
    cursor.execute("SELECT state_id, state_name FROM states;")
    states = cursor.fetchall()

    # If no states found, return None
    if not states:
        return None

    # Fuzzy matching to find the closest state name
    closest_match = process.extractOne(
        state_name, [s[1] for s in states], score_cutoff=80)
    return closest_match[2] if closest_match else None


def get_most_selected_user_id(cursor):
    cursor.execute("""
        SELECT user_id FROM users
        GROUP BY user_id
        ORDER BY COUNT(*) DESC
        LIMIT 1;
    """)
    result = cursor.fetchone()
    return result[0] if result else None


def insert_into_main(cursor, candidate_id, task_type_id, user_id, data):
    cursor.execute("""
        INSERT INTO main (candidate_id, task_type_id, user_id, job_title, interview_round, date_time_timezone, duration_in_hours)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
    """, (
        candidate_id,
        task_type_id,
        user_id,
        data['Job Title in JD'],
        data['Interview Round'],
        data['Date and Time of Interview'],
        data['Duration']
    ))


def enter_data_into_db(extracted_data):
    # Connect to the database
    with psycopg2.connect(**db_credentials) as conn:
        with conn.cursor() as cursor:
            # Start transaction
            conn.autocommit = False

            # Insert or retrieve candidate_id
            gender_id = get_gender_id(cursor, extracted_data['Gender'])
            state_id = get_state_id(cursor, extracted_data['State'])
            candidate_id = get_or_create_candidate(
                cursor, extracted_data, gender_id, state_id)

            # Get task_type_id from the task_types table
            task_type_id = get_task_type_id(
                cursor, extracted_data['Task Type'])

            # Get user_id for the most selected user for previous support
            user_id = get_most_selected_user_id(cursor)

            # Insert into the main table
            insert_into_main(cursor, candidate_id,
                             task_type_id, user_id, extracted_data)

            # Commit the transaction
            conn.commit()


def find_closest_match(subject, task_types):
    # Find the closest match for the subject in the list of task types
    closest_match = process.extractOne(subject, task_types, score_cutoff=80)
    return closest_match[0] if closest_match else None


db_credentials = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "vizvacons123",
    "host": "tmsdb.cnqltqgk9yzu.us-east-1.rds.amazonaws.com",
    "port": "5432"
}


@app.route('/process_data', methods=['POST'])
def process_data():
    try:
        # Get the text data sent from the client
        text_data = request.data.decode('utf-8')

        if not text_data:
            return jsonify({"error": "No data provided"}), 400

        # Extract candidate details
        data_dict = extract_candidate_details(text_data)

        # Process date and time of interview
        if 'Date and Time of Interview' in data_dict:
            date_time = extract_date_time(
                data_dict['Date and Time of Interview'])
            if date_time:
                data_dict['Date and Time of Interview'] = date_time

        # Get task type ID based on subject
        subject = data_dict.get("Subject")
        # This should come from your actual task type list
        task_types = ["List", "Of", "Task", "Types"]
        task_type = find_closest_match(subject, task_types)

        if task_type:
            # Enter the data into the database
            with psycopg2.connect(**db_credentials) as conn:
                with conn.cursor() as cursor:
                    conn.autocommit = False  # Begin transaction
                    try:
                        candidate_id = get_or_create_candidate(
                            cursor, data_dict)
                        task_type_id = get_task_type_id(cursor, task_type)
                        user_id = get_most_selected_user_id(cursor)
                        insert_into_main(cursor, candidate_id,
                                         task_type_id, user_id, data_dict)
                        conn.commit()  # Commit the transaction
                    except Exception as e:
                        conn.rollback()  # Rollback the transaction on error
                        raise

            return jsonify(data_dict), 200
        else:
            return jsonify({"error": "Failed to determine the task type."}), 400

    except Exception as e:
        # Log the error here
        print("Error:", str(e))
        return jsonify({"error": "An error occurred while processing the data."}), 500


if __name__ == '__main__':
    app.run(debug=True)
