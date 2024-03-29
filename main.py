import warnings
import psycopg2
from flask import Flask, request, jsonify
from flask_cors import CORS
import re
from fuzzywuzzy import process
from loguru import logger
import spacy

nlp = spacy.load("en_core_web_sm")


warnings.filterwarnings("ignore")

app = Flask(__name__)
CORS(app)

log_messages = []


def add_log_message(message):
    log_messages.append(message)


@ app.route('/', methods=['GET'])
def get_logs():
    # Return all log messages as a JSON response
    return jsonify(log_messages)


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
        "Interview Round": r"Interview Round 1st 2nd 3rd or Final round:\s*(.*)",
        "Job Title in JD": r"Job Title in JD:\s*(.*)",
        "Email ID": r"Email ID:\s*(.*)",
        "Contact Number": r"Personal Contact Number:\s*(.*)",
        "Date and Time of Interview": r"Date and Time of Interview \(Mention time zone\):\s*(.*)|Date of Interview \(Mention time zone\):\s*(.*)",
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


def get_or_create_company(cursor, company_name):
    # Check if the company with the given name already exists
    cursor.execute(
        "SELECT company_id FROM companies WHERE company_name = %s;", (company_name,))
    result = cursor.fetchone()

    if result:
        return result[0]  # Return company_id if the company already exists
    else:
        # If the company doesn't exist, insert it and return the new company_id
        cursor.execute(
            "INSERT INTO companies (company_name) VALUES (%s) RETURNING company_id;", (company_name,))
        new_company_id = cursor.fetchone()[0]
        return new_company_id


def get_or_create_candidate(cursor, data, gender_id, state_id):
    # This function would need to check if the candidate exists and either get the ID or create a new candidate
    # For demonstration, let's assume we are creating a new candidate
    age = None
    total_experience_year_str = data.get('Total Experience in Years', '0')

    # Extract the numeric part (years) from the string
    total_experience_years = extract_numeric_years(total_experience_year_str)

    if total_experience_years is not None:
        age = total_experience_years  # Set age to the extracted value

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
        gender_id,  # Use gender_id obtained earlier
        age,  # Allow for None value
        data.get('Education', None),  # Allow for None value
        data.get('University', None),  # Allow for None value
        data.get('Technology', None),  # Allow for None value
        total_experience_years,  # Allow for None value
        state_id  # Use state_id obtained earlier
    ))

    result = cursor.fetchone()
    if result:
        return result[0]  # Return candidate_id if found
    else:
        return None  # Return None if no candidate is found


def extract_numeric_years(experience_str):
    try:
        # Extract numeric part from the string
        numeric_part = re.search(r'\b\d+\b', experience_str)
        if numeric_part:
            return int(numeric_part.group())
        else:
            return None
    except ValueError:
        return None


def get_task_type_id(cursor, task_type_name):
    cursor.execute(
        "SELECT task_type_id FROM task_type WHERE task_type_name = %s;", (task_type_name,))
    result = cursor.fetchone()
    return result[0] if result else None


def get_gender_id(cursor, gender_name):
    cursor.execute("SELECT gender_id, gender_name FROM gender;")
    genders = cursor.fetchall()

    if not genders:
        return None

    gender_dict = {name: g_id for g_id, name in genders}
    closest_match = process.extractOne(
        gender_name, gender_dict.keys(), score_cutoff=80)
    gender_id = gender_dict.get(closest_match[0]) if closest_match else 1

    add_log_message(f"Gender Name: {gender_name}")
    add_log_message(f"Matching Gender ID: {gender_id}")
    return gender_id


def get_state_id(cursor, state_name):
    try:
        add_log_message(f"Searching for State: {state_name}")
        # Query to retrieve state_id, acronym, and state_name from the 'state' table
        cursor.execute("SELECT state_id, acronym, state_name FROM state;")
        states = cursor.fetchall()

        # Check if no states are found
        if not states:
            add_log_message("No states found in the database.")
            return None

        # Check if the input state_name is an acronym or state_name
        matching_states = [state for state in states if state_name.lower(
        ) in state[1].lower() or state_name.lower() in state[2].lower()]

        if not matching_states:
            add_log_message(f"No match found for state name: {state_name}")
            return None

        # Extract the matching state ID
        state_id = matching_states[0][0]

        # Print the state name and matching state ID for debugging
        add_log_message(f"State Name: {state_name}")
        add_log_message(f"Matching State ID: {state_id}")

        return state_id

    except Exception as e:
        add_log_message(f"Error: {str(e)}")
        return None


def get_user_id_by_preference(cursor, preference):
    # This function should retrieve the user_id based on the candidate's preference
    # Modify this function according to your database schema and logic.
    # For demonstration, let's assume you have a table 'users' with 'user_id' and 'name' columns
    cursor.execute(
        "SELECT user_id FROM users WHERE user_name = %s;", (preference,))
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
        # Store as a text field or None if not available
        data.get('Interview Round', None),
        data['Date and Time of Interview'],
        data['Duration']
    ))


def enter_data_into_db(extracted_data, task_type, user_id):
    with psycopg2.connect(**db_credentials) as conn:
        with conn.cursor() as cursor:
            conn.autocommit = False

            # Convert gender and state names to their IDs
            gender_id = get_gender_id(cursor, extracted_data['Gender'])
            state_id = get_state_id(cursor, extracted_data['State'])
            if gender_id is None or state_id is None:
                raise ValueError("Gender or State not found.")

            # Parse date and time of the interview
            interview_datetime = extracted_data['Date and Time of Interview']
            if interview_datetime is None:
                raise ValueError("Invalid Date and Time of Interview format.")

            # Update extracted_data with formatted date and time
            extracted_data['Date and Time of Interview'] = interview_datetime

            # Insert or retrieve candidate_id
            candidate_id = get_or_create_candidate(
                cursor, extracted_data, gender_id, state_id)

            # Get task_type_id based on the subject
            task_type_id = get_task_type_id(cursor, task_type)

            # Check if there is an "End Client" in the extracted data
            end_client = extracted_data.get('End Client')

            if end_client:
                # Get or create the company and get its company_id
                company_id = get_or_create_company(cursor, end_client)

                try:
                    # Insert into the main table with the associated company_id
                    cursor.execute("""
                        INSERT INTO main (candidate_id, task_type_id, user_id, job_title, interview_round, date_time_timezone, duration_in_hours, company_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                    """, (
                        candidate_id,
                        task_type_id,
                        user_id,
                        extracted_data['Job Title in JD'],
                        # Store as a text field or None if not available
                        extracted_data.get('Interview Round', None),
                        extracted_data['Date and Time of Interview'],
                        extracted_data['Duration'],
                        company_id  # Pass the company_id
                    ))
                    add_log_message("Database operation successful")
                except Exception as e:
                    # Log the error if the database operation fails
                    error_message = f"Database operation failed: {e}"
                    add_log_message(error_message)
                    conn.rollback()  # Rollback the transaction in case of failure
            else:
                # Insert into the main table without the company_id
                try:
                    insert_into_main(cursor, candidate_id,
                                     task_type_id, user_id, extracted_data)
                    add_log_message("Database operation successful")
                except Exception as e:
                    # Log the error if the database operation fails
                    error_message = f"Database operation failed: {e}"
                    add_log_message(error_message)
                    conn.rollback()  # Rollback the transaction in case of failure

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


@ app.route('/process_data', methods=['POST'])
def process_data():
    try:
        # Get the text data sent from the client
        text_data = request.data.decode('utf-8')
        if not text_data:
            return jsonify({"error": "No data provided"}), 400

        # Extract candidate details
        data_dict = extract_candidate_details(text_data)
        add_log_message(f"Extracted data: {data_dict}")  # Add log message

        # Extract "Interview Round" as a string
        interview_round = data_dict.get("Interview Round", "")
        # Add log message
        add_log_message(f"Interview Round: {interview_round}")

        # Process date and time of interview
        if 'Date and Time of Interview' in data_dict:
            date_time = data_dict['Date and Time of Interview']
            if date_time:
                data_dict['Date and Time of Interview'] = date_time

        # Get task type ID based on subject
        subject = data_dict.get("Subject")
        task_types = ['Resume Understanding', 'Resume Making/Reviewing',
                      'Technical Support', 'Assessment', 'Job Support', 'Training', 'Mock Interviews']
        task_type = find_closest_match(subject, task_types)
        add_log_message(f"task_type: {task_type}")  # Add log message

        # Initialize user_id to None
        user_id = None

        try:
            # Check if there is a value in "Previous Support by/Preferred by Candidate"
            preferred_by_candidate = data_dict.get(
                'Previous Support by/Preferred by Candidate')

            if preferred_by_candidate:
                # Fetch user_id based on the candidate's preference
                with psycopg2.connect(**db_credentials) as conn:
                    with conn.cursor() as cursor:
                        user_id = get_user_id_by_preference(
                            cursor, preferred_by_candidate)
                        if user_id:
                            add_log_message(
                                f"User ID based on candidate preference: {user_id}")
                        else:
                            add_log_message(
                                "User not found based on candidate preference.")

            # Enter the data into the database using the function
            enter_data_into_db(data_dict, task_type, user_id)
            add_log_message("Database operation successful")
            return jsonify(data_dict), 200
        except Exception as e:
            # Log the error
            error_message = f"Database operation failed: {e}"
            add_log_message(error_message)
            return jsonify({"error": "Database operation failed"}), 500

    except Exception as e:
        # Log the error
        error_message = f"An error occurred while processing the data: {e}"
        add_log_message(error_message)
        return jsonify({"error": "An error occurred while processing the data."}), 500


if __name__ == '__main__':
    app.run(debug=True)
