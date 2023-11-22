from flask import Flask, request, jsonify
from flask_cors import CORS
import re
from datetime import datetime
import pytz
from dateutil import parser as date_parser
import json
import psycopg2
import pandas as pd
import warnings
import re
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


def store_data_in_database(data_dict):
    try:
        # Connection parameters for your PostgreSQL database
        conn_params = {
            "dbname": "postgres",
            "user": "postgres",
            "password": "vizvacons123",
            "host": "tmsdb.cnqltqgk9yzu.us-east-1.rds.amazonaws.com",
            "port": "5432"
        }

        # Establishing a connection to the PostgreSQL database
        conn = psycopg2.connect(**conn_params)

        # Creating a cursor object using the cursor() method
        cursor = conn.cursor()
        print(cursor)
        # Generate the placeholders dynamically for the insertion query
        fields = ', '.join(data_dict.keys())
        placeholders = ', '.join(['%s'] * len(data_dict))

        # Construct the dynamic insertion query
        insert_query = f"INSERT INTO email_table ({fields}) VALUES ({placeholders})"

        # Extract the values from the data dictionary and convert them to a tuple
        values = tuple(data_dict.values())
        print(values)
        # Execute the dynamic insert query with parameterized values
        cursor.execute(insert_query, values)

        # Commit your changes in the database
        conn.commit()

        # Close the cursor and connection
        cursor.close()
        conn.close()

        return True  # Data was successfully stored in the database
    except Exception as e:
        return str(e)  # Return the error message if an exception occurs


@app.route('/process_data', methods=['POST'])
def process_data():
    try:
        # Get the text data sent from the client
        text_data = request.data.decode('utf-8')

        if not text_data:
            return jsonify({"error": "No data provided"}), 400
        print(text_data)
        # Define regular expressions for each field
        # Replace these with your actual regular expressions
        candidate_name_pattern = r"Candidate Name:\s*(.*?)\s*Birth date:"
        birth_date_pattern = r"Birth date:\s*(.*?)\s*Gender:"
        gender_pattern = r"Gender:\s*(.*?)\s*Education:"
        education_pattern = r"Education:\s*(.*?)\s*University:"
        university_pattern = r"University:\s*(.*?)\s*Total Experience in Years:"
        experience_pattern = r"Total Experience in Years:\s*(.*?)\s*State:"
        state_pattern = r"State:\s*(.*?)\s*Technology:"
        technology_pattern = r"Technology:\s*(.*?)\s*End Client:"
        end_client_pattern = r"End Client:\s*(.*?)\s*Interview Round"
        interview_round_pattern = r"Interview Round 1st 2nd 3rd or Final round\s*(.*?)\s*Job Title"
        job_title_pattern = r"Job Title in JD:\s*(.*?)\s*Email ID:"
        email_pattern = r"Email ID:\s*(.*?)\s*Personal Contact Number:"
        contact_number_pattern = r"Personal Contact Number:\s*(.*?)\s*Data and Time of Interview \(Mention time zone\):"
        duration_pattern = r"Duration (\d+\s*\w+)"

        # Extract information using the defined regular expressions
        candidate_name_match = re.search(
            candidate_name_pattern, text_data)
        candidate_name = candidate_name_match.group(
            1).strip() if candidate_name_match else None

        birth_date_match = re.search(birth_date_pattern, text_data)
        birth_date = birth_date_match.group(
            1).strip() if birth_date_match else None

        gender_match = re.search(gender_pattern, text_data)
        gender = gender_match.group(1).strip() if gender_match else None

        education_match = re.search(education_pattern, text_data)
        education = education_match.group(
            1).strip() if education_match else None

        university_match = re.search(university_pattern, text_data)
        university = university_match.group(
            1).strip() if university_match else None

        total_experience_match = re.search(experience_pattern, text_data)
        total_experience = total_experience_match.group(
            1).strip() if total_experience_match else None

        state_match = re.search(state_pattern, text_data)
        state = state_match.group(1).strip() if state_match else None

        technology_match = re.search(technology_pattern, text_data)
        technology = technology_match.group(
            1).strip() if technology_match else None

        end_client_match = re.search(end_client_pattern, text_data)
        end_client = end_client_match.group(
            1).strip() if end_client_match else None

        # Extract other fields similarly...

        # Create a dictionary with the extracted data
        data_dict = {
            "candidate_name": candidate_name,
            "birth_date": birth_date,
            "gender": gender,
            "education": education,
            "university": university,
            "total_experience": total_experience,
            "state_name": state,
            "technology": technology,
            "end_client": end_client,
            "interview_round": interview_round,
            "job_title": job_title,
            "email_id": email_id,
            "contact_number": contact_number,
            "duration": duration,
        }
        print(data_dict)
        # Store the data in a database
        success = store_data_in_database(data_dict)

        if success:
            return jsonify({"message": "Data received and stored successfully"}), 200
        else:
            return jsonify({"error": "Failed to store data in the database"}), 500
    except Exception as e:
        print("Error:", str(e))
        return str(e)


if __name__ == '__main__':
    app.run(debug=True)
