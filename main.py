from flask import Flask, request, jsonify
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


def extract_date_time(normalized_text):
    # Define a pattern to extract the time zone
    timezone_pattern = r"\((.*?)\)"
    timezone_match = re.search(timezone_pattern, normalized_text)
    timezone_str = timezone_match.group(1).strip() if timezone_match else None

    # Extract the date and time string (excluding time zone)
    date_time_pattern = r"Data and Time of Interview \(Mention time zone\):\s*(.*?)\s*Duration"
    date_time_match = re.search(date_time_pattern, normalized_text)
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

        # Generate the placeholders dynamically for the insertion query
        fields = ', '.join(data_dict.keys())
        placeholders = ', '.join(['%s'] * len(data_dict))

        # Construct the dynamic insertion query
        insert_query = f"INSERT INTO your_table_name ({fields}) VALUES ({placeholders})"

        # Extract the values from the data dictionary and convert them to a tuple
        values = tuple(data_dict.values())

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
        # Get the JSON data sent from Power Automate
        data = request.json

        if not data:
            return jsonify({"error": "No data provided"}), 400

        # Extract information using the defined regular expressions
        candidate_name = data.get("candidate_name")
        birth_date = data.get("birth_date")
        gender = data.get("gender")
        education = data.get("education")
        university = data.get("university")
        total_experience = data.get("total_experience")
        state = data.get("state_name")
        technology = data.get("technology")
        end_client = data.get("end_client")
        interview_round = data.get("interview_round")
        job_title = data.get("job_title")
        email_address = data.get("email_id")
        contact_number = data.get("contact_number")
        date_time = extract_date_time(data.get("normalized_text"))
        duration = data.get("duration")

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
            "email_id": email_address,
            "contact_number": contact_number,
            "date_time_of_interview": date_time,
            "duration": duration,
        }

        # Store the data in a database
        success = store_data_in_database(data_dict)

        if success:
            return jsonify({"message": "Data received and stored successfully"}), 200
        else:
            return jsonify({"error": "Failed to store data in the database"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True)
