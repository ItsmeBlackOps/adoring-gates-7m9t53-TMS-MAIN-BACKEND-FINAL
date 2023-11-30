import warnings
import psycopg2
from dateutil import parser as date_parser
import pytz
from flask import Flask, request, jsonify
from prometheus_flask_exporter import PrometheusMetrics
from flask_cors import CORS
import re
from fuzzywuzzy import process

warnings.filterwarnings("ignore")

app = Flask(__name__)
metrics = PrometheusMetrics(app)

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

        # Extract candidate details using the new function
        data_dict = extract_candidate_details(text_data)

        # Check if date and time are included and process them
        if 'Date and Time of Interview' in data_dict:
            date_time = extract_date_time(
                data_dict['Date and Time of Interview'])
            if date_time:
                data_dict['Date and Time of Interview'] = date_time

        # Store the data in a database
        success = store_data_in_database(data_dict)

        if success:
            return jsonify(data_dict), 200
        else:
            return jsonify({"error": "Failed to store data in the database"}), 500
    except Exception as e:
        print("Error:", str(e))
        return str(e)


if __name__ == '__main__':
    app.run(debug=True)
