from flask import Flask, request, render_template
import pyodbc
import os
import requests
import json

app = Flask(__name__)

# Database connection details
server = os.getenv('DB_SERVER', 'warehouseproject.sql.azuresynapse.net')
database = os.getenv('DB_NAME', 'Dproject')
username = os.getenv('DB_USER', 'geeks')
password = os.getenv('DB_PASS', 'helloWorld0')  # Use environment variables for secure handling
driver = '{ODBC Driver 17 for SQL Server}'

# Databricks connection details
databricks_token = os.getenv('DATABRICKS_TOKEN')  # Securely load the token from environment variables
databricks_url = 'https://adb-269216331150110.10.azuredatabricks.net'
cluster_id = '1016-115932-2or3uxrn'
notebook_path = '/Workspace/Users/hm30102241300392@depi.eui.edu.eg/Fraud_Notebook'

# Function to establish a database connection
def get_db_connection():
    conn = pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}')
    return conn

# Route for the home page
@app.route('/', methods=['GET'])
def home():
    return render_template('index.html')

# Function to convert national ID to scientific notation string
def convert_national_id(national_id):
    # Convert to float and format as scientific notation with three digits in exponent
    float_id = float(national_id)
    return '{:.2e}'.format(float_id).replace('e+', 'e+0')  # Ensure exponent has leading zero

# Route to search customer by National ID
@app.route('/search_customer', methods=['POST'])
def search_customer():
    try:
        national_id = request.form['national_id']
        customer_name = request.form['customer_name']
        mother_name = request.form['mother_name']
        gender = request.form['gender']
        city = request.form['city']
        zip_code = request.form['zip_code']
        address = request.form['address']
        email = request.form['email']
        
        print("Received data:", request.form)  # Log the received data
        
        # Convert national ID to scientific notation
        scientific_national_id = convert_national_id(national_id)
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Using N prefix for Unicode string literals in SQL
        cursor.execute("SELECT * FROM dbo.Customer WHERE national_id = ?", scientific_national_id)
        result = cursor.fetchone()
        
        if result:
            # Sending customer information to Databricks
            data_to_send = {
                "national_id": national_id,
                "name": customer_name,
                "mother_name": mother_name,
                "gender": gender,
                "city": city,
                "zip_code": zip_code,
                "address": address,
                "email": email
            }
            
            # Define the job settings
            run_settings = {
                "run_name": "Customer Data Processing",
                "existing_cluster_id": cluster_id,
                "notebook_task": {
                    "notebook_path": notebook_path,
                    "base_parameters": data_to_send
                }
            }
            
            # Send the request to run the notebook
            response = requests.post(
                f"{databricks_url}/api/2.0/jobs/runs/submit",
                headers={"Authorization": f"Bearer {databricks_token}"},
                json=run_settings
            )
            
            if response.status_code == 200:
                return "Customer found and data sent to Databricks!"
            else:
                return f"Customer found but failed to send information to Databricks: {response.text}"

        else:
            # Insert customer information into 'bank 3' database
            bank_server = 'fraudd.database.windows.net'
            bank_database = 'bank 3'
            bank_username = 'geeks'
            bank_password = 'helloWorld0'
            bank_driver = '{ODBC Driver 17 for SQL Server}'
            
            bank_conn = pyodbc.connect(f'DRIVER={bank_driver};SERVER={bank_server};DATABASE={bank_database};UID={bank_username};PWD={bank_password}')
            bank_cursor = bank_conn.cursor()

            # Get the last customer_id
            bank_cursor.execute("SELECT MAX(customer_id) FROM dbo.Customer")
            last_customer_id = bank_cursor.fetchone()[0]
            new_customer_id = last_customer_id + 1 if last_customer_id else 1

            # Insert the new customer
            bank_cursor.execute("INSERT INTO dbo.Customer (customer_id, national_id, name, mother_name, email, gender, city, address, zip_code, pub_rec_bankruptcies) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                new_customer_id, national_id, customer_name, mother_name, email, gender, city, address, zip_code, 0)
            bank_conn.commit()
            bank_cursor.close()
            bank_conn.close()
            
            return "Customer not found! Added to bank 3 database."
    except Exception as e:
        return f"An error occurred: {str(e)}"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
