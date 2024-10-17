# Import required libraries
import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sqlalchemy import create_engine
import joblib
import os

# Create a Spark session
spark = SparkSession.builder.appName("MyApp").getOrCreate()

# This cell will execute when the notebook is triggered

# Get parameters passed from the Flask application
dbutils.widgets.text("national_id", "")
dbutils.widgets.text("name", "")
dbutils.widgets.text("mother_name", "")
dbutils.widgets.text("gender", "")
dbutils.widgets.text("city", "")
dbutils.widgets.text("zip_code", "")
dbutils.widgets.text("address", "")
dbutils.widgets.text("email", "")

# Retrieve the parameters
national_id = dbutils.widgets.get("national_id")
name = dbutils.widgets.get("name")
mother_name = dbutils.widgets.get("mother_name")
gender = dbutils.widgets.get("gender")
city = dbutils.widgets.get("city")
zip_code = dbutils.widgets.get("zip_code")
address = dbutils.widgets.get("address")
email = dbutils.widgets.get("email")

# Print the received parameters to indicate successful execution
print("Databricks notebook triggered successfully!")
print(f"Received parameters: National ID: {national_id}, Name: {name}, Mother's Name: {mother_name}, Gender: {gender}, City: {city}, Zip Code: {zip_code}, Address: {address}, Email: {email}")

# Step 1: Set Up Database Connections
# Define JDBC connection details
warehouse_url = "jdbc:sqlserver://warehouseproject.sql.azuresynapse.net;databaseName=Dproject;user=geeks;password=helloWorld0;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
bank3_url = "jdbc:sqlserver://fraudd.database.windows.net;databaseName=bank 3;user=geeks;password=helloWorld0;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

# Convert national_id from string to float
float_national_id = float(national_id)
formatted_national_id = f"{float_national_id:.2e}".replace('e+', 'e+0')
print(f"Formatted National ID: {formatted_national_id}")

# Step 2: Fetch Data
query = """
SELECT DISTINCT
    c.pub_rec_bankruptcies,
    f.acc_now_delinq,
    f.delinq_2Yrs,  -- Fixed the casing to match model training
    d.pub_rec,
    e.emp_title,
    e.emp_length,
    f.payment_status,
    f.fraud_indicator,
    f.suspicious_flag,
    f.anomaly_score,
    f.transaction_status
FROM dbo.Customer AS c
JOIN dbo.Debt AS d ON c.debt_id = d.debt_id
JOIN dbo.Fact_Transactions_Loans AS f ON c.customer_id = f.customer_id
JOIN dbo.Employment AS e ON c.Employment_id = e.employment_id
WHERE c.national_id = '{}'
""".format(formatted_national_id)

# Load data from SQL Server using JDBC
try:
    data = spark.read \
        .format("jdbc") \
        .option("url", warehouse_url) \
        .option("dbtable", f"({query}) as query") \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load()

    data.show()  # Display the data to verify loading
except Exception as e:
    print(f"Error loading data: {e}")

# Step 3: Preprocess Data
# Convert Spark DataFrame to Pandas DataFrame for sklearn
data = data.toPandas()
print("Data loaded into Pandas DataFrame.")

# Handle missing values (customize as needed)
data.fillna(0, inplace=True)

# Add a temporary target column 'is_eligible' and set all values to 0
data['is_eligible'] = 0

# Define features and target
X = data.drop(columns=['is_eligible'])  # Use the temporary target column as your y
y = data['is_eligible']  # The target column for training

# Step 4: Load the Model
model_path = '/dbfs/mnt/models/fraud_detector.pkl'
model = joblib.load(model_path)  # Load the pre-trained model
print('Model loaded successfully.')

# Step 5: Prepare New User Features for Prediction
new_user_features = pd.DataFrame({
    'pub_rec_bankruptcies': [0],  # Replace with actual feature values from the incoming user
    'acc_now_delinq': [0],
    'delinq_2Yrs': [0],  # Ensure the name matches the trained model
    'pub_rec': [0],
    'emp_title': ['Engineer'],  # Replace with actual value
    'emp_length': [5],
    'payment_status': ['Paid'],  # Convert categorical variables to numerical if necessary
    'fraud_indicator': [0],
    'suspicious_flag': [0],
    'anomaly_score': [0.1],
    'transaction_status': ['Completed']  # Ensure this is in a suitable format
})

# Perform One-Hot Encoding
new_user_encoded = pd.get_dummies(new_user_features, drop_first=True)

# Ensure the same columns as training data
missing_cols = set(X.columns) - set(new_user_encoded.columns)
for c in missing_cols:
    new_user_encoded[c] = 0

# Reorder the columns to match the training data
new_user_encoded = new_user_encoded[X.columns]

print("Shape of new_user_features after encoding:", new_user_encoded.shape)

# Check if the model is fitted
try:
    eligibility_probs = model.predict(new_user_encoded)
    print("Prediction:", eligibility_probs)
except Exception as e:
    print("Error in prediction:", e)

threshold = 0.5  # Define your threshold
eligibility = (eligibility_probs >= threshold).astype(int)
print("Eligibility predictions:", eligibility)

# Step 1: Database connection details for Bank 3
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
database_host = "fraudd.database.windows.net"
database_port = "1433"
database_name = "bank 3"
user = "geeks"
password = "helloWorld0"
table = "dbo.Customer"

# JDBC URL for the database
url = f'jdbc:sqlserver://{database_host}:{database_port};database={database_name}'

# Step 2: Query the database to get the maximum customer_id
max_customer_id_query = "(SELECT MAX(customer_id) AS max_customer_id FROM dbo.Customer)"

# Read the maximum customer_id from the Bank 3 database using JDBC
try:
    max_customer_id_df = spark.read \
        .format("jdbc") \
        .option("driver", driver) \
        .option("url", url) \
        .option("dbtable", f"({max_customer_id_query}) as max_id") \
        .option("user", user) \
        .option("password", password) \
        .load()

    # Collect the result and store it in a variable
    max_customer_id = max_customer_id_df.collect()[0]['max_customer_id']
    if max_customer_id is None:
        max_customer_id = 0  # If no customers exist, start from 0
    customer_id = max_customer_id + 1  # Increment for the new customer

    print(f"Max customer_id: {max_customer_id}, New customer_id: {customer_id}")

except Exception as e:
    print(f"Error fetching max customer_id: {str(e)}")
    customer_id = 1  # Default to 1 if error occurs

# Step 3: Insert Eligible Users
if eligibility[0] == 1:  # Assuming 1 means eligible
    values = [(customer_id, national_id, name, mother_name, email, gender, city, address, zip_code, 0)]  # Set pub_rec_bankruptcies to 0
    
    # Define schema for DataFrame
    columns = ['customer_id', 'national_id', 'name', 'mother_name', 'email', 'gender', 'city', 'address', 'zip_code', 'pub_rec_bankruptcies']

    # Create a Spark DataFrame
    df = spark.createDataFrame(values, schema=columns)

    # Write data to Bank 3 database using Spark JDBC
    try:
        df.write \
            .format("jdbc") \
            .option("driver", driver) \
            .option("url", url) \
            .option("dbtable", table) \
            .option("user", user) \
            .option("password", password) \
            .mode("append") \
            .save()

        print("Data successfully inserted into Bank 3")
    except Exception as e:
        print(f"Error inserting user into Bank 3: {str(e)}")


print('Notebook Ended')

