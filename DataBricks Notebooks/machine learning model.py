# Import required libraries
import pandas as pd
from pyspark.sql import SparkSession
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
import joblib  # Library to save and load the model
import os  # Library to handle file operations

# Create a Spark session
spark = SparkSession.builder.appName("ModelTraining").getOrCreate()

# Sample Data Creation
data = {
    'pub_rec_bankruptcies': [0, 1, 0, 3, 0, 2],
    'acc_now_delinq': [1, 0, 2, 0, 0, 1],
    'delinq_2Yrs': [0, 1, 0, 1, 0, 2],
    'pub_rec': [0, 1, 1, 0, 2, 1],
    'emp_title': ['Engineer', 'Scientist', 'Teacher', 'Doctor', 'Nurse', 'Lawyer'],
    'emp_length': [5, 3, 10, 2, 6, 1],
    'payment_status': ['Paid', 'Unpaid', 'Paid', 'Unpaid', 'Paid', 'Unpaid'],
    'fraud_indicator': [0, 1, 0, 1, 0, 1],
    'suspicious_flag': [0, 1, 0, 0, 1, 1],
    'anomaly_score': [0.1, 0.2, 0.3, 0.4, 0.5, 0.6],
    'transaction_status': ['Completed', 'Failed', 'Completed', 'Failed', 'Completed', 'Failed'],
    'is_eligible': [1, 0, 1, 0, 1, 0]  # Sample target column
}

# Create a DataFrame
df = pd.DataFrame(data)

# Step 1: Preprocess Data
# Encoding categorical variables
df['emp_title'] = pd.factorize(df['emp_title'])[0]
df['payment_status'] = pd.factorize(df['payment_status'])[0]
df['transaction_status'] = pd.factorize(df['transaction_status'])[0]

# Fill NaN values (if any)
df.fillna(0, inplace=True)

# Define features and target
X = df.drop(columns=['is_eligible'])  # Features
y = df['is_eligible']  # Target variable

# Step 2: Define Scoring System
def calculate_score(row):
    score = 0
    
    # emp_title scoring
    if row['emp_title'] in [0, 1]:  # Engineer or Scientist
        score += 3
    elif row['emp_title'] == 2:  # Teacher
        score += 1
    elif row['emp_title'] == 4:  # Nurse
        score -= 2
    
    # emp_length scoring
    if row['emp_length'] >= 10:
        score += 2
    elif row['emp_length'] >= 5:
        score += 1
    else:
        score -= 2
    
    # pub_rec_bankruptcies scoring
    if row['pub_rec_bankruptcies'] == 0:
        score += 2
    elif row['pub_rec_bankruptcies'] == 1:
        score += 1
    else:
        score -= 2
    
    # acc_now_delinq scoring
    if row['acc_now_delinq'] == 0:
        score += 2
    elif row['acc_now_delinq'] == 1:
        score += 1
    else:
        score -= 2
    
    # delinq_2Yrs scoring
    if row['delinq_2Yrs'] == 0:
        score += 2
    elif row['delinq_2Yrs'] == 1:
        score += 1
    else:
        score -= 2
    
    # fraud_indicator scoring
    if row['fraud_indicator'] == 0:
        score += 2
    else:
        score -= 3
    
    # suspicious_flag scoring
    if row['suspicious_flag'] == 0:
        score += 2
    else:
        score -= 2
    
    # anomaly_score scoring
    if row['anomaly_score'] < 0.2:
        score += 2
    elif row['anomaly_score'] <= 0.5:
        score += 1
    else:
        score -= 3
    
    # payment_status scoring
    if row['payment_status'] == 0:  # Paid
        score += 3
    else:
        score -= 3
    
    return score

# Apply scoring to each row
df['score'] = df.apply(calculate_score, axis=1)

# Step 3: Define a threshold for eligibility
threshold = 5  # Adjust threshold as needed
df['is_eligible'] = df['score'] > threshold

# Define features again excluding the score
X = df.drop(columns=['is_eligible', 'score'])  # Features
y = df['is_eligible']  # Target variable

# Step 4: Train the Model
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
model = RandomForestClassifier(random_state=42)
model.fit(X_train, y_train)

# Step 5: Save the Model
model_dir = '/dbfs/mnt/models/'
os.makedirs(model_dir, exist_ok=True)  # Create the directory if it doesn't exist
model_path = os.path.join(model_dir, 'fraud_detector.pkl')
joblib.dump(model, model_path)  # Save the model to DBFS

print("Model trained and saved successfully.")
