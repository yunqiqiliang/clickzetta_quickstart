import subprocess
import sys
import warnings
import contextlib
import os

# Suppress warnings
warnings.filterwarnings("ignore", message="A value is trying to be set on a copy of a slice from a DataFrame")

# Suppress stderr
@contextlib.contextmanager
def suppress_stderr():
    with open(os.devnull, 'w') as devnull:
        old_stderr = sys.stderr
        sys.stderr = devnull
        try:
            yield
        finally:
            sys.stderr = old_stderr

with suppress_stderr():
    # Install kaggle
    subprocess.run([sys.executable, "-m", "pip", "install", "kaggle", "--target", "/home/system_normal", "-i", "https://pypi.tuna.tsinghua.edu.cn/simple"], stderr=subprocess.DEVNULL)
    sys.path.append('/home/system_normal')

import pandas as pd
import boto3
import random
import os, json, io
import zipfile
from datetime import datetime

def load_random_sample(csv_file, sample_size):

    # Count total rows in the CSV file
    total_rows = sum(1 for line in open(csv_file, encoding='utf-8')) - 1  # Subtract header row

    # Calculate indices of rows to skip (non-selected)
    skip_indices = random.sample(range(1, total_rows + 1), total_rows - sample_size)

    # Load DataFrame with random sample of rows
    df = pd.read_csv(csv_file, skiprows=skip_indices)

    policy_table = df[['policy_id', 'subscription_length', 'region_code', 'segment']].copy()
    vehicles_table = df[['policy_id', 'vehicle_age', 'fuel_type', 'is_parking_sensors', 'is_parking_camera', 'rear_brakes_type', 'displacement', 'transmission_type', 'steering_type', 'turning_radius', 'gross_weight', 'is_front_fog_lights', 'is_rear_window_wiper', 'is_rear_window_washer', 'is_rear_window_defogger', 'is_brake_assist', 'is_central_locking', 'is_power_steering', 'is_day_night_rear_view_mirror', 'is_speed_alert', 'ncap_rating']].copy()
    customers_table = df[['policy_id', 'customer_age', 'region_density']].copy()
    claims_table = df[['policy_id', 'claim_status']].copy()

    vehicles_table.rename(columns={'policy_id': 'vehicle_id'}, inplace=True)
    customers_table.rename(columns={'policy_id': 'customer_id'}, inplace=True)
    claims_table.rename(columns={'policy_id': 'claim_id'}, inplace=True)

    return policy_table, vehicles_table, customers_table, claims_table

def upload_df_to_s3():
    try:
        with suppress_stderr():
            # Setup Kaggle API

            # Ensure the directory exists
            config_dir = '/home/system_normal/tempdata/.config/kaggle'
            if not os.path.exists(config_dir):
                os.makedirs(config_dir)

            # Create the kaggle.json file with the given credentials
            kaggle_json = {
                "username": ${kaggle_username},
                "key": ${kaggel_key}
            }
            with open(os.path.join(config_dir, 'kaggle.json'), 'w') as f:
                json.dump(kaggle_json, f)

            # Set the environment variable to the directory containing kaggle.json
            os.environ['KAGGLE_CONFIG_DIR'] = config_dir
            from kaggle.api.kaggle_api_extended import KaggleApi
            # Authenticate the Kaggle API
            api = KaggleApi()
            api.authenticate()

            # Define the dataset
            dataset = 'litvinenko630/insurance-claims'

            # Define the CSV file name
            csv_file = 'Insurance claims data.csv'

            # Download the entire dataset as a zip file
            api.dataset_download_files(dataset, path='/home/system_normal/tempdata')

            # Extract the CSV file from the downloaded zip file
            with zipfile.ZipFile('/home/system_normal/tempdata/insurance-claims.zip', 'r') as zip_ref:
                zip_ref.extract(csv_file, path='/home/system_normal/tempdata')

            policy_data, vehicles_data, customers_data, claims_data = load_random_sample(f'/home/system_normal/tempdata/{csv_file}', 20)
            # Convert DataFrame to CSV string
            policy = policy_data.to_csv(index=False)
            vehicles = vehicles_data.to_csv(index=False)
            customers = customers_data.to_csv(index=False)
            claims = claims_data.to_csv(index=False)

            current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Ensure you have set your AWS credentials in environment variables or replace the following with your credentials
            s3_client = boto3.client(
                's3',
                aws_access_key_id= ${aws_access_key_id},
                aws_secret_access_key= ${aws_secret_access_key},
                region_name= ${aws_region_name}
            )

            # Define S3 bucket and keys with current date and time
            s3_bucket = 'insurance-data-clickzetta-etl-project'
            s3_key_policy = f'policy/policy_{current_datetime}.csv'
            s3_key_vehicles = f'vehicles/vehicles_{current_datetime}.csv'
            s3_key_customers = f'customers/customers_{current_datetime}.csv'
            s3_key_claims = f'claims/claims_{current_datetime}.csv'

            # Upload to S3
            s3_client.put_object(Bucket=s3_bucket, Key=s3_key_policy, Body=policy)
            s3_client.put_object(Bucket=s3_bucket, Key=s3_key_vehicles, Body=vehicles)
            s3_client.put_object(Bucket=s3_bucket, Key=s3_key_customers, Body=customers)
            s3_client.put_object(Bucket=s3_bucket, Key=s3_key_claims, Body=claims)
            printf("upload_df_to_s3 down:{s3_key_policy},{s3_key_vehicles},{s3_key_customers},{s3_key_claims}")
    
    except Exception as e:
        pass  # Ignore errors

# Run the upload function
upload_df_to_s3()
