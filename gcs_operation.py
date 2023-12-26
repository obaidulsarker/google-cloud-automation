from google.oauth2 import service_account
from google.cloud import storage
from google.auth import exceptions
from init_variables import get_variables
import os

class GCSoperation():
    def __init__(self):
        self.project_id = get_variables().PROJECT_ID
        self.region = get_variables().REGION_NAME
        self.zone = get_variables().ZONE_NAME
        self.vpc_name = get_variables().VPC_NAME
        self.bucket_name= get_variables().BUCKET_NAME
        self.gcloud_auth_scopes= get_variables().GCLOUD_AUTH_SCOPE
        self.bucket_dir_for_log= get_variables().BUCKET_DIR_FOR_LOG
        self.bucket_dir_for_db_backup= get_variables().BUCKET_DIR_FOR_DB_BACKUP
        self.service_account_key_file = get_variables().GOOGLE_SERVICE_ACCOUNT_FILE
        
    def connect_gcs(self):

        # Set the GOOGLE_APPLICATION_CREDENTIALS environment variable
        #os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.service_account_key_file
        print(self.service_account_key_file)
        # Authenticate with the Google Cloud API using a service account key JSON file
        # Authenticate with the service account
        credentials = service_account.Credentials.from_service_account_file(
            self.service_account_key_file,
            scopes=[self.gcloud_auth_scopes]
        )

        # credentials = service_account.Credentials.from_service_account_file(
        #     service_account_key_file,
        #     scopes=['https://www.googleapis.com/auth/cloud-platform']
        # )

        # Connect Google cloud
        try:
            storage_client = storage.Client(credentials=credentials)
            return storage_client
        
        except exceptions.DefaultCredentialsError:
            print("Please set up your Google Cloud credentials.")
            return None
        except Exception as e:
            print(f"Error: {e}")
            return None

    # Function to sync log file to Google Cloud Storage
    def upload_file_to_gcs(self, local_log_file, remote_log_file):

        # Get the specified GCS bucket
        bucket = self.connect_gcs().bucket(self.bucket_name)

        try:
            # Upload the local log file to GCS
            blob = bucket.blob(remote_log_file)
            blob.upload_from_filename(local_log_file)
            print(f"Log file uploaded to GCS: gs://{self.bucket_name}/{remote_log_file}")
        except Exception as e:
            print(f"Error uploading log file to GCS: {str(e)}")

    def upload_folder_to_gcs(self, source_folder, destination_folder):

        try:

             # Get the specified GCS bucket
            bucket = self.connect().bucket(self.bucket_name)

            # List files in the source folder
            files = os.listdir(source_folder)

            for file_name in files:
                # Define the source and destination paths
                source_path = os.path.join(source_folder, file_name)
                destination_path = os.path.join(destination_folder, file_name)

                # Upload the file to the GCS bucket
                blob = bucket.blob(destination_path)
                blob.upload_from_filename(source_path)

                print(f"Uploaded {file_name} to {destination_path}")

        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Error: {e}")

    def get_gcs_file_size_by_bytes(self, file_name):
        try:

            # Get the specified GCS bucket
            bucket = self.connect_gcs().bucket(self.bucket_name)
            
            # Specify the bucket and file name
            blob = bucket.get_blob(file_name)
            
            if not blob.exists():
                print(f"File '{file_name}' not found in bucket '{self.bucket_name}'.")
                return None

            # print(blob)
            # print(blob.size)

            # Get the file size
            file_size = blob.size

            return file_size
        except Exception as e:
            print(f"Error: {e}")
            return None

    def get_gcs_file_size_by_kb(self, file_name):
        try:
            size = self.get_gcs_file_size_by_bytes(file_name=file_name)

            return round(size/(1024),1)

        except Exception as e:
            print(f"Error: {e}")
            return None
        
    def get_gcs_file_size_by_mb(self, file_name):
        try:
            size = self.get_gcs_file_size_by_bytes(file_name=file_name)

            return round(size/(1024*1024),1)

        except Exception as e:
            print(f"Error: {e}")
            return None

    def get_gcs_file_size_by_gb(self, file_name):
        try:
            size = self.get_gcs_file_size_by_bytes(file_name=file_name)

            return round(size/(1024*1024*1024),1)

        except Exception as e:
            print(f"Error: {e}")
            return None
    
    def get_gcs_file_size_by_tb(self, file_name):
        try:
            size = self.get_gcs_file_size_by_bytes(file_name=file_name)

            return round(size/(1024*1024*1024*1024),1)

        except Exception as e:
            print(f"Error: {e}")
            return None
        
# if __name__ == "__main__":
#     # Specify your GCS bucket name
#     operation_log="logs\log_26fb0e36-c91c-4f78-b2fb-01a2dd44b554_2023-10-28_23-40-10.log"
    
#     gcs = GCSoperation()

#     FileName = "erp/sbicloud_bd_archive_20231125063828.dump"
#     fileSize = gcs.get_gcs_file_size_by_gb(file_name=FileName)
#     print(f"File Size={fileSize}")