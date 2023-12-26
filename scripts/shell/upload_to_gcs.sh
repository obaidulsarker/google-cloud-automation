
#!/bin/bash
# Set the GCS bucket and folder paths
bucket_name=bits-database-backup
db_backup_path=~/sbicloud_bd_public_archive_20230922004937
destination_folder=erp

# Authenticate with Google Cloud using gcloud CLI
gcloud auth activate-service-account --key-file=/tmp/service_account_key.json

# Upload the database backup file to GCS
gsutil cp $db_backup_path gs://$bucket_name/$destination_folder/

# Check if the upload was successful
if [ $? -eq 0 ]; then
echo "Upload completed successfully."
else
echo "Upload failed."
fi
