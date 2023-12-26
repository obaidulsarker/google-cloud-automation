from google.cloud import secretmanager
import json

def get_secret(project_id, secret_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")

def get_credentials_from_secret_manager(project_id,secret_name):
    secret_data = json.loads(get_secret(project_id, secret_name))
    return secret_data

def main():
    secret_name = "small-db-automation"  # Replace with the name of your secret in Secret Manager
    project_id ="bracit-dev"
    try:
        credentials_data = get_credentials_from_secret_manager(project_id, secret_name)
        # Use the credentials_data to create a client for the Google Cloud Client API
        # Example: client = your_api_client(credentials_data)
        print("Credentials successfully retrieved from Secret Manager.")
        print(credentials_data)
    except Exception as e:
        print(f"Error retrieving credentials: {str(e)}")

if __name__ == "__main__":
    main()
