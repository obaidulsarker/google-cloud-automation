from dotenv import load_dotenv
from pathlib import Path
import os
import platform

class EnvVariables:
    def __init__(self):

        dotenv_path = None
        os_name = platform.system()

        if (os_name=="Windows"):
            dotenv_path = Path('cred\.env')
        else:
            dotenv_path = Path('cred/.env')

        load_dotenv(dotenv_path=dotenv_path)

        self.GOOGLE_SERVICE_ACCOUNT_FILE=None

        if (os_name=="Windows"):
            self.GOOGLE_SERVICE_ACCOUNT_FILE=os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE").replace("/", "\\")
        else:
            self.GOOGLE_SERVICE_ACCOUNT_FILE=os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE").replace("\\", "/")

        self.GCLOUD_AUTH_SCOPE=os.getenv("GCLOUD_AUTH_SCOPE")

        self.CLOUD_SECRET_NAME=os.getenv("CLOUD_SECRET_NAME")
        self.SECRET_VERSION=os.getenv("SECRET_VERSION")
        self.SECRET_VALUE=""

        secret_string=os.getenv("SECRET_VALUE")

        result = secret_string.split('[')
        
        if len(result)>1:
            self.SECRET_VALUE=result[1]
        else:
            self.SECRET_VALUE=result[0]

        self.PROJECT_ID=os.getenv("PROJECT_ID")
        self.REGION_NAME= os.getenv("REGION_NAME")
        self.ZONE_NAME=os.getenv("ZONE_NAME")

        self.SOURCE_VM_NAME=os.getenv("SOURCE_VM_NAME")
        self.SOURCE_VM_IP=os.getenv("SOURCE_VM_IP")

        self.GCLOUD_AUTH_SCOPE = os.getenv("GCLOUD_AUTH_SCOPE")
        self.VM_IMAGE_NAME_PREFIX = os.getenv("VM_IMAGE_NAME_PREFIX")
        self.STORAGE_LOCATION = os.getenv("STORAGE_LOCATION")
        self.DATA_DISK_MOUNT_POINT = os.getenv("DATA_DISK_MOUNT_POINT")
        self.VPC_NAME = os.getenv("VPC_NAME")
        self.FIXED_ID = os.getenv("FIXED_IP")
        self.SUBNETWORK_NAME = os.getenv("SUBNETWORK_NAME")
        self.MACHINE_TYPE_NAME = os.getenv("MACHINE_TYPE_NAME")
        self.VM_NAME_PREFIX = os.getenv("VM_NAME_PREFIX")
        self.DATA_DISK_NAME_PREFIX = os.getenv("DATA_DISK_NAME_PREFIX")
        self.DATA_DISK_BUFFER_STORAGE_GB = os.getenv("DATA_DISK_BUFFER_STORAGE_GB")
        self.BOOT_DISK_NAME = os.getenv("BOOT_DISK_NAME")
        self.DB_SERVICE_NAME = os.getenv("DB_SERVICE_NAME")
        self.SSH_USER = os.getenv("SSH_USER")
        self.SSH_USER_PASS = os.getenv("SSH_USER_PASS")
        self.SSH_PORT = os.getenv("SSH_PORT")

        self.DUMP_BINARY = os.getenv("DUMP_BINARY")
        self.DB_DATA_DIR = os.getenv("DB_DATA_DIR")
        self.TURN_OFF_PG_PARAMS = os.getenv("TURN_OFF_PG_PARAMS")
        self.DB_NAME = os.getenv("DB_NAME")
        self.DB_USER = os.getenv("DB_USER")
        self.DB_PORT = os.getenv("DB_PORT")

        self.APP_SCHEMA_NAME = os.getenv("APP_SCHEMA_NAME")
        self.DB_USER_PASS = os.getenv("DB_USER_PASS")
        self.SMALLDB_SCHEMA = os.getenv("SMALLDB_SCHEMA")
        self.KEEP_SCHEMA_ONLY = os.getenv("KEEP_SCHEMA_ONLY")
        self.BUFFER_STORAGE_DATA_DISK_GB = os.getenv("BUFFER_STORAGE_DATA_DISK_GB")
        self.BUCKET_NAME = os.getenv("BUCKET_NAME")
        self.BUCKET_DIR_FOR_DB_BACKUP = os.getenv("BUCKET_DIR_FOR_DB_BACKUP")
        self.BUCKET_DIR_FOR_LOG = os.getenv("BUCKET_DIR_FOR_LOG")
        self.BUCKET_MOUNT_DIR = os.getenv("BUCKET_MOUNT_DIR")
        self.RESOURCE_LABEL = os.getenv("RESOURCE_LABEL")

        self.SQL_XML_FILE = None
        self.TASK_XML_FILE = None
        self.PG_CONF_XML_FILE = None
        self.LOG_DIRECTORY = None
        self.PG_HBA_FILE = None
        self.AUTOMATION_DB = None
        self.PID_FILE = None
        self.LOG_FILE = None
        self.EMAIL_TEMPLATE=None
        self.NOTIFICATION_LOG=None

        # File Path
        if (os_name=="Windows"):
            self.SQL_XML_FILE = os.getenv("SQL_XML_FILE").replace("/", "\\")
            self.TASK_XML_FILE = os.getenv("TASK_XML_FILE").replace("/", "\\")
            self.PG_CONF_XML_FILE = os.getenv("PG_CONF_XML_FILE").replace("/", "\\")
            self.LOG_DIRECTORY = os.getenv("LOG_DIRECTORY").replace("/", "\\")
            self.PG_HBA_FILE = os.getenv("PG_HBA_FILE").replace("/", "\\")
            self.AUTOMATION_DB = os.getenv("AUTOMATION_DB").replace("/", "\\")
            self.PID_FILE = os.getenv("PID_FILE").replace("/", "\\")
            self.LOG_FILE = os.getenv("LOG_FILE").replace("/", "\\")
            self.EMAIL_TEMPLATE = os.getenv("EMAIL_TEMPLATE").replace("/", "\\")
            self.NOTIFICATION_LOG = os.getenv("NOTIFICATION_LOG").replace("/", "\\")
        else:
            self.SQL_XML_FILE = os.getenv("SQL_XML_FILE").replace("\\", "/")
            self.TASK_XML_FILE = os.getenv("TASK_XML_FILE").replace("\\", "/")
            self.PG_CONF_XML_FILE = os.getenv("PG_CONF_XML_FILE").replace("\\", "/")
            self.LOG_DIRECTORY = os.getenv("LOG_DIRECTORY").replace("\\", "/")
            self.PG_HBA_FILE = os.getenv("PG_HBA_FILE").replace("\\", "/")
            self.AUTOMATION_DB = os.getenv("AUTOMATION_DB").replace("\\", "/")
            self.PID_FILE = os.getenv("PID_FILE").replace("\\", "/")
            self.LOG_FILE = os.getenv("LOG_FILE").replace("\\", "/")
            self.EMAIL_TEMPLATE = os.getenv("EMAIL_TEMPLATE").replace("\\", "/")
            self.NOTIFICATION_LOG = os.getenv("NOTIFICATION_LOG").replace("\\", "/")

        # Notification
        self.SMTP_LOGIN_USERNAME = os.getenv("SMTP_LOGIN_USERNAME")
        self.SMTP_LOGIN_PASSWORD = os.getenv("SMTP_LOGIN_PASSWORD")

        self.SMTP_SERVER = os.getenv("SMTP_SERVER")
        self.SMTP_PORT = os.getenv("SMTP_PORT")
        self.SENDER_EMAIL = os.getenv("SENDER_EMAIL")
        # self.SENDER_EMAIL_PASSWORD = os.getenv("SENDER_EMAIL_PASSWORD")
        self.RECEEIVER_EMAIL = os.getenv("RECEEIVER_EMAIL")
        self.NOTIFICATION_INTERVAL_HOUR = os.getenv("NOTIFICATION_INTERVAL_HOUR")
        self.TLS_ENABLED = os.getenv("TLS_ENABLED")
        self.EMAIL_PASS_AUTH_ENABLED = os.getenv("EMAIL_PASS_AUTH_ENABLED")
        self.EMAIL_SUBJECT = os.getenv("EMAIL_SUBJECT")

        # API
        self.PENDING_VOUCHER_CHECK_API = os.getenv("PENDING_VOUCHER_CHECK_API")
        
        # Disk Snapshot
        self.SNAPSHOT_PREFIX = os.getenv("SNAPSHOT_PREFIX")
        self.SNAPSHOT_RETENTION_DAYS = os.getenv("SNAPSHOT_RETENTION_DAYS")

def get_variables():
    try:
        env_variable = EnvVariables()

        return env_variable
    
    except Exception as e:
        print(f"Error: {e}")

   
if __name__ == "__main__":
    VARIABLES = EnvVariables()
    print(f"SSH_USER_PASS = {VARIABLES.SSH_USER_PASS}")
   