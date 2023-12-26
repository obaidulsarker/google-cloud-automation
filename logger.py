import logging
from gcs_operation import *

class Logger(GCSoperation):
    def __init__(self, logfile) -> None:
        try:
            super().__init__() 
            # Set up logging
            self.log_file = logfile
            self.log_directory, self.log_file_name = os.path.split(logfile)
            
            #self.log_file_name = logfile.split('\\')[-1]
            self.remote_log_file_location=f"{self.bucket_dir_for_log}/{self.log_file_name}"
            #self.remote_log_file_location='logs/app_log.txt'
            logging.basicConfig(filename=logfile, level=logging.INFO, format='%(asctime)s [%(levelname)s]: %(message)s')
        
        except Exception as e:
            self.log_error(f"Exception: {str(e)}")
            #self.upload_file_to_gcs(local_log_file=logfile, remote_log_file=self.remote_log_file_location)
    
    def upload_log(self):
        self.upload_file_to_gcs(local_log_file=self.log_file, remote_log_file=self.remote_log_file_location)

    # Function to log errors
    def log_error(self, message):
        try:
            logging.error(message)
            self.upload_log()
        except Exception as e:
            print(f"Exception: {str(e)}")
         
    # Function to log warnings
    def log_warning(self, message):
        try:
            logging.warning(message)
            
        except Exception as e:
            self.log_error(f"Exception: {str(e)}")
            
    # Function to log info messages
    def log_info(self, message):
        try:
            logging.info(message)
            
        except Exception as e:
            self.log_error(f"Exception: {str(e)}")
           
    # Function to log fatal messages
    def log_fatal(self, message):
        try:
            logging.fatal(message)
            
        except Exception as e:
            self.log_error(f"Exception: {str(e)}")
           

# if __name__ == "__main__":
#     try:
#         local_log_file_path = r'logs\app_log.txt'
#         print(local_log_file_path.split('\\')[-1])
#         log = Logger(local_log_file_path)
#         # Log messages of different types
#         log.log_info("This is an info message.")
#         log.log_warning("This is a warning message.")
#         log.log_error("This is an error message.")
#         log.log_fatal("This is a fatal message.")
#         log.upload_log()
#     except Exception as e:
#         # Log any exceptions
#         log.log_error(f"Exception: {str(e)}")
#         log.upload_log()

  