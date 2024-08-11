import paramiko
from datetime import datetime
from init_variables import get_variables
from logger import *
import re

class ShellOperation(Logger):

    def __init__(self, logfile):
        super().__init__(logfile)
        self.ssh_host=get_variables().FIXED_ID
        self.ssh_username= get_variables().SSH_USER
        self.ssh_password=get_variables().SSH_USER_PASS
        self.ssh_port=get_variables().SSH_PORT
        self.db_password = get_variables().DB_USER_PASS

    # Connect Remote Server
    def ssh_connect(self):
        try:
            # Set up SSH client
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname=self.ssh_host, port=self.ssh_port, username=self.ssh_username, password=self.ssh_password)

            return ssh_client
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Error: {e}")
            return None

    # test SSH connection
    def ssh_connect_test(self):
        status = False
        try:
            command =f"export PGPASSWORD={self.db_password}"
            stdin, stdout, stderr = self.ssh_connect().exec_command(command)

            result = stdout.read().decode("utf-8")
            stderr_data = stderr.read().decode("utf-8")

            if len(stderr_data.strip())>0:
                status = False
                raise Exception("SSH connection is failed!")
            else: 
                print("SSH Connection is successful!")
                self.log_info("SSH Connection is successful!")
                status = True

            return status
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Error: {e}")
            return False

    # Set Environment Variables
    def set_env_variable(self, variable_name, variable_value):
        try:
            # Command to add the environment variable to .bashrc
            command1 = f"""echo "export {variable_name}='{variable_value}'" >> ~/.bashrc"""
            stdin1, stdout1, stderr1 = self.ssh_connect().exec_command(command1)

            command2 = f"source ~/.bashrc"
            stdin2, stdout2, stderr2 = self.ssh_connect().exec_command(command2)

            command3 = f"echo ${variable_name}"
            stdin3, stdout3, stderr3 = self.ssh_connect().exec_command(command3)

            result = stdout3.read().decode("utf-8")
            stderr_data = stderr3.read().decode("utf-8")

            # print(f"result = {result}")
            # print(f"stderr_data = {stderr_data}")

            return True
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Error: {e}")
            return False
        
    # Execute shell command
    def shell_command(self, command):

        try:
            status = False

            print(f"Command executing: {command}")
            self.log_info(f"Command executing: {command}")

            stdin, stdout, stderr = self.ssh_connect().exec_command(command)

            result = stdout.read().decode("utf-8")
            stderr_data = stderr.read().decode("utf-8")

            if len(stderr_data.strip())>0:
                self.log_error(stderr_data)
                self.log_error(f"Failed: {command}")
                raise Exception("Failed to execute command")
            
            # Print the output of the command
            print(f"Command executed: {command}")
            self.log_info(f"Command executed: {command}")

            status = True

        except Exception as e:
            print(f"Error occured: {command}")
            print(f"Error: {e}")
            self.log_error(f"Error occured: {command}")
            self.log_error(f"Error: {e}")
            status = False
        finally:
            return status

    # Comment out PostgreSQL paramters
    def comment_out_paramters(self, file_name, param_to_comment_out, commented_param = '# '):

        try:
            status = False

            # generate command
            data_dir = get_variables().DB_DATA_DIR
            file_path= data_dir +"/"+file_name
            #command = f"sed -i 's/^{param_to_comment_out}/{commented_param}/' {file_path}"
            command = f"sudo sed -i 's/^{param_to_comment_out}/{commented_param}{param_to_comment_out}/' {file_path}"

            print(f"Command executing: {command}")
            self.log_info(f"Command executing: {command}")

            stdin, stdout, stderr = self.ssh_connect().exec_command(command)

            result = stdout.read().decode("utf-8")
            stderr_data = stderr.read().decode("utf-8")

            if len(stderr_data.strip())>0:
                self.log_error(stderr_data)
                self.log_error(f"Failed: {command}")
                raise Exception("Failed to execute command")
            
            # Print the output of the command
            print(f"Command executed: {command}")
            self.log_info(f"Command executed: {command}")

            status = True

        except Exception as e:
            print(f"Error occured: {command}")
            print(f"Error: {e}")
            self.log_error(f"Error occured: {command}")
            self.log_error(f"Error: {e}")
            status = False
        finally:
            return status
    # Execute shell command
    def shell_command_without_log(self, command, remarks):

        try:
            status = False

            print(f"Command executing: {remarks}")
            self.log_info(f"Command executing: {remarks}")

            stdin, stdout, stderr = self.ssh_connect().exec_command(command)

            result = stdout.read().decode("utf-8")
            stderr_data = stderr.read().decode("utf-8")

            if len(stderr_data.strip())>0:
                self.log_error(stderr_data)
                self.log_error(f"Failed: {remarks}")
                raise Exception("Failed to execute command")
            
            # Print the output of the command
            print(f"Command executed: {remarks}")
            self.log_info(f"Command executed: {remarks}")

            status = True

        except Exception as e:
            print(f"Error occured: {remarks}")
            print(f"Error: {e}")
            self.log_error(f"Error occured: {remarks}")
            self.log_error(f"Error: {e}")
            status = False
        finally:
            return status
        
    # Reset database password
    def reset_db_password(self):
        try:
            DUMP_BINARY = get_variables().DUMP_BINARY
            DB_NAME = get_variables().DB_NAME
            DB_PORT = get_variables().DB_PORT
            DB_USER = get_variables().DB_USER
            DB_PASS = get_variables().DB_USER_PASS
            LIVE_DB_PASS = get_variables().SECRET_VALUE

            #command = f""" sudo -i && su - {DB_USER} && who && {DUMP_BINARY}/psql -p {DB_PORT} -d {DB_NAME} -U {DB_USER} -c "ALTER USER {DB_USER} WITH PASSWORD '{DB_PASS}';" && echo "Successfull"; """
            
            command = f"""sudo PGPASSWORD='{LIVE_DB_PASS}' {DUMP_BINARY}/psql -p {DB_PORT} -d {DB_NAME} -U {DB_USER} -c "ALTER USER {DB_USER} WITH PASSWORD '{DB_PASS}';" && echo "Successfull"; """
             
            status = self.shell_command_without_log(command=command, remarks="DB User's Password Reset")
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Error: {e}")
            status = False
        finally:
            return status

    # Replace File contents
    def replace_file_contents(self, remote_file, contents):
        try:
            command = f"""sudo printf "{contents}" > {remote_file}"""
             
            status = self.shell_command(command=command)
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Error: {e}")
            status = False
        finally:
            return status

    # Replace pg_hba.conf file
    def update_pg_hba_config_file(self):
        try:
            hba_file = f"{get_variables().DB_DATA_DIR}/pg_hba.conf"
            hba_file_tmp = f"/tmp/pg_hba.conf"
            new_hba_file = get_variables().PG_HBA_FILE
            #file_contents=""

            # Connect to the remote server

            # Use SFTP to upload the file
            sftp = self.ssh_connect().open_sftp()
            sftp.put(f'{new_hba_file}', f'{hba_file_tmp}')
            
            command = f"sudo rm -rf {hba_file}"
            status = self.shell_command(command=command)

            command = f"sudo cp -f {hba_file_tmp} {hba_file}"
            status = self.shell_command(command=command)

            # with open('conf/pg_hba.conf', 'r') as file:
            #     # Read the entire file into a string
            #     file_contents = file.read()
             
            # status = self.replace_file_contents(remote_file=hba_file, contents=file_contents)
        
            # Change ownership of the file
            command = f"sudo chown enterprisedb:enterprisedb {hba_file}"
            status = self.shell_command(command=command)

        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Error: {e}")
            status = False
        finally:
            return status
        
    # Find HDD device partision existance
    # lsblk --noheadings --output NAME | grep sdc
    # it return the partition number
    def get_hdd_device_partition(self, device_name):
        try:
            status = None
            command = f"sudo lsblk --noheadings --output NAME | grep {device_name}"
            
            # Execute a command on the remote server
            # command = "sudo systemctl status edb-as-11"
            stdin, stdout, stderr = self.ssh_connect().exec_command(command)
            
            result = stdout.read().decode("utf-8")
            stderr_data = stderr.read().decode("utf-8")

            if len(stderr_data.strip())>0:
                self.log_error(f"Failed: {command}")
                raise Exception("Failed to read lsblk!")
            
            pattern = r'\b\w+\d\b'
            # Use re.findall to extract all matching words from the text
            matches = re.findall(pattern, result)
            # Print the matched words
            for match in matches:
                if len(match)>3:
                    status=match
                else:
                    status=None
            
            if status==None:
                status=device_name
        
            print(f"Command executed: {command}")
            self.log_info(f"Command executed: {command}")

        except Exception as e:
            print(f"Error occured: {command}")
            print(f"Error: {e}")
            self.log_error(f"Error occured: {command}")
            self.log_error(f"Error: {e}")
            status = None
        finally:
            return status
        
    # Get HDD Device for boot disk
    def get_hdd_device(self, boot_disk):
        try:
            status = None
            command = f"sudo lsblk -d -o NAME | grep -v '{boot_disk}' |  grep -v 'NAME'"
            
            # Execute a command on the remote server
            # command = "sudo systemctl status edb-as-11"
            stdin, stdout, stderr = self.ssh_connect().exec_command(command)
            
            result = stdout.read().decode("utf-8")
            stderr_data = stderr.read().decode("utf-8")

            if len(stderr_data.strip())>0:
                self.log_error(f"Failed: {command}")
                raise Exception("Failed to read hdd!")
            
            # Print the output of the command
            #print(stdout.read().decode("utf-8"))
            print(f"Command executed: {command}")
            self.log_info(f"Command executed: {command}")

            
            #print(result)

            ls = result.split("\n")
            sorted_ls = sorted(ls, reverse=True)
            print(ls)
            print(sorted_ls)

            status = sorted_ls[0]

            # get partition
            status= self.get_hdd_device_partition(status)

        except Exception as e:
            print(f"Error occured: {command}")
            print(f"Error: {e}")
            self.log_error(f"Error occured: {command}")
            self.log_error(f"Error: {e}")
            status = None
        finally:
            return status

    # Check Directory Existance
    def check_directory_existance(self, directory_full_path):
        try:
            
            # Check Existance of Directory
            command = f"test -d {directory_full_path} && echo 'True' || echo 'False'"

            # Execute   
            stdin, stdout, stderr = self.ssh_connect().exec_command(command)

            # Get result
            result = bool(stdout.read().decode().strip())
            
            if stderr.read():
                self.log_error(f"Failed: {command}")
                return False

            print(f"Command executed: {command}")
            self.log_info(f"Command executed: {command}")

            return result
        
        except Exception as e:
            self.log_error(f"Error: {e}")
            print(f"Error: {e}")
            return False

    # Check Disk Mounted or Not
    def check_disk_mount_status(self, device_name):
        try:
            
            # Check Existance of Directory
            command = f"findmnt -rno SOURCE,TARGET | grep {device_name}"

            # Execute   
            stdin, stdout, stderr = self.ssh_connect().exec_command(command)

            # Get result
            result = bool(stdout.read().decode().strip())
            
            if stderr.read():
                self.log_error(f"Failed: {command}")
                return False

            print(f"Command executed: {command}")
            self.log_info(f"Command executed: {command}")

            print (f"mount status of device {device_name} = {result}")
            self.log_info (f"mount status of device {device_name} = {result}")

            return result
        
        except Exception as e:
            self.log_error(f"Error: {e}")
            print(f"Error: {e}")
            return False
        
    # Check when the Service running or not
    def check_service_status(self, service_name):
        try:
            
            # Check Existance of Directory
            command = f"sudo systemctl is-active {service_name}"

            # Execute   
            stdin, stdout, stderr = self.ssh_connect().exec_command(command)

            # Get result
            result = stdout.read().decode().strip()
            
            if stderr.read():
                self.log_error(f"Failed: {command}")
                print(f"Failed: {command}")
                return False

            print(f"Command executed: {command}")
            self.log_info(f"Command executed: {command}")

            return result == 'active'
        
        except Exception as e:
            self.log_error(f"Error: {e}")
            print(f"Error: {e}")
            return False
                
    # Mount a Disk
    def mount_disk(self, device_name, mount_point, is_new_disk=False):
        try:
            
            # Check Disk already mounted or not
            mount_status = self.check_disk_mount_status(device_name=device_name)
            if (mount_status==True):
                print(f"Device {device_name} already mounted to {mount_point}")
                self.log_info(f"Device {device_name} already mounted to {mount_point}")
                return True
            
            # Check directory Existance
            if (is_new_disk==True):
                # Format disk
                #command = f"sudo mkfs.ext4 -m 0 -F -E lazy_itable_init=0,lazy_journal_init=0,discard /dev/{device_name}"
                command = f"sudo mkfs.ext4 /dev/{device_name}"
                format_status = self.shell_command(command)

                command = f"sudo mkdir -p {mount_point}"
                create_target_dir = self.shell_command(command)
                
                # directory = self.check_directory_existance(directory_full_path=mount_point)
                # if (directory==False):
                #     command = f"sudo mkdir -p {mount_point}"
                #     create_target_dir = self.shell_command(command)
                #     if (create_target_dir==False):
                #         raise Exception(f"Unable to create new directory {mount_point}")
     

            # Mount the disk 
            command = f"sudo mount -o discard,defaults /dev/{device_name} {mount_point}"
            mount_status = self.shell_command(command)

            #stdin, stdout, stderr = self.ssh_connect().exec_command(command)

            # if stderr.read():
            #     self.log_error(f"Failed: {command}")
            #     print(f"Failed: {command}")
            #     return False

            # print(f"Command executed: {command}")
            # self.log_info(f"Command executed: {command}")

            if (is_new_disk==True):
                command = f"sudo chmod a+w {mount_point}"
                permission_status = self.shell_command(command)

            return True
        except Exception as e:
            self.log_error(f"Error: {e}")
            print(f"Error: {e}")
            return False

    def mount_disk_xfs(self, device_name, mount_point, is_new_disk=False):
        try:
            
            # Check Disk already mounted or not
            mount_status = self.check_disk_mount_status(device_name=device_name)
            if (mount_status==True):
                print(f"Device {device_name} already mounted to {mount_point}")
                self.log_info(f"Device {device_name} already mounted to {mount_point}")
                return True
            
            # Check directory Existance
            if (is_new_disk==True):
                # Format disk
                #command = f"sudo mkfs.ext4 -m 0 -F -E lazy_itable_init=0,lazy_journal_init=0,discard /dev/{device_name}"
                command = f"sudo mkfs.xfs /dev/{device_name}"
                format_status = self.shell_command(command)

                command = f"sudo mkdir -p {mount_point}"
                create_target_dir = self.shell_command(command)
                
                # directory = self.check_directory_existance(directory_full_path=mount_point)
                # if (directory==False):
                #     command = f"sudo mkdir -p {mount_point}"
                #     create_target_dir = self.shell_command(command)
                #     if (create_target_dir==False):
                #         raise Exception(f"Unable to create new directory {mount_point}")
     

            # Mount the disk 
            command = f"sudo mount -o discard,defaults /dev/{device_name} {mount_point}"
            mount_status = self.shell_command(command)

            #stdin, stdout, stderr = self.ssh_connect().exec_command(command)

            # if stderr.read():
            #     self.log_error(f"Failed: {command}")
            #     print(f"Failed: {command}")
            #     return False

            # print(f"Command executed: {command}")
            # self.log_info(f"Command executed: {command}")

            if (is_new_disk==True):
                command = f"sudo chmod a+w {mount_point}"
                permission_status = self.shell_command(command)

            return True
        except Exception as e:
            self.log_error(f"Error: {e}")
            print(f"Error: {e}")
            return False    
    # Get UUID for a HDD
    def get_hdd_device_uuid(self, boot_disk):
        try:
            status = None
            command = f"sudo blkid -d | grep -v '{boot_disk}' |  grep -v 'NAME'"
            
            stdin, stdout, stderr = self.ssh_connect().exec_command(command)

            # Print the output of the command
            print(f"Command executed: {command}")
            self.log_info(f"Command executed: {command}")

            result = stdout.read().decode("utf-8")

            #print(result)

            ls = result.split(" ")

            print(f"ls = {ls}")
            self.log_info(f"ls = {ls}")

            uuid_lst = ls[1].split("=")

            print(f"uuid_lst = {uuid_lst}")
            self.log_info(f"uuid_lst = {uuid_lst}")

            uuid = uuid_lst[1].replace("\"","")

            print(f"uuid = {uuid}")
            self.log_info(f"uuid = {uuid}")

            status = uuid

            #print(status)

        except Exception as e:
            print(f"Error occured: {command}")
            print(f"Error: {e}")
            self.log_error(f"Error occured: {command}")
            self.log_error(f"Error: {e}")
            status = None
            
        finally:
            return status

    # Sync files and folder between directories 
    def sync_data_between_folders(self, source_directory, destination_directory):
        try:
            
            # Check directory Existance
            s_directory = self.check_directory_existance(directory_full_path=source_directory)
            if (s_directory==False):
                print(f"Directory {source_directory} does not exists.")
                self.log_error(f"Directory {source_directory} does not exists.")
                raise Exception(f"Directory {source_directory} does not exists.")

            d_directory = self.check_directory_existance(directory_full_path=destination_directory)
            if (d_directory==False):
                print(f"Directory {destination_directory} does not exists.")
                self.log_error(f"Directory {destination_directory} does not exists.")
                raise Exception(f"Directory {destination_directory} does not exists.")
            
            command = f"sudo rsync -avh --progress --delete {source_directory}/ {destination_directory}/"

            # Mount the disk    
            result = self.shell_command(command=command)

            
            # Compare File Size of both system
            source_size = self.calculate_used_storage_gb(mount_point=source_directory)
            target_size = self.calculate_used_storage_gb(mount_point=destination_directory)
            print(f"soruce data size ({source_directory}) = {source_size}GB, Destnation Data Size ({destination_directory}) = {target_size}GB")
            self.log_info(f"soruce data size ({source_directory}) = {source_size}GB, Destnation Data Size ({destination_directory}) = {target_size}GB")
            if (source_size==target_size):
                result = True
            else: 
                result=False

            return result
        except Exception as e:
            self.log_error(f"Error: {e}")
            print(f"Error: {e}")
            return False
        
    # Count directories in a directory
    def count_remote_directories(self, remote_directory):
        try:
            # Execute the command to count directories remotely
            command = f"find {remote_directory} -type d | wc -l"
            stdin, stdout, stderr = self.ssh_connect().exec_command(command)

            # Get the count of directories from the command output
            directory_count = int(stdout.read().decode().strip())

            return directory_count

        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Error: {e}")
            return None

    # Calculate Disk used space
    def calculate_used_storage_gb(self, mount_point):
        try:

            # Run the 'df' command to get disk usage information
            #command = f"df -h {mount_point}"
            command = f"sudo du -sh {mount_point}"
            stdin, stdout, stderr = self.ssh_connect().exec_command(command)
            
            # Read the output of the 'df' command
            df_output = stdout.read().decode('utf-8')
            #print(df_output)

            # Parse the output to extract the used storage space in GB
            used_space_gb = None
            lines = df_output.split(" ")
            #print(f"lines={lines}")
            if len(lines) >= 1:
                # Extract the second line (the one containing disk usage info)
                disk_usage_info = lines[0]
                
                #print(disk_usage_info)

                disk_usage_info = disk_usage_info.split("\t")
                disk_usage_info = disk_usage_info[0]
                disk_usage_info = disk_usage_info.strip()

                print(f"disk_usage_info={disk_usage_info}")
                #disk_size_data = [item for item in disk_df_info if item!='']
                #print(disk_size_data)

                used_space_numeric = float(disk_usage_info[:-1])
                used_space_char = disk_usage_info[-1]
                #print(f"used_space_numeric={used_space_numeric}")
                #print(f"used_space_char={used_space_char}")

                if (used_space_char=='G'):
                    used_space_gb = used_space_numeric
                elif (used_space_char=='T'):
                    used_space_gb = round(used_space_numeric * 1024, 1)
                elif (used_space_char=='M'):
                    used_space_gb = round(used_space_numeric/1024, 1)
                elif (used_space_char=='K'):
                    used_space_gb = round(used_space_numeric/(1024 * 1024), 1)
                else:
                    used_space_gb = used_space_numeric

                # Add buffer

            return used_space_gb
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Error: {e}")
            return None

    # Calculate file size
    def calculate_file_size_bytes(self, file_path):
        try:
            command = f"sudo du -b {file_path}"
            #print(f"command={command}")
            stdin, stdout, stderr = self.ssh_connect().exec_command(command)
            
            # Read the output of the 'df' command
            df_output = stdout.read().decode('utf-8')
            #print(f"df_output={df_output}")

            # Parse the output to extract the used storage space
            lines = df_output.split("\t")
            #print(f"lines={lines}")
            if len(lines) > 0:
                # Extract the first line
                used_space_numeric = lines[0]

                print(f"File {file_path} Size={used_space_numeric} bytes")
                self.log_info(f"File {file_path} Size={used_space_numeric} bytes")

            return int(used_space_numeric)
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Error: {e}")
            return None
        
    # Calculate file size in KB
    def calculate_file_size_kb(self, file_path):
        try:
            size = self.calculate_file_size_bytes(file_path=file_path)
            return round((size/1024),1)
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Error: {e}")
            return None
    
    # Calculate file size in MB
    def calculate_file_size_mb(self, file_path):
        try:
            size = self.calculate_file_size_bytes(file_path=file_path)
            return round((size/(1024*1024)),1)
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Error: {e}")
            return None

    # Calculate file size in GB
    def calculate_file_size_gb(self, file_path):
        try:
            size = self.calculate_file_size_bytes(file_path=file_path)
            return round((size/(1024*1024*1024)),1)
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Error: {e}")
            return None

    # Calculate file size in TB
    def calculate_file_size_tb(self, file_path):
        try:
            size = self.calculate_file_size_bytes(file_path=file_path)
            return round((size/(1024*1024*1024*1024)),1)
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Error: {e}")
            return None
                
    # Calculate RAM size
    def get_ram_size_gb(self):
        try:

            # Run the 'df' command to get disk usage information
            command = "free -b | awk '/^Mem:/{print $2}'"
            stdin, stdout, stderr = self.ssh_connect().exec_command(command)
            
            # Read the RAM size from the command output
            ram_size_bytes = int(stdout.read().strip())
            ram_size_gb = int(round(ram_size_bytes / (1024 ** 3),0))

            return ram_size_gb
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Error: {e}")
            return None
        
    # Return Server's total CPU core
    def get_cpu_count(self):
        try:

            # Run the 'df' command to get disk usage information
            command = "nproc"
            stdin, stdout, stderr = self.ssh_connect().exec_command(command)
            
            # Read the output of the command
            num_cores = int(stdout.read().strip())

            return num_cores
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Error: {e}")
            return None
                
    # Mount Google cloud Bucket
    def upload_remote_file_google_cloud_bucket(self, bucket_name, dest_location, src_file):

        try:
            
            service_account_key = get_variables().GOOGLE_SERVICE_ACCOUNT_FILE
            # Step 1: Upload the service_account_key to the database server
            ftp = self.ssh_connect().open_sftp()
            ftp.put(service_account_key, 'service_account_key.json')
            ftp.close()
            print(f"Uploaded file: {service_account_key}")
            self.log_info(f"Uploaded file: {service_account_key}")

            # Step 2: Activate gcloud authentication
            shell_cmd = "sudo gcloud auth activate-service-account --key-file=service_account_key.json"
            output_shell = self.shell_command(shell_cmd)
            print(f"gclloud authenication = {output_shell}")
            self.log_info(f"gclloud authenication = {output_shell}")

            # Step 3: Upload file
            trg_directory = f"{bucket_name}/{dest_location}"
            shell_cmd = f"""sudo gsutil cp "{src_file}" "gs://{trg_directory}" """
            output_shell = self.shell_command(shell_cmd)

            # Read File Size
            local_file_size = self.calculate_file_size_bytes(file_path=src_file)
            file_name = os.path.basename(src_file)
            remote_file=f"{dest_location}/{file_name}"
            remote_file_size = self.get_gcs_file_size_by_bytes(file_name=remote_file)

            print(f"Source File - {src_file}: {local_file_size} bytes")
            print(f"Bucket File - {remote_file}: {remote_file_size} bytes")

            self.log_info(f"Source File - {src_file} size: {local_file_size} bytes")
            self.log_info(f"Bucket File - {remote_file} size: {remote_file_size} bytes")

            if (local_file_size==remote_file_size):
                status = True
            else:
                status = False

        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Error: {e}")
            status = False
        finally:
            return status


    # Mount Google cloud Bucket
    def mount_google_cloud_bucket(self, service_account_key, bucket_name, mount_folder):

        status = False

        shell_script=f"""
    #!/bin/bash

    # Replace these variables with your specific values
    BUCKET_NAME="{bucket_name}"
    MOUNT_POINT="{mount_folder}"

    # Install gcsfuse (if not already installed)
    if ! command -v gcsfuse &> /dev/null; then
        echo "Installing gcsfuse..."
        sudo tee /etc/yum.repos.d/gcsfuse.repo <<EOF
    [gcsfuse]
    name=gcsfuse (packages.cloud.google.com)
    baseurl=https://packages.cloud.google.com/yum/repos/gcsfuse-el7-x86_64
    enabled=1
    gpgcheck=1
    repo_gpgcheck=1
    gpgkey=https://packages.cloud.google.com/yum/doc/yum-key.gpg
        https://packages.cloud.google.com/yum/doc/rpm-package-key.gpg
    EOF
        sudo yum -y install gcsfuse
    fi

    # Create a mount point directory if it doesn't exist
    if [ ! -d "$MOUNT_POINT" ]; then
        echo "Creating mount point directory: $MOUNT_POINT"
        sudo mkdir -p $MOUNT_POINT
    fi

    # Mount the GCS bucket
    echo "Mounting GCS bucket '$BUCKET_NAME' to '$MOUNT_POINT'..."
    gcsfuse $BUCKET_NAME $MOUNT_POINT

    # echo "OUTPUT=$?"

    # Check if the mount was successful
    if [ $? -eq 0 ]; then
        echo "GCS bucket mounted successfully."
    else
        echo "Failed to mount GCS bucket."
    fi
    """
        try:
            
            # Step 3: Write the shell script to a temporary file
            temp_script_path = r"scripts\shell\bucket_mount.sh"
            with open(temp_script_path, 'w') as script_file:
                    script_file.write(shell_script)

            # Step 4: Upload the shell script to the database server
            ftp = self.ssh_connect().open_sftp()
            ftp.put(temp_script_path, 'bucket_mount.sh')
            print(f"Uploaded file: {temp_script_path}")
            self.log_info(f"Uploaded file: {temp_script_path}")
            ftp.put(service_account_key, 'service_account_key.json')
            ftp.close()
            print(f"Uploaded file: {service_account_key}")
            self.log_info(f"Uploaded file: {service_account_key}")

            # Activate gcloud authentication
            shell_cmd = "gcloud auth activate-service-account --key-file=service_account_key.json"
            output_shell = self.shell_command(shell_cmd)
            print(f"gclloud authenication = {output_shell}")
            self.log_info(f"gclloud authenication = {output_shell}")

            # Prepare Mount Point Directory
            shell_cmd = f"mkdir -p {mount_folder}"
            output_shell = self.shell_command(shell_cmd)
            print(f"Bucket mount point = {output_shell}")
            self.log_info(f"Bucket mount point = {output_shell}")

            # Step 5: Make the script executable and execute it on the database server
            shell_cmd = "chmod +x bucket_mount.sh"
            output_shell = self.shell_command(shell_cmd)
            #print(output_shell)

            shell_cmd = "sudo yum install dos2unix -y"
            output_shell = self.shell_command(shell_cmd)
            
            shell_cmd = "dos2unix bucket_mount.sh"
            output_shell = self.shell_command(shell_cmd)

            shell_cmd = "sh bucket_mount.sh"
            output_shell = self.shell_command(shell_cmd)
            print(f"GCS mounted : {output_shell}")
            self.log_info(f"GCS mounted : {output_shell}")

            # Verify mount
            dir_cnt = self.count_remote_directories(mount_folder)
            if (dir_cnt>0):
                status = True
                print("GCS Mount is Successful.")
                self.log_info("GCS Mount is Successful.")
            else:
                status = False
                print("GCS Mount is Failed.")
                self.log_error("GCS Mount is Failed.")

        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Error: {e}")
            status = False
        finally:
            return status

    # UnMount Google cloud Bucket
    def unmount_google_cloud_bucket(self, mount_folder):
        status = False
        try:
            shell_cmd = f"fusermount -u {mount_folder}/"
            output_shell = self.shell_command(shell_cmd)

            status = True

        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Error: {e}")
            status = False
        finally:
            return status

    # Take database Dump Backup
    def take_database_schema_dump_backup(self, schemaname, isSchemaOnly=True):

        DUMP_BINARY=f"{get_variables().DUMP_BINARY}/pg_dump"
        #DB_SERVER = self.ssh_host
        DB_SERVER = "localhost"
        DB_NAME = get_variables().DB_NAME
        SCHEMA_NAME = schemaname
        DB_PORT = get_variables().DB_PORT
        DB_USER = get_variables().DB_USER
        DB_USER_PASS = get_variables().DB_USER_PASS
        BACKUP_FILE=""
        BACKUP_PATH = get_variables().DATA_DISK_MOUNT_POINT
        LOG_FILE =f"db_dump.log"

        result = None

        now = datetime.now()
        dt_string = now.strftime("%Y%m%d%H%M%S")

        if (isSchemaOnly==True):
            OPTION="--schema-only"
            BACKUP_FILE=f"{BACKUP_PATH}/{DB_NAME}_{SCHEMA_NAME}_schemaonly_{dt_string}.dump"
        else:
            OPTION=""
            BACKUP_FILE=f"{BACKUP_PATH}/{DB_NAME}_{SCHEMA_NAME}_{dt_string}.dump"

        command = f"""
sudo PGPASSWORD='{self.db_password}' {DUMP_BINARY} --host {DB_SERVER} --port {DB_PORT} --username {DB_USER} --lock-wait-timeout=600 --no-sync --schema {SCHEMA_NAME} {OPTION} -F c -f {BACKUP_FILE} {DB_NAME}
"""     
        command_print = f"""
sudo  {DUMP_BINARY} --host {DB_SERVER} --port {DB_PORT} --username {DB_USER} --lock-wait-timeout=600 --no-sync --schema {SCHEMA_NAME} {OPTION} -F c -f {BACKUP_FILE} {DB_NAME}
"""

        file_size = 0.0

        try:
            
            backup_file =""
            
            # SET environment variables
            #env_status = self.set_env_variable(variable_name="PGPASSWORD", variable_value=self.db_password)
 

            stdin, stdout, stderr = self.ssh_connect().exec_command(command)

            result = stdout.read().decode("utf-8")
            stderr_data = stderr.read().decode("utf-8")

            if len(result.strip())>0:
                print(f"{result}")
                self.log_info(f"{result}")

            if len(stderr_data.strip())>0:
                print(f"Error: {stderr_data}")
                self.log_error(f"Error: {stderr_data}")

            if len(stderr_data.strip())>0:
                status= False
            else:
                status= True
                backup_file = BACKUP_FILE
                if (status):
                    self.log_info(f"{SCHEMA_NAME} schema is dumped successfully.")
                    print(f"{SCHEMA_NAME} schema is dumped successfully.")

                    file_size = self.calculate_file_size_kb(file_path=backup_file)

                    #print(f"file_size={file_size}")
                    # Check file size . it should minimum > 1MB = 1024 kb
                    if (file_size)<1:
                        backup_file = None
                        raise Exception("Dump backup is failed!")
                    
                    file_size_unit = ""
                    # print file size

                    if (file_size>(1024*1024*1024)):
                        file_size = round(file_size/(1024*1024*1024)) # TB
                        file_size_unit = "TB"
                    elif (file_size>(1024*1024)):
                        file_size = round(file_size/(1024*1024)) # GB
                        file_size_unit = "GB"
                    elif (file_size>1024):
                        file_size = round(file_size/1024) # MB
                        file_size_unit = "MB"
                    else:
                        file_size_unit = "KB"
                
                    print(f"Backup dump size of {SCHEMA_NAME} schema: {file_size} {file_size_unit}")
                    self.log_info(f"Backup dump size of {SCHEMA_NAME} schema: {file_size} {file_size_unit}")

            return backup_file
        
        except Exception as e:
            print(f"Error occured: {command_print}")
            print(f"Error: {e}")
            self.log_error(f"Error occured: {command_print}")
            self.log_error(f"Error: {e}")
            return None

    # Restore database Dump Backup
    def restore_database_schema_dump_backup(self, db_backup_file):

        RESTORE_BINARY=f"{get_variables().DUMP_BINARY}/pg_restore"
        DB_SERVER = "localhost"
        DB_NAME = get_variables().DB_NAME
        DB_PORT = get_variables().DB_PORT
        DB_USER = get_variables().DB_USER
        BACKUP_FILE=db_backup_file
        BACKUP_PATH = get_variables().DATA_DISK_MOUNT_POINT
        LOG_FILE =f"db_restore.log"
        status = False

        command = f"""
sudo PGPASSWORD='{self.db_password}' {RESTORE_BINARY} --host {DB_SERVER} --port {DB_PORT} --username {DB_USER} -d {DB_NAME} {BACKUP_FILE}
"""
        command_print = f"""
        sudo {RESTORE_BINARY} --host {DB_SERVER} --port {DB_PORT} --username {DB_USER} -d {DB_NAME} {BACKUP_FILE}
        """
        try:
            
            stdin, stdout, stderr = self.ssh_connect().exec_command(command)

            result = stdout.read().decode("utf-8")
            stderr_data = stderr.read().decode("utf-8")

            if len(stderr_data.strip())>0:
                status = False
            else:
                status = True

            if (status):
                self.log_info(f"{BACKUP_FILE} backup is restored successfully.")
                print(f"{BACKUP_FILE} backup is restored successfully.")

            return status
        
        except Exception as e:
            print(f"Error occured: {command_print}")
            print(f"Error: {e}")
            self.log_error(f"Error occured: {command_print}")
            self.log_error(f"Error: {e}")
            return False  

    # Execute procedure and function, but don't return anything
    def execute_stored_procedure_function(self, sql_exec_command):

        RESTORE_BINARY=f"{get_variables().DUMP_BINARY}/psql"
        DB_SERVER = "localhost"
        DB_NAME = get_variables().DB_NAME
        DB_PORT = get_variables().DB_PORT
        DB_USER = get_variables().DB_USER
        #DB_USER_PASS = get_variables().DB_USER_PASS
        LOG_FILE="sql_proc_exec.log"

        command = f"""
sudo PGPASSWORD='{self.db_password}' {RESTORE_BINARY} --host {DB_SERVER} --port {DB_PORT} --username {DB_USER} -d {DB_NAME} -c "{sql_exec_command}" 2>> {LOG_FILE}
"""     
        command_print = f"""
sudo {RESTORE_BINARY} --host {DB_SERVER} --port {DB_PORT} --username {DB_USER} -d {DB_NAME} -c "{sql_exec_command}" 2>> {LOG_FILE}
"""
        try:

            #status = self.shell_command(command)

            stdin, stdout, stderr = self.ssh_connect().exec_command(command)
            
            output_msg = stdout.read().decode("utf-8")
            status = bool(output_msg)
            stderr_data = stderr.read().decode("utf-8")

            # write output msg
            if len(output_msg.strip())>0:
                self.log_info(f"output = {output_msg}")
                print(output_msg)

            # write error msg
            if len(stderr_data.strip())>0:
                self.log_error(f"Failed: {command_print}")
                self.log_error(stderr_data)
                print(stderr_data)
                raise Exception(f"Failed to execute {sql_exec_command}!")
            
            if (status):
                self.log_info(f"{sql_exec_command} is executed successfully.")
                print(f"{sql_exec_command} is executed successfully.")

            return status
        
        except Exception as e:
            print(f"Error occured: {command_print}")
            print(f"Error: {e}")
            self.log_error(f"Error occured: {command_print}")
            self.log_error(f"Error: {e}")
            return False  

     # Execute VACCUMDB command
    def execute_vaccumdb(self, dbname):

        RESTORE_BINARY=f"{get_variables().DUMP_BINARY}/vacuumdb"
        DB_SERVER = "localhost"
        #DB_NAME = get_variables().DB_NAME
        DB_NAME = dbname
        DB_PORT = get_variables().DB_PORT
        DB_USER = get_variables().DB_USER
        #DB_USER_PASS = get_variables().DB_USER_PASS
        LOG_FILE="vaccumdb.log"

        command = f"""
sudo PGPASSWORD='{self.db_password}' {RESTORE_BINARY} --host {DB_SERVER} --port {DB_PORT} --username {DB_USER} -d {DB_NAME} --analyze --verbose 2>> {LOG_FILE}
"""     
        command_print = f"""
sudo {RESTORE_BINARY} --host {DB_SERVER} --port {DB_PORT} --username {DB_USER} -d {DB_NAME} --analyze --verbose 2>> {LOG_FILE}
"""
        try:

            #status = self.shell_command(command)

            stdin, stdout, stderr = self.ssh_connect().exec_command(command)
            
            output_msg = stdout.read().decode("utf-8")
            status = bool(output_msg)
            stderr_data = stderr.read().decode("utf-8")

            # write output msg
            if len(output_msg.strip())>0:
                self.log_info(f"output = {output_msg}")
                print(output_msg)

            # write error msg
            if len(stderr_data.strip())>0:
                self.log_error(f"Failed: {command_print}")
                self.log_error(stderr_data)
                print(stderr_data)
                raise Exception(f"Failed to execute vaccumdb on {dbname}!")
            
            if (status):
                self.log_info(f"vaccumdb on {dbname} is executed successfully.")
                print(f"vaccumdb on {dbname} is executed successfully.")

            return status
        
        except Exception as e:
            print(f"Error occured: {command_print}")
            print(f"Error: {e}")
            self.log_error(f"Error occured: {command_print}")
            self.log_error(f"Error: {e}")
            return False 
           
# if __name__ == "__main__":

#     operation_log="logs\log_928c2183-c22e-4085-8658-2675bcf2085f_2023-10-31_12-39-06.log"
#     #Shell Executor
#     sh = ShellOperation(operation_log)
    
#     DB = sh.reset_db_password()

#     print(f"DB_PASS ={DB}")

#     HBA = sh.update_pg_hba_config_file()
    
#     print(f"PG_HBA ={HBA}")