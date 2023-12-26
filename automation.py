from execute_sql import *
from shell_execute import *
from vm_operation import *
from gcs_operation import *
from init_variables import get_variables
from notification import notification
from operationdb import operation_db, OperationMaster
import time
from timeit import default_timer as timer
from datetime import datetime

class Automation(Logger):
    def __init__(self, logfile, operation_id):
        super().__init__(logfile)
        self.operation_log=logfile
        self.operation_id = operation_id

        # Global Variables
        # self.vm_image_name=None
        # self.instance_name=None
        # self.ssh_login_status=None
        # self.db_login_status=None
        # self.public_schema_only_dump=None
        # self.archive_schema_only_dump=None
        # self.public_schema_dump=None
        # self.archive_schema_dump=None
        # self.data_disk_dev=None

        # Initialize instances
        # self.tracker = OperationTracker()
        # self.operation_id = None

        # Create a list to store Task objects
        self.task_list = []


    # Doing automation tasks
    def start_jobs(self):
        try:
            # Generate Operation ID
            # self.operation_id = self.tracker.generate_operation_id()
            # self.tracker.start_operation(self.operation_id)

            # # Generate Log File
            # log_directory = get_variables().LOG_DIRECTORY
            # operation_log=self.tracker.generate_log_file(log_directory=log_directory, operation_id=self.operation_id)
            
            operation_log = self.operation_log

            # Google cloud compute Instance
            gcp_compute_instance = VMoperation(operation_log)
            # if gcp_compute_instance.setup_google_credential()==None:
            #     print("Google credentials setup is failed!")
            #     self.log_error("Google credentials setup is failed!")
            #     raise Exception("Google credentials setup is failed!")
            
            # Google Cloud storage Instance
            #gcp_storage_instance = GCSoperation()

            # Shell Executor
            sh = ShellOperation(operation_log)

            # Defind SQL execution Instance
            sql_instance = DatabaseExecutor(operation_log)
            
            # Global Variables
            vm_image_name=None
            instance_name=None
            ssh_login_status=None
            db_login_status=None
            public_schema_only_dump=None
            archive_schema_only_dump=None
            public_schema_dump=None
            archive_schema_dump=None
            data_disk_dev=None
            data_disk_snapshoot_name=None

            db_service_name=get_variables().DB_SERVICE_NAME
            source_instance_name=get_variables().SOURCE_VM_NAME
            source_instance_ip=get_variables().SOURCE_VM_IP
            
            # Notification Instance
            notification_log_file = get_variables().NOTIFICATION_LOG
            notification_instance = notification(logfile=notification_log_file)

            # Execute Tasks , Log each Tasks and Sync To Cloud Storage
            # Get All task List
            self.task_list = sql_instance.get_task_list()
            total_passed_tasks = 0
            grand_total_seconds = 0

            # load operation info into database
            operationMasterObj = OperationMaster(operation_id=self.operation_id,
                                                 operation_log=self.operation_log,
                                                 start_datetime=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                                 end_datetime=None,
                                                 operation_status="In Progress",
                                                 total_duration=None,
                                                 source_database_vm_name=source_instance_name,
                                                 source_database_vm_ip=source_instance_ip,
                                                 total_passed_tasks=0,
                                                 total_tasks=len(self.task_list),
                                                 output_data_disk_snapshot=None,
                                                 output_dump_file=None
                                                 )
            
            operation_db_instance = operation_db(operation_log=operation_log, 
                                                 operation_master=operationMasterObj,
                                                 task_lst=self.task_list
                                                 )
            load_status = operation_db_instance.setup_operation_database()

            # Glabal task 
            current_task = None

            if (load_status!=True):
                raise Exception("Unable to initilize operational databaes!")

            task_indx = -1
            for task in operation_db_instance.operation_detail_lst:
                task_indx = task_indx + 1
                task_id = task.task_id
                task_name = task.task_name
                task_description = task.task_description

                # Update Task Status
                task.task_start_datetime=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                task.task_status = "In Progress"
                task_status = task.task_status

                # Update Task into database
                upd_task_status = operation_db_instance.update_operation_detail(OperationDetail=task)

                # Timer
                start = timer()

                # Log Tasks
                print("=====================================================================")
                print(f"Task # {task_id}, Task Name : {task_name}, Status : {task_status}")
                self.log_info("===============================================================")
                self.log_info(f"Task # {task_id}, Task Name : {task_name}, Status : {task_status}")
                self.log_info(task_description)

                if (task_name=="Connect_Google_Cloud"): # 1
                    if (gcp_compute_instance.connect()!=None):
                        total_passed_tasks = total_passed_tasks + 1
                        task.task_status="Completed"
                        self.log_info("GCP connection is successful.")
                    else:
                        task.task_status="Failed"
                        current_task = task
                        self.log_error("GCP connection is failed !!!")
                        task.remarks="GCP connection is failed !!!"
                        break
                        
                elif (task_name=="Connect_Google_Storage"): # 2
                    try:
                        total_passed_tasks = total_passed_tasks + 1
                        task.task_status="Completed"
                    except Exception as e:
                        task.task_status="Failed"
                        current_task = task
                        task.remarks=str(e)
                        self.log_error("Google cloud storage connection is failed !!!")
                        break

                elif (task_name=="Create_Machine_Image"): # 3
                    vm_image_name=gcp_compute_instance.create_machine_snapshot(instance_name=source_instance_name)
                    time.sleep(30)

                    if vm_image_name ==None:
                        task.task_status="Failed"
                        current_task = task
                        self.log_error(f"Unable to create VM image of {source_instance_name}.")
                        print(f"Unable to create VM image of {source_instance_name}.")
                        task.remarks=f"Unable to create VM image of {source_instance_name}."
                        raise Exception(f"Unable to create VM image of {source_instance_name}.")
                    else:
                        total_passed_tasks = total_passed_tasks + 1
                        task.task_status="Completed"
                    
                elif (task_name=="Create_Compute_Instance"): #4
                    instance_name = gcp_compute_instance.create_vm_from_machine_snapshoot(machine_image_name=vm_image_name)
                    time.sleep(30)
                    if (instance_name==None):
                        task.task_status="Failed"
                        task.remarks=f"Unable to create virtual machine."
                        current_task = task
                        self.log_error(f"Unable to create virtual machine.")
                        print(f"Unable to create virtual machine.")
                        raise Exception(f"Unable to create virtual machine.")
                    else:
                        total_passed_tasks = total_passed_tasks + 1
                        task.task_status="Completed"

                elif (task_name=="Connect_New_Instance_By_SSH"): #5
                    time.sleep(300) # wait until boot
                    # get local ip of the VM instance
                    vm_local_ip_address = gcp_compute_instance.get_vm_ip_address(instance_name=instance_name)
                    ram_size_gb = sh.get_ram_size_gb()
                    cpu_count = sh.get_cpu_count()

                    print(f"New Instance IP address: {vm_local_ip_address}")
                    print(f"New Instance Configuration: CPU - {cpu_count} core, RAM - {ram_size_gb}GB")
                    self.log_info(f"New Instance IP address: {vm_local_ip_address}")
                    self.log_info(f"New Instance Configuration: CPU - {cpu_count} core, RAM - {ram_size_gb}GB")

                    # Reinitialize IP Address
                    sh.ssh_host=vm_local_ip_address
                    sql_instance.host =vm_local_ip_address

                    # Test SSH
                    ssh_login_status = sh.ssh_connect_test()

                    if (ssh_login_status==False):
                        task.task_status="Failed"
                        task.remarks=f"SSH Connection on {vm_image_name} VM is failed!"
                        current_task = task
                        self.log_error(f"SSH Connection on {vm_image_name} VM is failed!")
                        raise Exception(f"SSH Connection on {vm_image_name} VM is failed!")
                    else:
                        total_passed_tasks = total_passed_tasks + 1
                        task.task_status="Completed"
                        self.log_info(f"SSH Connection on {vm_image_name} VM is successfull.")
                        print(f"SSH Connection on {vm_image_name} VM is successfull.")

                elif (task_name=="Mount_Data_Disk"): #6

                    boot_disk = get_variables().BOOT_DISK_NAME
                    mount_point = get_variables().DATA_DISK_MOUNT_POINT
                    data_disk_dev=sh.get_hdd_device(boot_disk=boot_disk)

                    # Mount existing data disk, if it is not mounted
                    mount_status = sh.mount_disk(device_name=data_disk_dev, mount_point=mount_point, is_new_disk=False)

                    if (mount_status==False):
                        task.task_status="Failed"
                        task.remarks=f"Unable to mount data disk!"
                        current_task = task
                        self.log_fatal(f"Unable to mount data disk!")
                        raise Exception(f"Unable to mount data disk!")
                    else:
                        total_passed_tasks = total_passed_tasks + 1
                        task.task_status="Completed"
                        print(f"Data disk mounted: {mount_point}")
                        self.log_info(f"Data disk mounted: {mount_point}")

                elif (task_name=="Remove_Recovery_Conf_File"): #7
                    
                    # remove recovery file
                    ssh_command_remove_recovery_file=f"sudo rm -rf {get_variables().DB_DATA_DIR}/recovery.conf"
                    ssh_command_status= sh.shell_command(command=ssh_command_remove_recovery_file)

                    # update pg_hba file
                    pg_hba_status = sh.update_pg_hba_config_file()

                    # Disable Archive log 

                    # restart DB service
                    command = f"sudo systemctl restart {db_service_name}"
                    db_start = sh.shell_command(command)
                    
                    if (ssh_command_status==True):
                        total_passed_tasks = total_passed_tasks + 1
                        task.task_status="Completed"
                        print(f"Recovery file is removed successfully.")
                        self.log_info(f"Recovery file is removed successfully.")
                    else:
                        task.task_status="Failed"
                        task.remarks="Unable to remove Recovery file!"
                        current_task = task
                        print("Unable to remove Recovery file!")
                        self.log_error("Unable to remove Recovery file!")

                elif (task_name=="Start_Database_Service"): # 8
                    
                    db_start = False
                    current_status = sh.check_service_status(service_name=db_service_name) 

                    if (current_status==False):
                        # Start Database
                        command = f"sudo systemctl start {db_service_name}"
                        db_start = sh.shell_command(command)
                    else:
                        db_start=True

                    if (db_start==True):
                        total_passed_tasks = total_passed_tasks + 1
                        task.task_status="Completed"
                        print(f"Database service is started successfully.")
                        self.log_info(f"Database service is started successfully.")
                    else:
                        task.task_status="Failed"
                        task.remarks=f"Unable to start database service."
                        current_task = task
                        print(f"Unable to start database service.")
                        self.log_error(f"Unable to start database service.")
                        raise Exception("Unable to start database service.")
                    
                elif (task_name=="Test_Database_Connection"): # 9
                    time.sleep(300) 
                    db_login_status=sql_instance.test_connection()

                    if (db_login_status==True):
                        
                        # Reset DB password
                        password_reset = sh.reset_db_password()
                        if (password_reset==True):
                            total_passed_tasks = total_passed_tasks + 1
                            task.task_status="Completed"
                            print(f"Database connection is successful.")
                            self.log_info(f"Database connection is successful.")
                        else:
                            task.task_status="Failed"
                            task.remarks="DB Password Reset is Unsuccessfull"
                            current_task = task
                            print("DB Password Reset is Unsuccessfull")
                            self.log_info("DB Password Reset is Unsuccessfull")
                            raise Exception("DB Password Reset is Unsuccessfull")
                    else:
                        task.task_status="Failed"
                        task.remarks=f"Unable to connect database."
                        print(f"Unable to connect database.")
                        self.log_error(f"Unable to connect database.")
                        raise Exception("Unable to connect database.")
                    
                elif (task_name=="Configure_Database_Parameters"): # 10
                    ram_size_gb = sh.get_ram_size_gb()
                    cpu_count = sh.get_cpu_count()
                    config_status = False

                    # Print Server Configuration

                    if (ram_size_gb!=None):
                        config_status = sql_instance.execute_db_configuration_xml(server_mem_size_gb=ram_size_gb,server_cpu_core=cpu_count)

                    if (config_status==True):
                        total_passed_tasks = total_passed_tasks + 1
                        task.task_status="Completed"
                        print(f"Database paramgters are reconfigured successfully.")
                        self.log_info(f"Database paramgters are reconfigured successfully")
                    else:
                        task.task_status="Failed"
                        current_task = task
                        task.remarks=f"Database Configuration is not successfull."
                        print(f"Database Configuration is not successfull.")
                        self.log_error(f"Database Configuration is not successfull.")

                elif (task_name=="Restart_Database_Service"): # 11
                    # Start Database
                    command = f"sudo systemctl restart {db_service_name}"
                    db_restart = sh.shell_command(command)
                    time.sleep(30)
                    if (db_restart==True):
                        total_passed_tasks = total_passed_tasks + 1
                        task.task_status="Completed"
                        task.remarks=f"Database service is restarted successfully."
                        print(f"Database service is restarted successfully.")
                        self.log_info(f"Database service is restarted successfully.")
                    else:
                        task.task_status="Failed"
                        task.remarks=f"Unable to restart database service."
                        current_task = task
                        print(f"Unable to restart database service.")
                        self.log_error(f"Unable to restart database service.")
                        raise Exception("Unable to restart database service.")

                elif (task_name=="Take_Dump_Backup_Public_SchemaOnly"): # 12
                    public_schema_only_dump = sh.take_database_schema_dump_backup(schemaname="public", isSchemaOnly=True)
                    time.sleep(15)
                    if (public_schema_only_dump==None):
                        task.task_status="Failed"
                        task.remarks=f"Unable to take public schema dump."
                        current_task = task
                        print(f"Unable to take public schema dump.")
                        self.log_error(f"Unable to take public schema dump.")
                        raise Exception("Unable to take public schema dump.")
                    else:
                        total_passed_tasks = total_passed_tasks + 1
                        task.task_status="Completed"

                elif (task_name=="Take_Dump_Backup_Archive_SchemaOnly"): # 13
                    archive_schema_only_dump = sh.take_database_schema_dump_backup(schemaname="archive", isSchemaOnly=True)
                    time.sleep(15)
                    if (archive_schema_only_dump==None):
                        task.task_status="Failed"
                        task.remarks=f"Unable to take archive schema dump."
                        current_task = task
                        print(f"Unable to take archive schema dump.")
                        self.log_error(f"Unable to take archive schema dump.")
                        raise Exception("Unable to take archive schema dump.")
                    else:
                        total_passed_tasks = total_passed_tasks + 1
                        task.task_status="Completed"

                elif (task_name=="Rename_Public_Schema"): # 14

                    ddl_query ="ALTER SCHEMA public RENAME TO public_original;"
                    schema_rename = sql_instance.execute_ddl(query=ddl_query)

                    if (schema_rename==False):
                        task.task_status="Failed"
                        task.remarks=f"Unable to rename schema public->public_original."
                        current_task = task
                        print(f"Unable to rename schema public->public_original.")
                        self.log_error(f"Unable to rename schema public->public_original.")
                        raise Exception("Unable to rename schema public->public_original.")
                    else:
                        total_passed_tasks = total_passed_tasks + 1
                        task.task_status="Completed"

                elif (task_name=="Rename_Archive_Schema"): # 15

                    ddl_query ="ALTER SCHEMA archive RENAME TO archive_original;"
                    schema_rename = sql_instance.execute_ddl(query=ddl_query)

                    if (schema_rename==False):
                        task.task_status="Failed"
                        task.remarks=f"Unable to rename schema archive->archive_original."
                        current_task = task
                        print(f"Unable to rename schema archive->archive_original.")
                        self.log_error(f"Unable to rename schema archive->archive_original.")
                        raise Exception("Unable to rename schema archive->archive_original.")
                    else:
                        total_passed_tasks = total_passed_tasks + 1
                        task.task_status="Completed"

                elif (task_name=="Restore_Public_Schema_Dump"): # 16
                    restore_schema = sh.restore_database_schema_dump_backup(db_backup_file=public_schema_only_dump)

                    if (restore_schema==None):
                        # Check table count
                        if (sql_instance.count_tables_in_schema(schema_name="public")==0):
                            task.task_status="Failed"
                            task.remarks=f"Unable to restore public schema dump."
                            current_task = task
                            print(f"Unable to restore public schema dump.")
                            self.log_error(f"Unable to restore public schema dump.")
                            raise Exception("Unable to restore public schema dump.")
                    else:
                        total_passed_tasks = total_passed_tasks + 1
                        task.task_status="Completed"

                elif (task_name=="Restore_Archive_Schema_Dump"): # 17
                    restore_schema = sh.restore_database_schema_dump_backup(db_backup_file=archive_schema_only_dump)

                    if (restore_schema==None):
                        # count tables
                        if (sql_instance.count_tables_in_schema(schema_name="archive")==0):
                            task.task_status="Failed"
                            task.remarks=f"Unable to restore archive schema dump."
                            current_task = task
                            print(f"Unable to restore archive schema dump.")
                            self.log_error(f"Unable to restore archive schema dump.")
                            raise Exception("Unable to restore archive schema dump.")
                    else:
                        total_passed_tasks = total_passed_tasks + 1
                        task.task_status="Completed"

                elif (task_name=="Execute_Xml_Sql"): # 18
                    sql_xml_file = sql_instance.execute_sql_xml()

                    if (sql_xml_file==None):
                        task.task_status="Failed"
                        task.remarks=f"Unable to execute sql xml file."
                        current_task = task
                        print(f"Unable to execute sql xml file.")
                        self.log_error(f"Unable to execute sql xml file.")
                        raise Exception("Unable to execute sql xml file.")
                    else:
                        total_passed_tasks = total_passed_tasks + 1
                        task.task_status="Completed"

                elif (task_name=="Drop_Schema"): # 19
                    drop_schema=sql_instance.drop_schemas()
                    if (drop_schema==False):
                        task.task_status="Failed"
                        task.remarks="Unable to drop schema!"
                        current_task = task
                        self.log_error("Unable to drop schema!")
                        print("Unable to drop schema!")
                        raise Exception("Unable to drop schema!")
                    else:
                        total_passed_tasks = total_passed_tasks + 1
                        task.task_status="Completed"

                    time.sleep(10)

                elif (task_name=="Take_Dump_Backup_Public_Schema"): # 20
                    time.sleep(15)
                    public_schema_dump = sh.take_database_schema_dump_backup(schemaname="public", isSchemaOnly=False)
                    if (public_schema_dump==None):
                        task.task_status="Failed"
                        task.remarks=f"Unable to take public schema dump."
                        current_task = task
                        print(f"Unable to take public schema dump.")
                        self.log_error(f"Unable to take public schema dump.")
                        raise Exception("Unable to take public schema dump.")
                    else:
                        total_passed_tasks = total_passed_tasks + 1
                        task.task_status="Completed"

                elif (task_name=="Take_Dump_Backup_Archive_Schema"): # 21
                    time.sleep(10)
                    archive_schema_dump = sh.take_database_schema_dump_backup(schemaname="archive", isSchemaOnly=False)
                    if (archive_schema_dump==None):
                        task.task_status="Failed"
                        task.remarks=f"Unable to take archive schema dump."
                        current_task = task
                        print(f"Unable to take archive schema dump.")
                        self.log_error(f"Unable to take archive schema dump.")
                        raise Exception("Unable to take archive schema dump.")
                    else:
                        total_passed_tasks = total_passed_tasks + 1
                        task.task_status="Completed"

                elif (task_name=="Upload_Public_Schema_Dump"): # 22
                    time.sleep(10)
                    bucket_name = get_variables().BUCKET_NAME
                    bucket_dest_location= get_variables().BUCKET_DIR_FOR_DB_BACKUP
                    public_dump_transfer = sh.upload_remote_file_google_cloud_bucket(bucket_name=bucket_name, dest_location=bucket_dest_location, src_file=public_schema_dump)
                    if (public_dump_transfer==False):
                        task.task_status="Failed"
                        task.remarks=f"Unable to upload public schema dump to {bucket_name}/{bucket_dest_location}"
                        current_task = task
                        self.log_error(f"Unable to upload public schema dump to {bucket_name}/{bucket_dest_location}")
                        print(f"Unable to upload public schema dump to {bucket_name}/{bucket_dest_location}")
                        raise Exception(f"Unable to upload public schema dump to {bucket_name}/{bucket_dest_location}")
                    else:
                        total_passed_tasks = total_passed_tasks + 1
                        task.task_status="Completed"

                elif (task_name=="Upload_Archive_Schema_Dump"): # 23
                    time.sleep(10)
                    bucket_name = get_variables().BUCKET_NAME
                    bucket_dest_location= get_variables().BUCKET_DIR_FOR_DB_BACKUP
                    archive_schema_dump_status = sh.upload_remote_file_google_cloud_bucket(bucket_name=bucket_name, dest_location=bucket_dest_location, src_file=archive_schema_dump)
                    if (archive_schema_dump_status==False):
                        task.task_status="Failed"
                        task.remarks=f"Unable to upload archive schema dump to {bucket_name}/{bucket_dest_location}"
                        current_task = task
                        self.log_error(f"Unable to upload archive schema dump to {bucket_name}/{bucket_dest_location}")
                        print(f"Unable to upload archive schema dump to {bucket_name}/{bucket_dest_location}")
                        raise Exception(f"Unable to upload archive schema dump to {bucket_name}/{bucket_dest_location}")
                    else:
                        total_passed_tasks = total_passed_tasks + 1
                        task.task_status="Completed"
                        
                        #update operation database
                        operation_db_instance.operation_master.output_dump_file = f"[{public_schema_dump},{archive_schema_dump}]"
                        upd_operation_status = operation_db_instance.update_operation_master()

                elif (task_name=="Take_Snapshot_Data_Disk"): # 24
                    time.sleep(10)
                    now_time = datetime.now()
                    date_string = now_time.strftime("%Y%m%d%H%M%S")
                    # Get Usage of existing Data disk
                    data_disk_mount_point = get_variables().DATA_DISK_MOUNT_POINT
                    data_disk_usage = sh.calculate_used_storage_gb(mount_point=data_disk_mount_point)
                    data_disk_usage = int(data_disk_usage) + 50 # add buffer 50GB
                    # Create new data disk
                    data_disk_prefix=get_variables().DATA_DISK_NAME_PREFIX
                    new_disk_name =f"{instance_name}-{data_disk_prefix}-{date_string}"
                    new_data_disk=gcp_compute_instance.create_persistent_disk(disk_name=new_disk_name, disk_size_gb=data_disk_usage)
                    if (new_data_disk==None):
                        task.task_status="Failed"
                        task.remarks="Unable to create partistent disk!"
                        current_task = task
                        self.log_error("Unable to create partistent disk!")
                        print("Unable to create partistent disk!")
                        raise Exception("Unable to create partistent disk!")
                        

                    # Attach new disk
                    attach_new_disk = gcp_compute_instance.attach_persistent_disk(disk_name=new_data_disk, instance_name=instance_name)
                    if (attach_new_disk==None):
                        task.task_status="Failed"
                        task.remarks="Unable to attach partistent disk!"
                        current_task = task
                        self.log_error("Unable to attach partistent disk!")
                        print("Unable to attach partistent disk!")
                        raise Exception("Unable to attach partistent disk!")
                    else:
                        total_passed_tasks = total_passed_tasks + 1
                        task.task_status="Completed"

                    # Mount new disk data2
                    boot_disk = get_variables().BOOT_DISK_NAME
                    source_dir = get_variables().DATA_DISK_MOUNT_POINT
                    target_dir = "/data2"
                    db_service_name=get_variables().DB_SERVICE_NAME
                    new_data_disk_dev=sh.get_hdd_device(boot_disk=target_dir)
                    mount_status = sh.mount_disk(device_name=new_data_disk_dev, mount_point=target_dir, is_new_disk=True)
                    if (mount_status==False):
                        task.task_status="Failed"
                        task.remarks="Unable to mount disk!"
                        current_task = task
                        self.log_error("Unable to mount disk!")
                        print("Unable to mount disk!")
                        raise Exception("Unable to mount disk!")
                    
                    # Stop DB service
                    command = f"sudo systemctl stop {db_service_name}"
                    db_service_status = sh.shell_command(command)

                    # Rsync data from /data to /data2
                    sync_data_status = sh.sync_data_between_folders(source_directory=source_dir, destination_directory=target_dir)
                    if (sync_data_status==False):
                        task.task_status="Failed"
                        task.remarks="Unable to sync data from {source_dir} to {target_dir}"
                        current_task = task
                        self.log_error(f"Unable to sync data from {source_dir} to {target_dir}")
                        print(f"Unable to sync data from {source_dir} to {target_dir}")
                        raise Exception(f"Unable to sync data from {source_dir} to {target_dir}")
                    else:
                        task.task_status="Completed"
                        print(f"Data Sync is done from {source_dir} to {target_dir}")
                        self.log_info(f"Data Sync is done from {source_dir} to {target_dir}")

                    # Unmount old data disk 
                    command = f"sudo umount {source_dir}"
                    unmount_status = sh.shell_command(command)

                    # # Detach /data disk
                    # old_data_disk_dev = data_disk_dev
                    # detach_old_disk_name = gcp_compute_instance.detach_persistent_disk(instance_name=instance_name, disk_name=old_data_disk_dev)
                    # if (mount_status==None):
                    #     task_status="Failed"
                    #     self.log_error(f"Unable to detach disk {old_data_disk_dev}")
                    #     print(f"Unable to detach disk {old_data_disk_dev}")
                    #     raise Exception(f"Unable to detach disk {old_data_disk_dev}")
                    
                    # # Delete /data disk
                    # delete_old_data_disk = gcp_compute_instance.delete_disk(disk_name=detach_old_disk_name)

                    # Unmount /data2
                    command = f"sudo umount {target_dir}"
                    unmount_status = sh.shell_command(command)

                    # Mount data disk as /data 
                    mount_status = sh.mount_disk(device_name=new_data_disk_dev, mount_point=source_dir, is_new_disk=False)
                    if (mount_status==False):
                        task.task_status="Failed"
                        task.remarks = "Unable to mount new data disk!"
                        current_task = task
                        self.log_error("Unable to mount new data disk!")
                        print("Unable to mount new data disk!")
                        raise Exception("Unable to mount new data disk!")
                    else:
                        task.task_status="Completed"
                    
                    # Give ownership on /data to enterprisedb:enterprisedb
                    command = f"sudo chown enterprisedb:enterprisedb -R {source_dir}"
                    command_status = sh.shell_command(command)

                    # start database service
                    command = f"sudo systemctl start {db_service_name}"
                    db_service_status = sh.shell_command(command)
                    if (db_service_status==False):
                        task.task_status="Failed"
                        task.remarks="Unable to start database service!"
                        current_task = task
                        self.log_error("Unable to start database service!")
                        print("Unable to start database service!")
                        raise Exception("Unable to start database service!")
                    else:
                        task.task_status="Completed"
                        
                    # Take snapshot of the data disk
                    data_disk_snapshoot_name = gcp_compute_instance.create_disk_snapshoot(disk_name=attach_new_disk)
                    if (data_disk_snapshoot_name==None):
                        task.task_status="Failed"
                        task.remarks="Unable to create disk snapshoot!"
                        current_task = task
                        self.log_error("Unable to create disk snapshoot!")
                        print("Unable to create disk snapshoot!")
                        raise Exception("Unable to create disk snapshoot!")
                    else:
                        task.task_status="Completed"

                        # update operational database
                        operation_db_instance.operation_master.output_data_disk_snapshot = data_disk_snapshoot_name
                        upd_operation_status = operation_db_instance.update_operation_master()
                        
                elif (task_name=="Remove_Machine_Image"): # 25
                    delete_machine_image = gcp_compute_instance.delete_machine_image(image_name=vm_image_name)
                    if (delete_machine_image==None):
                        task.task_status="Failed"
                        task.remarks="Unable to remove machine image!"
                        current_task = task
                        self.log_error("Unable to remove machine image!")
                        print("Unable to remove machine image!")
                        raise Exception("Unable to remove machine image!")
                    else:
                        total_passed_tasks = total_passed_tasks + 1
                        task.task_status="Completed"

                elif (task_name=="Remove_Compute_Instance"): # 26
                    remove_instance_status = gcp_compute_instance.delete_vm_and_disks(instance_name=instance_name)
                    if (remove_instance_status==None):
                        task.task_status="Failed"
                        task.remarks=f"Unable to remove compute instance {instance_name}!"
                        current_task = task
                        self.log_error(f"Unable to remove compute instance {instance_name}!")
                        print(f"Unable to remove compute instance {instance_name}!")
                        raise Exception(f"Unable to remove compute instance {instance_name}!")
                    else:
                        total_passed_tasks = total_passed_tasks + 1
                        task.task_status="Completed"

                # Task-wise end time
                end = timer()
                total_seconds = end - start
                duration = time.strftime("%H:%M:%S", time.gmtime(total_seconds))

                # update task
                task.task_end_datetime=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                task.task_duration=duration
                #task.remarks="Successfull"

                # Cumulative Time
                grand_total_seconds=grand_total_seconds+total_seconds

                # Log Tasks
                print(f"Task # {task_id}, Task Name : {task_name}, Status : {task.task_status}, Elapse Duration: {duration}")
                print("**********************************************************************************************")
                self.log_info(f"Task # {task_id}, Task Name : {task_name}, Status : {task.task_status}, Elapse Duration: {duration}")
                self.log_info("**********************************************************************************************")

                time.sleep(10)
                grand_total_duration = time.strftime("%H:%M:%S", time.gmtime(grand_total_seconds))
                print(f"Total Elapse Duration: {grand_total_duration}")
                self.log_info(f"Total Elapse Duration: {grand_total_duration}")

                # Update Task into database
                upd_task_status = operation_db_instance.update_operation_detail(OperationDetail=task)

                # Update master info into database
                operation_db_instance.operation_master.total_duration = grand_total_duration
                operation_db_instance.operation_master.total_passed_tasks = total_passed_tasks
                upd_operation_status = operation_db_instance.update_operation_master()

                # print(f"Master Data Update Staus: {upd_operation_status}")

                # Upload Log, if success it is okay
                self.upload_log()
                
            #grand_total_duration = time.strftime("%H:%M:%S", time.gmtime(grand_total_seconds))
            print(f"Total Elapse Duration: {grand_total_duration}")
            self.log_info(f"Total Elapse Duration: {grand_total_duration}")
            
            # Update operation into Database
            operation_db_instance.operation_master.end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            operation_db_instance.operation_master.total_duration = grand_total_duration
            operation_db_instance.operation_master.operation_status = "Completed"
            operation_db_instance.operation_master.total_passed_tasks = total_passed_tasks
            operation_db_instance.operation_master.output_dump_file = f"[{public_schema_dump},{archive_schema_dump}]"
            operation_db_instance.operation_master.output_data_disk_snapshot = data_disk_snapshoot_name
        
            upd_operation_status = operation_db_instance.update_operation_master()

            # Upload Log, if success it is okay
            self.upload_log()
            
            #Final Notication
            notification_instance.single_notification()

            return True
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Error: {e}")

            # Update status 
            if (total_passed_tasks>0):
                
                if (current_task !=None):
                    # Update Task into database
                    upd_task_status = operation_db_instance.update_operation_detail(OperationDetail=current_task)

                cleanup_jobs = 0
                # Clean resources
                if (total_passed_tasks>=3):

                    # Machine Image
                    try:
                        # delete machine image
                        if (vm_image_name!=None):
                            # start = timer()
                            # current_task.task_id = 25
                            # current_task.task_name = "Remove_Machine_Image"

                            delete_machine_image = gcp_compute_instance.delete_machine_image(image_name=vm_image_name)
                            # cleanup_jobs = cleanup_jobs + 1
                            # end = timer()
                            # total_seconds = end - start
                            # current_task.duration = time.strftime("%H:%M:%S", time.gmtime(total_seconds))
                            # task.task_end_datetime=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            # current_task.task_status = "Completed"

                            # upd_task_status = operation_db_instance.update_operation_detail(OperationDetail=current_task)

                    except Exception as e:
                        print(f"Error: {e}")

                    # Compute Instance
                    try:
                        # delete machine
                        if (instance_name!=None):
                            # start = timer()
                            # current_task.task_id = 26
                            # current_task.task_name = "Remove_Compute_Instance"
                            
                            remove_instance_status = gcp_compute_instance.delete_vm_and_disks(instance_name=instance_name)
                            # cleanup_jobs = cleanup_jobs + 1

                            # end = timer()
                            # total_seconds = end - start
                            # current_task.duration = time.strftime("%H:%M:%S", time.gmtime(total_seconds))
                            # task.task_end_datetime=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            # current_task.task_status = "Completed"

                    except Exception as e:
                        print(f"Error: {e}")
                    
                # Update operation into Database
                operation_db_instance.operation_master.end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                operation_db_instance.operation_master.total_duration = grand_total_duration
                operation_db_instance.operation_master.operation_status = "Failed"
                operation_db_instance.operation_master.total_passed_tasks = total_passed_tasks + cleanup_jobs
                operation_db_instance.operation_master.output_dump_file = f"[{public_schema_dump},{archive_schema_dump}]"
                operation_db_instance.operation_master.output_data_disk_snapshot = data_disk_snapshoot_name
            
                upd_operation_status = operation_db_instance.update_operation_master()

            # upload log
            self.upload_log()

            #Final Notication
            notification_instance.single_notification()

            return None