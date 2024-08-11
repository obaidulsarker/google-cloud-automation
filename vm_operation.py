from googleapiclient import discovery
from google.cloud import secretmanager
from google.oauth2 import service_account
from google.auth import exceptions
from datetime import datetime, timedelta, timezone
import time
from logger import *
from init_variables import get_variables
import os
import json

class VMoperation(Logger):
    def __init__(self, logfile):
        super().__init__(logfile)
        self.project_id = get_variables().PROJECT_ID
        self.region = get_variables().REGION_NAME
        self.zone = get_variables().ZONE_NAME
        self.vpc_name = get_variables().VPC_NAME
        self.subnetwork_name= get_variables().SUBNETWORK_NAME
        self.fixed_ip = get_variables().FIXED_ID
        self.storage_locations = get_variables().STORAGE_LOCATION
        self.vm_img_prefix=get_variables().VM_IMAGE_NAME_PREFIX
        self.vm_name_prefix=get_variables().VM_NAME_PREFIX
        self.machine_type=get_variables().MACHINE_TYPE_NAME
        self.gcloud_auth_scopes= get_variables().GCLOUD_AUTH_SCOPE
        self.service_account_key_file = get_variables().GOOGLE_SERVICE_ACCOUNT_FILE
        self.secret_name = get_variables().CLOUD_SECRET_NAME
        self.secret_version = get_variables().SECRET_VERSION

    def setup_google_credential(self):
        
        try:
            # Define the secret version ID you want to access
            secret_name = f"projects/{self.project_id}/secrets/{self.secret_name}/versions/{self.secret_version}"

            # Create a Secret Manager client
            client = secretmanager.SecretManagerServiceClient()

            # Access the secret
            response = client.access_secret_version(request={"name": secret_name})
            secret_data = response.payload.data.decode("UTF-8")

            # Check if the destination file exists
            if os.path.exists(self.service_account_key_file):
                # Remove the existing file
                os.remove(self.service_account_key_file)

            # Create a temporary service account key file
            #key_file_path = "cred\service-account-key.json"
            with open(self.service_account_key_file, "w") as key_file:
                key_file.write(secret_data)
            
            print(f"Google Credential setup is done successfully.")
            self.log_info(f"Google Credential setup is done successfully.")

            return True
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Error: {e}")
            return None

    def connect(self):

        # Set the GOOGLE_APPLICATION_CREDENTIALS environment variable
        #os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_account_key_file

        # Authenticate with the Google Cloud API using a service account key JSON file
        # Authenticate with the service account
        credentials = service_account.Credentials.from_service_account_file(
            self.service_account_key_file,
            scopes=[self.gcloud_auth_scopes]
        )

        # Connect Google cloud
        try:
            compute = discovery.build('compute', 'beta', credentials=credentials, cache_discovery=False)
            return compute
        
        except exceptions.DefaultCredentialsError:
            print("Please set up your Google Cloud credentials.")
            self.log_error("Please set up your Google Cloud credentials.")
            return None
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Error: {e}")
            return None
        

    # Get VM information
    def get_vm_info(self, instance_name):
        try:
            vm_info = self.connect().instances().get(
                project=self.project_id,
                zone=self.zone,
                instance=instance_name
            ).execute()

            return vm_info
    
        except Exception as e:
            print(f"Error: {e}")
            return None

    # Get VM information
    def get_vm_ip_address(self, instance_name):
        try:
            vm_info = self.get_vm_info(instance_name)

            return vm_info["networkInterfaces"][0]["networkIP"]
    
        except Exception as e:
            print(f"Error: {e}")
            return None
            
    # Create Machine Snapshoot and return Machine Image name. If error occured, return None
    def create_machine_snapshot(self, instance_name):
        #now = datetime.now()
        #dt_string = now.strftime("%Y%m%d%H%M%S")
        dt_string = datetime.now().strftime("%Y%m%d%H%M%S")

        vm_image_name = '{0}-{1}-{2}'.format(self.vm_img_prefix, instance_name, dt_string)

        # Construct the source VM URL
        source_vm = f'projects/{self.project_id}/zones/{self.zone}/instances/{instance_name}'
        #storage_locations = f"['{self.storage_locations}']"

        # Construct the request body
        request_body = {
            'guestFlush': False,
            'name': vm_image_name,
            'sourceInstance': source_vm,
            'storageLocations': ['asia']
        }
        # 'storageLocations': f"['{self.storage_locations}']"
        try:
            # Create the image from the VM
            operation = self.connect().machineImages().insert(
                project=self.project_id, 
                body=request_body
            ).execute()

            print(f"Creating Machine Image of {instance_name} VM...")
            self.log_info(f"Creating Machine Image of {instance_name} VM...")

            while True:

                op_status = self.connect().machineImages().get(
                    project=self.project_id,
                    machineImage=vm_image_name
                ).execute()
                
                if (op_status['status'] == 'READY'):
                    print(f"Created Machine Image: {vm_image_name}")
                    self.log_info(f"Created Machine Image: {vm_image_name} successfully")
                    break
                elif op_status['status'] == 'FAILED':
                    print("Machine Image creation is failed.")
                    self.log_error(f"Machine Image creation is failed.")
                    break
                else:
                    print(f"Creating Machine Image of {instance_name} VM...")
                    self.log_info(f"Creating Machine Image of {instance_name} VM...")
                    time.sleep(60)

            time.sleep(30)
            
            return vm_image_name
        
        except Exception as e:
            self.log_error(f"Error: {e}")
            print(f"Error: {e}")
            return None
    
    # Remove Machine Image
    def delete_machine_image(self, image_name):
        try:
            #name = f"{self.project_id}/global/machineImages/{image_name}"

            # Delete the machine image
            operation = self.connect().machineImages().delete(
                project=self.project_id, 
                machineImage=image_name
                ).execute()

            # print(operation)
            print(f"Deleting {image_name} Machine Image...")
            self.log_info(f"Deleting {image_name} Machine Image...")

            # # wait until completion
            # self.connect().zoneOperations().wait(
            #     project=self.project_id,
            #     zone=self.zone,
            #     operation=operation['name']
            # ).execute()
            
            # while True:
            #     op_status = self.connect().machineImages().get(
            #         project=self.project_id,
            #         machineImage=vm_image_name
            #     ).execute()
                
            #     print(f"status = {op_status['status']}")

            #     if (op_status['status'] == 'INVALID'):
            #         print(f"Deleted Machine Image: {image_name}")
            #         self.log_info(f"Deleted Machine Image: {image_name}")
            #         break
            #     else:
            #         print(f"Deleting {image_name} Machine Image...")
            #         self.log_info(f"Deleting {image_name} Machine Image...")
            #         time.sleep(60)

                # op_status = self.connect().zoneOperations().get(
                #     project=self.project_id,
                #     zone=self.zone,
                #     operation=operation['name']
                # ).execute()

                # print(f"op_status={op_status}")

                # if (op_status['status'] == 'DONE'):
                #     print(f"Deleted Machine Image: {image_name}")
                #     self.log_info(f"Deleted Machine Image: {image_name}")
                #     break
                # else:
                #     print(f"Deleting {image_name} Machine Image...")
                #     self.log_info(f"Deleting {image_name} Machine Image...")
                #     time.sleep(60)

            time.sleep(30)

            print(f"Deleted Machine Image: {image_name}")
            self.log_info(f"Deleted Machine Image: {image_name}")

            return image_name

        except Exception as e:
            print(f"Error: {e}")
            return None
                
    # Create Virtual Machine from Machine Snapshoot
    def create_vm_from_machine_snapshoot(self, machine_image_name):
        now = datetime.now()
        dt_string = now.strftime("%Y%m%d%H%M%S")
        new_instance_name = '{0}-{1}'.format(self.vm_name_prefix,dt_string)

        resource_level = get_variables().RESOURCE_LABEL.replace("$NEW_VM_NAME",f"{new_instance_name}")
        resource_level = json.loads(resource_level)

        machine_type_name=self.machine_type
        machine_type = 'projects/{0}/zones/{1}/machineTypes/{2}'.format( self.project_id, self.zone, machine_type_name)
        #"machineType": "projects/bracit-dev/zones/asia-east1-b/machineTypes/n1-standard-1"

        network = 'projects/{0}/global/networks/{1}'.format(self.project_id, self.vpc_name)
        subnetwork = 'projects/{0}/regions/{1}/subnetworks/{2}'.format(self.project_id, self.region, self.subnetwork_name)
        #"subnetwork": "projects/bracit-dev/regions/asia-east1/subnetworks/bracit-dev-taiwan"

        zone_url='projects/{0}/zones/{1}'.format(self.project_id, self.zone)
        machine_image_name_url = 'projects/{0}/global/machineImages/{1}'.format(self.project_id, machine_image_name)
        #"projects/bracit-dev/global/machineImages/vm-img-small-db-1-20231025215300",

        # Construct the request body for the new VM
        request_body = {
            'name': new_instance_name,
            'machineType': machine_type,
            'sourceMachineImage': machine_image_name_url,
            'zone': zone_url,
            'networkInterfaces': [
                {
                    'network': network,
                    'subnetwork': subnetwork,
                    'networkIP': self.fixed_ip,
                    'accessConfigs': [
                        {
                            'type': 'ONE_TO_ONE_NAT',
                            'name': 'External NAT',
                            'networkTier': 'PREMIUM'
                        }
                    ],
                    'stackType': 'IPV4_ONLY'
                }
            ],
            'labels': resource_level,
            'deletionProtection': False,
            "metadata": {
                "items": [
                {
                    "key": "ssh-keys",
                    "value": "user1:ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQDASoppjmWU8y0JBQir8gvr3rxcJdFBh6gIJtsflbWi90I6ozO0+mk894JFoosTj073GPafzLOQt3Ybz7gvLGX9CqkMr0q4H32To1rlgaGkdq6x4+e7D9Bk9NViF4dVYmoac2oTLz+6eJl4h/y5GcOlKcTQjKCgZAxjY51oVNpEFO2r5SMgbRv3x0UhxG64FrRjUAw67WmkOzALc2BF6nd1l7YqDLaCb/pAq/lXknpX/QYpLCtetAPjgO2O4UDY534gz3X2tjMNpm1aY3fCaN2lmTGURqrBQ9bgaQ6i8FR3rd8I8hMxR4F/CDVjR85ZIx3knMk7zObQMMJUlEnFB+mzaN8ml3aGUsLo7EeiCrudMXUYwWeVjldyVeqlNkVC8Iv+Pkbfcv2m7yi8CWgf8OM6wrkS1NJSg75wYCP+Q1QTnvFRr5BwBW8DuN91nQr9V4XkUqyw8Z8C4E8Ndmo9dxEEtOvKvOib/gyVsRujQhFVluYin7Qee1PR2ziKwHvYG6s= bitsuser\nomarfaruk:ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDjln46qet+cDsOStm/S2xldsfi5OM3CYWrdaG9nBkfU6yoFnkUVYYYX3bIwWpwHM+DA2yLYIcEpD1n9V9X2k4TnCzVPsGDBE2qCj72/Zr06QDLcTyYcZ6tfbCzgy9/h5bbOByWCCHB3xrhBX/6cJFW4AIrTJjAcfkZth4GMwZ6iCnk+lXtP3Da+T+E0Oc5Sdj8o1l7nmztNHWBv6ebBEOALjYLIBapxmCvqNIIiftVFTsPF1IZ user1"
                }
                ]
        },
        "tags": {
            "items": [
            "allow-ssh",
            "allow-db"
            ]
        }
        }

        try:

            # Create the new VM
            operation = self.connect().instances().insert(project=self.project_id, zone=self.zone, body=request_body).execute()

            print(f"Creating {new_instance_name} Compute Instance...")
            self.log_info(f"Creating {new_instance_name} Compute Instance...")

            time.sleep(120)

            # wait until completion
            # self.connect().zoneOperations().wait(
            #     project=self.project_id,
            #     zone=self.zone,
            #     operation=operation['name']
            # ).execute()

            while True:
                
                #print ("STATUS")
                # op_status = self.connect().zoneOperations().get(
                #     project=self.project_id,
                #     zone=self.zone,
                #     operation=operation['name']
                # ).execute()

                op_status = self.connect().instances().get(
                            project=self.project_id,
                            zone=self.zone,
                            instance=new_instance_name
                    ).execute()
                
                if (op_status['status'] == 'RUNNING'):
                    print(f"Created Compute Instance: {new_instance_name}")
                    self.log_info(f"Created Compute Instance: {new_instance_name}")
                    break
                elif op_status['status'] == 'REPAIRING':
                    print(f"Compute Instance {new_instance_name} is failed.")
                    self.log_error(f"Compute Instance {new_instance_name} is failed.")
                    break
                else:
                    print(f"Creating {new_instance_name} Compute Instance...")
                    self.log_info(f"Creating {new_instance_name} Compute Instance...")
                    time.sleep(60)

            time.sleep(30)
            
            return new_instance_name
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Error: {e}")
            return None
        
    # Create a Disk Snapshot
    def create_disk_snapshoot(self, disk_name):
        
        print(f"Started disk snapshot deletion job")
        self.log_info(f"Started disk snapshot deletion job")
        
        now = datetime.now()
        dt_string = now.strftime("%Y%m%d%H%M%S")

        # Snapshot Expiration Date
        expiration_date = now + timedelta(days=10)
        expiration_label = expiration_date.strftime('%Y-%m-%d')

        # snapshoot name
        snapshot_prefix=get_variables().SNAPSHOT_PREFIX
        snapshot_name = f"{snapshot_prefix}-{disk_name}"

        # Create snapshot request body
        request_body = {
            "name": snapshot_name,
            "sourceDisk": f"projects/{self.project_id}/zones/{self.zone}/disks/{disk_name}",
            'labels': {
            'delete_after': expiration_label
            }
        }

        try:
            # Create the snapshot
            operation = self.connect().disks().createSnapshot(project=self.project_id, zone=self.zone, disk=disk_name, body=request_body).execute()

            print(f"Creating snapshoot of {disk_name} Disk...")
            self.log_info(f"Creating snapshoot of {disk_name} Disk...")

            # self.connect().zoneOperations().wait(
            #     project=self.project_id,
            #     zone=self.zone,
            #     operation=operation['name']
            # ).execute()

            while True:
                op_status = self.connect().zoneOperations().get(
                    project=self.project_id,
                    zone=self.zone,
                    operation=operation['name']
                ).execute()

                if (op_status['status'] == 'DONE'):
                    print(f"Created Disk Snapshot: {snapshot_name}")
                    self.log_info(f"Created Disk Snapshot: {snapshot_name}")
                    break
                else:
                    print(f"Creating snapshot of {disk_name} Disk...")
                    self.log_info(f"Creating snapshot of {disk_name} Disk...")
                    time.sleep(60)
            
            
            return snapshot_name
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Error: {e}")
            return None

    # Remove older snapshots
    def delete_old_snapshots(self, prefix, days_old=10):
        try:
            # Get the current time
            #now = datetime.utcnow()
            now = datetime.now(timezone.utc)
            
            # Calculate the cutoff time
            cutoff_time = now - timedelta(days=int(days_old))
            
            # Authenticate and build the compute service
            #credentials, project = google.auth.default()
            #service = build('compute', 'v1', credentials=credentials)
            
            # List all snapshots
            request = self.connect().snapshots().list(project=self.project_id)
            
            while request is not None:
                response = request.execute()
                
                if 'items' in response:
                    for snapshot in response['items']:
                        # Check if the snapshot name starts with the given prefix
                        if snapshot['name'].startswith(prefix):
                            # Parse the snapshot creation time
                            creation_time_str = snapshot['creationTimestamp']
                            creation_time = datetime.fromisoformat(creation_time_str.replace('Z', '+00:00'))
                    
                            # creation_time = datetime.strptime(snapshot['creationTimestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
                            
                            # Check if the snapshot is older than the cutoff time
                            if creation_time < cutoff_time:
                                # Delete the snapshot
                                self.log_info(f"Deleting snapshot: {snapshot['name']} (created on {creation_time})")
                                print(f"Deleting snapshot: {snapshot['name']} (created on {creation_time})")
                                delete_request = self.connect().snapshots().delete(project=self.project_id, snapshot=snapshot['name'])
                                delete_request.execute()
                                time.sleep(5)

                request = self.connect().snapshots().list_next(previous_request=request, previous_response=response)
            return True
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Error: {e}")
            return None
        
    # Get Disk list of a particular VM. It return list of disks detail info
    def get_vm_disk_list(self, instance_name):
        try:
            # Get instance details
            instance = self.get_vm_info(instance_name)

            return instance['disks']
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Error: {e}")
            return None

    # Get a Disk detail information
    def get_disk_info(self, disk_id):
        try:
            disk_info = self.connect().disks().get(project=self.project_id, zone=self.zone, disk=disk_id).execute()

            return disk_info
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Error: {e}")
            return None

    # Delete a Disk
    def delete_disk(self, disk_name):
        try:
            operation = self.connect().disks().delete(project=self.project_id, zone=self.zone, disk=disk_name).execute()

            print(f"Deleting {disk_name} Disk...")
            self.log_info(f"Deleting {disk_name} Disk...")

            self.connect().zoneOperations().wait(
                project=self.project_id,
                zone=self.zone,
                operation=operation['name']
            ).execute()

            while True:
                op_status = self.connect().zoneOperations().get(
                    project=self.project_id,
                    zone=self.zone,
                    operation=operation['name']
                ).execute()

                if (op_status['status'] == 'DONE'):
                    print(f"Deleted Disk: {disk_name}")
                    self.log_info(f"Deleted Disk: {disk_name}")
                    break
                else:
                    print(f"Disk Deletion {disk_name} in progess ...")
                    self.log_info(f"Disk Deletion {disk_name} in progess ...")
                    time.sleep(60)

            return disk_name
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Error: {e}")
            return None
                
    # Delete VM including disks
    def delete_vm_and_disks(self, instance_name):

        try:
            # Get information about the instance
            instance_info = self.get_vm_info(instance_name)

            # Delete the instance and its disks
            operation = self.connect().instances().delete(
                project=self.project_id,
                zone=self.zone,
                instance=instance_name
            ).execute()

            # Wait for the operation to complete
            print(f"Deleting Compute instance {instance_name}...")
            self.log_info(f"Deleting Compute instance {instance_name}...")

            # self.connect().zoneOperations().wait(
            #     project=self.project_id,
            #     zone=self.zone,
            #     operation=operation['name']
            # ).execute()

            while True:
                op_status = self.connect().zoneOperations().get(
                    project=self.project_id,
                    zone=self.zone,
                    operation=operation['name']
                ).execute()

                if (op_status['status'] == 'DONE'):
                    print(f"Deleted Compute instance {instance_name}.")
                    self.log_info(f"Deleted Compute instance {instance_name}.")
                    break
                else:
                    print(f"Deleting Compute instance {instance_name}...")
                    self.log_info(f"Deleting Compute instance {instance_name}...")
                    time.sleep(60)

            # Get Disks which is attached this VM
            deleted_vm_disk_list_tmp = instance_info.get('disks', [])
            # Get Disks which autoDelete is False
            deleted_vm_disk_list = [item for item in deleted_vm_disk_list_tmp if item['autoDelete'] == False]

            # Delete any additional non-boot disks associated with the instance
            for disk in deleted_vm_disk_list:
                disk_name = disk['source'].split('/')[-1]
                operation = self.connect().disks().delete(
                    project=self.project_id,
                    zone=self.zone,
                    disk=disk_name
                ).execute()
                
                print(f"Deleting disk {disk_name}...")
                self.log_info(f"Deleting disk {disk_name}...")

                self.connect().zoneOperations().wait(
                    project=self.project_id,
                    zone=self.zone,
                    operation=operation['name']
                ).execute()

                print(f"Disk {disk_name} deleted successfully.")
                self.log_info(f"Disk {disk_name} deleted successfully.")

            return "Success"
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Error: {e}")
            return None
        
    # Get disk name by a mount point
    def get_data_disk_name_by_mount_point(self, instance_name, mount_point):

        disk_name = ""

        try:

            # Get instance details
            instance = self.get_vm_info(instance_name)

            # Get associated disk
            for disk in instance.get('disks', []):
                    if disk.get('deviceName') and disk.get('source'):
                        disk_name = disk['deviceName']
                        source_link = disk['source']
                        source_parts = source_link.split('/')
                        disk_zone = self.zone_name
                        disk_id = source_parts[-1]
                        disk_info = self.connect().disks().get(project=self.project_id, zone=disk_zone, disk=disk_id).execute()

                        #print(disk_info)
                        if 'users' in disk_info:
                            for user in disk_info['users']:
                                if user == mount_point:
                                    print(f"Disk name associated with mount point {mount_point}: {disk_name}")
                                    self.log_info(f"Disk name associated with mount point {mount_point}: {disk_name}")
                                    break

            print(f"Disk name associated with mount point {mount_point}: {disk_name}")
            self.log_info(f"Disk name associated with mount point {mount_point}: {disk_name}")

            return disk_name
        
        except exceptions.DefaultCredentialsError:
            print("Please set up your Google Cloud credentials.")
            self.log_error("Please set up your Google Cloud credentials.")
            return None
        except Exception as e:
            print(f"An error occurred: {e}")
            self.log_error(f"An error occurred: {e}")
            return None

    # Check a disk is used by a instance or not   
    def is_disk_attached(self, disk_name, instance_name):
        try:
 
            # List all instances in the specified project and zone
            request = self.connect().instances().get(project=self.project_id, zone=self.zone, instance=instance_name)
            response = request.execute()
            instances = response.get('items', [])

            if not instances:
                return False

            for instance in instances:
                # Check if the disk is attached to the instance
                if 'disks' in instance:
                    for attached_disk in instance['disks']:
                        if attached_disk['deviceName'] == disk_name:
                            return True

            return False

        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Error: {e}")
            return False

    # Update autoDelete=True for all disk of a VM
    def update_disk_auto_delete(self, instance_name):
        try:

            # Get the VM instance details
            instance_request = self.connect().instances().get(project=self.project_id, zone=self.zone, instance=instance_name)
            instance_response = instance_request.execute()
            
            if 'disks' in instance_response:
                for disk in instance_response['disks']:
                    disk_name = disk['deviceName']
                    disk_auto_delete = True  # Set autoDelete to True

                    # Update the disk autoDelete attribute
                    disk_request = self.connect().instances().setDiskAutoDelete(
                        project=self.project_id, zone=self.zone, instance=instance_name,
                        autoDelete=disk_auto_delete, deviceName=disk_name
                    )
                    disk_request.execute()

                    print(f"Updated autoDelete for disk {disk_name} to {disk_auto_delete}")
                    self.log_info(f"Updated autoDelete for disk {disk_name} to {disk_auto_delete}")

            else:
                print(f"No disks found for instance {instance_name}")
                self.log_info(f"No disks found for instance {instance_name}")

            return "Sucessful"
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Error: {e}")
            return None

    # Create a empy parsistent DISK   
    def create_persistent_disk(self, disk_name, disk_size_gb):
        try:
            
            # Disk Type
            disk_type = f'projects/{self.project_id}/zones/{self.zone}/diskTypes/pd-balanced'

            # Define the request body for the persistent disk
            request_body = {
                "name": disk_name,
                "sizeGb": disk_size_gb,
                "type": disk_type,
                "zone": f"projects/{self.project_id}/zones/{self.zone}",
                "autoDelete": True,
                "boot": False,
                "mode": "READ_WRITE"
            }

            # print(request_body)

            # Create the persistent disk
            operation = self.connect().disks().insert(
                    project=self.project_id, 
                    zone=self.zone, 
                    body=request_body
                ).execute()

            # print(operation)

            while True:
                op_status = self.connect().zoneOperations().get(
                    project=self.project_id,
                    zone=self.zone,
                    operation=operation['name']
                ).execute()

                # Get Disk Status
                disk_info = self.get_disk_info(disk_name)

                if (op_status['status'] == 'DONE' and disk_info['status']=='READY'):
                    print(f"Created Disk : {disk_name}")
                    self.log_info(f"Created Disk : {disk_name}")
                    break
                else:
                    print(f"Creating Disk {disk_name}...")
                    self.log_info(f"Creating Disk {disk_name}...")
                    time.sleep(60)

            return disk_name
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Error: {e}")
            return None

    # Attach the created DISK to Instance   
    def attach_persistent_disk(self, disk_name, instance_name):
        try:
            
            # Disk Info reqest body
            # disk_request_body = {
            #     "source": f"projects/{self.project_id}/zones/{self.zone}/disks/{disk_name}",
            #     "autoDelete": False,
            #     "boot": False,
            #     "deviceName": disk_name,
            #     "mode": "READ_WRITE"
            # }

            disk_request_body = {
                "deviceName": disk_name,
                "source": f"projects/{self.project_id}/zones/{self.zone}/disks/{disk_name}",
                "autoDelete": False
            }

            #print(disk_request_body)

            # Get VM info
            # instance_info = self.get_vm_info(instance_name=instance_name)

            # print(instance_info)

            # # Attach Disk
            # instance_info['disks'].append(instance_info)

            # Create the persistent disk
            operation = self.connect().instances().attachDisk(
                    project=self.project_id, 
                    zone=self.zone, 
                    instance=instance_name, 
                    body=disk_request_body
                ).execute()

            # print(operation)
            
            while True:
                op_status = self.connect().zoneOperations().get(
                    project=self.project_id,
                    zone=self.zone,
                    operation=operation['name']
                ).execute()

                if (op_status['status'] == 'DONE'):
                    print(f"Attached Disk {disk_name} to {instance_name} instance.")
                    self.log_info(f"Attached Disk {disk_name} to {instance_name} instance.")
                    break
                else:
                    print(f"Attaching Disk {disk_name} to {instance_name} instance ...")
                    self.log_info(f"Attaching Disk {disk_name} to {instance_name} instance ...")
                    time.sleep(60)

            return disk_name
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Error: {e}")
            return None

    # Detach the created DISK to Instance   
    def detach_persistent_disk(self, disk_name, instance_name):
        try:

            disk_request_body = {
                "deviceName": disk_name
            }

            #print(disk_request_body)

            # Detach the persistent disk
            operation = self.connect().instances().detachDisk(
                    project=self.project_id, 
                    zone=self.zone, 
                    instance=instance_name, 
                    body=disk_request_body
                ).execute()

            # print(operation)
            
            while True:
                op_status = self.connect().zoneOperations().get(
                    project=self.project_id,
                    zone=self.zone,
                    operation=operation['name']
                ).execute()

                if (op_status['status'] == 'DONE'):
                    print(f"Detached Disk {disk_name} from {instance_name} instance.")
                    self.log_info(f"Detached Disk {disk_name} from {instance_name} instance.")
                    break
                else:
                    print(f"Detaching Disk {disk_name} from {instance_name} instance ...")
                    self.log_info(f"Detaching Disk {disk_name} to {instance_name} instance ...")
                    time.sleep(60)

            return disk_name
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Error: {e}")
            return None

# if __name__ == "__main__":
#     operation_log="logs\log_26fb0e36-c91c-4f78-b2fb-01a2dd44b554_2023-10-28_23-40-10.log"
#     vm_image="vm-img-small-db-1-20231123204727"

#     # Defind SQL execution Instance
#     gcp_compute_instance = VMoperation(operation_log)
#     vm = gcp_compute_instance.create_vm_from_machine_snapshoot(machine_image_name=vm_image)
#     print(f"VM = {vm}")

    # Take snapshot of the data disk
    # data_disk_snapshoot_name = gcp_compute_instance.create_disk_snapshoot(disk_name=new_disk_name)
    # if (data_disk_snapshoot_name==None):
    #     gcp_compute_instance.log_error("Unable to create disk snapshoot!")
    #     print("Unable to create disk snapshoot!")
    #     raise Exception("Unable to create disk snapshoot!")
    
    # vm_image_name="data-check-temp-vm"
    # delete_machine_image = gcp_compute_instance.delete_machine_image(image_name=vm_image_name)
    # if (delete_machine_image==None):
    #         gcp_compute_instance.log_error("Unable to remove machine image!")
    #         print("Unable to remove machine image!")
    #         raise Exception("Unable to remove machine image!")
    
    # instance_name="smalldb-20231028235501"
    # remove_instance_status = gcp_compute_instance.delete_vm_and_disks(instance_name=instance_name)
    # if (remove_instance_status==None):
    #     log.log_error(f"Unable to remove compute instance {instance_name}!")
    #     print(f"Unable to remove compute instance {instance_name}!")
    #     raise Exception(f"Unable to remove compute instance {instance_name}!")
 
