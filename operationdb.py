import sqlite3
from init_variables import get_variables
from logger import Logger
import requests

# operation master class
class OperationMaster:
    def __init__(self, operation_id, operation_log, start_datetime, end_datetime, total_duration, operation_status, source_database_vm_name, source_database_vm_ip, total_tasks, total_passed_tasks, output_dump_file, output_data_disk_snapshot, office_list, has_pending_voucher):
        self.operation_id =  operation_id
        self.operation_log = operation_log
        self.start_datetime=start_datetime
        self.end_datetime = end_datetime
        self.total_duration = total_duration
        self.operation_status = operation_status
        self.source_database_vm_name = source_database_vm_name
        self. source_database_vm_ip = source_database_vm_ip
        self.total_tasks = total_tasks
        self.total_passed_tasks = total_passed_tasks
        self.output_dump_file = output_dump_file
        self.output_data_disk_snapshot = output_data_disk_snapshot
        self.office_list=office_list 
        self.has_pending_voucher=has_pending_voucher

# operation detail class
class OperationDetail:
    def __init__(self, operation_id, task_id, task_name, task_description, task_start_datetime, task_end_datetime, task_duration, task_status, remarks):
        self.operation_id =  operation_id
        self.task_id=task_id
        self.task_name = task_name
        self.task_description = task_description
        self.task_start_datetime = task_start_datetime
        self.task_end_datetime = task_end_datetime
        self.task_duration = task_duration
        self.task_status = task_status
        self.remarks = remarks

class OperationMasterData(Logger):
    def __init__(self, logfile, OperationMasterObj):
        super().__init__(logfile)
        self.operation_id =  OperationMasterObj.operation_id
        self.operation_log = OperationMasterObj.operation_log
        self.start_datetime=OperationMasterObj.start_datetime
        self.end_datetime = OperationMasterObj.end_datetime
        self.total_duration = OperationMasterObj.total_duration
        self.operation_status = OperationMasterObj.operation_status
        self.source_database_vm_name = OperationMasterObj.source_database_vm_name
        self. source_database_vm_ip = OperationMasterObj.source_database_vm_ip
        self.total_tasks = OperationMasterObj.total_tasks
        self.total_passed_tasks = OperationMasterObj.total_passed_tasks
        self.output_dump_file = OperationMasterObj.output_dump_file
        self.output_data_disk_snapshot = OperationMasterObj.output_data_disk_snapshot
        self.office_list = OperationMasterObj.office_list
        self.has_pending_voucher=OperationMasterObj.has_pending_voucher

        self.pending_voucher_check_api=get_variables().PENDING_VOUCHER_CHECK_API
        self.db = get_variables().AUTOMATION_DB

    def connect(self):
        try:
            connection = sqlite3.connect(self.db)
            connection.isolation_level = None

            # print(f"Connected to {self.db}")

            return connection  # Success
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error
             
    #@staticmethod
    def create(self):
        try:
            cursor = self.connect().cursor()
            sql = f"""INSERT INTO operation (operation_id, operation_log, start_datetime, end_datetime, total_duration, operation_status, source_database_vm_name, source_database_vm_ip, 
            total_tasks, total_passed_tasks, output_dump_file, output_data_disk_snapshot, office_list, has_pending_voucher)
            VALUES ('{self.operation_id}', '{self.operation_log}', '{self.start_datetime}', '{self.end_datetime}', '{self.total_duration}', '{self.operation_status}', '{self.source_database_vm_name}', '{self.source_database_vm_ip}', '{self.total_tasks}', '{self.total_passed_tasks}', '{self.output_dump_file}', '{self.output_data_disk_snapshot}', '{self.office_list}', '{self.has_pending_voucher}');
            """
            #print(sql)
            cursor.execute(sql)
            self.connect().commit()

            return True
        except Exception as e:
            print(f"Exception: {str(e)}")
            self.log_error(f"Exception: {str(e)}")
            return False
    
    #@staticmethod
    def read_all(self):
        try:
            cursor = self.connect().cursor()
            cursor.execute("SELECT * FROM operation")
            rows = cursor.fetchall()
            return [OperationMaster(*row) for row in rows]
        except Exception as e:
            print(f"Exception: {str(e)}")
            self.log_error(f"Exception: {str(e)}")
            return None
    
    #@staticmethod
    def read_by_id(self, id):
        try:
            sql = f"SELECT * FROM operation WHERE operation_id='{id}'"
            cursor = self.connect().cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            return [OperationMaster(*row) for row in rows]
        except Exception as e:
            print(f"Exception: {str(e)}")
            self.log_error(f"Exception: {str(e)}")
            return None
        
    #@staticmethod
    def update(self):
        try:
            if (id==None):
                raise Exception("Please provide Id of the operation!")
            
            #print("START---")
            sql_upd_col = "UPDATE operation SET "
            sql_where = f" WHERE operation_id='{self.operation_id}';"
            col_no = 0

            if (self.operation_log!=None):
                if (col_no==0):
                    sql_upd_col = sql_upd_col + f" operation_log='{self.operation_log}'"
                else:
                    sql_upd_col = sql_upd_col + f", operation_log='{self.operation_log}'"
                col_no = col_no +1

            if (self.start_datetime!=None):
                if (col_no==0):
                    sql_upd_col = sql_upd_col + f" start_datetime='{self.start_datetime}'"
                else:
                    sql_upd_col = sql_upd_col + f", start_datetime='{self.start_datetime}'"
                col_no = col_no +1
            
            if (self.end_datetime!=None):
                if (col_no==0):
                    sql_upd_col = sql_upd_col + f" end_datetime='{self.end_datetime}'"
                else:
                    sql_upd_col = sql_upd_col + f", end_datetime='{self.end_datetime}'"
                col_no = col_no +1

            if (self.total_duration!=None):
                if (col_no==0):
                    sql_upd_col = sql_upd_col + f" total_duration='{self.total_duration}'"
                else:
                    sql_upd_col = sql_upd_col + f", total_duration='{self.total_duration}'"
                col_no = col_no +1

            if (self.operation_status!=None):
                if (col_no==0):
                    sql_upd_col = sql_upd_col + f" operation_status='{self.operation_status}'"
                else:
                    sql_upd_col = sql_upd_col + f", operation_status='{self.operation_status}'"
                col_no = col_no +1

            if (self.source_database_vm_name!=None):
                if (col_no==0):
                    sql_upd_col = sql_upd_col + f" source_database_vm_name='{self.source_database_vm_name}'"
                else:
                    sql_upd_col = sql_upd_col + f", source_database_vm_name='{self.source_database_vm_name}'"
                col_no = col_no +1

            if (self.source_database_vm_ip!=None):
                if (col_no==0):
                    sql_upd_col = sql_upd_col + f" source_database_vm_ip='{self.source_database_vm_ip}'"
                else:
                    sql_upd_col = sql_upd_col + f", source_database_vm_ip='{self.source_database_vm_ip}'"
                col_no = col_no +1

            if (self.total_tasks!=None):
                if (col_no==0):
                    sql_upd_col = sql_upd_col + f" total_tasks={self.total_tasks}"
                else:
                    sql_upd_col = sql_upd_col + f", total_tasks={self.total_tasks}"
                col_no = col_no +1

            if (self.total_passed_tasks!=None):
                if (col_no==0):
                    sql_upd_col = sql_upd_col + f" total_passed_tasks={self.total_passed_tasks}"
                else:
                    sql_upd_col = sql_upd_col + f", total_passed_tasks={self.total_passed_tasks}"
                col_no = col_no +1

            if (self.output_dump_file!=None):
                if (col_no==0):
                    sql_upd_col = sql_upd_col + f" output_dump_file='{self.output_dump_file}'"
                else:
                    sql_upd_col = sql_upd_col + f", output_dump_file='{self.output_dump_file}'"
                col_no = col_no +1

            if (self.output_data_disk_snapshot!=None):
                if (col_no==0):
                    sql_upd_col = sql_upd_col + f" output_data_disk_snapshot='{self.output_data_disk_snapshot}'"
                else:
                    sql_upd_col = sql_upd_col + f", output_data_disk_snapshot='{self.output_data_disk_snapshot}'"
                col_no = col_no +1
            
            if (self.office_list!=None):
                if (col_no==0):
                    sql_upd_col = sql_upd_col + f" office_list='{self.office_list}'"
                else:
                    sql_upd_col = sql_upd_col + f", office_list='{self.office_list}'"
                col_no = col_no +1

            if (self.has_pending_voucher!=None):
                if (col_no==0):
                    sql_upd_col = sql_upd_col + f" has_pending_voucher='{self.has_pending_voucher}'"
                else:
                    sql_upd_col = sql_upd_col + f", has_pending_voucher='{self.has_pending_voucher}'"
                col_no = col_no +1

            # concat
            sql = sql_upd_col +" "+ sql_where
            # print(sql)
            # self.log_info(sql)

            cursor = self.connect().cursor()
            cursor.execute(sql)
            self.connect().commit()

            return True
        except Exception as e:
            print(f"Exception: {str(e)}")
            self.log_error(f"Exception: {str(e)}")
            return None

    #@staticmethod
    def delete(self, id):
        try:
            sql = f"DELETE FROM operation WHERE operation_id = '{id}'"
            cursor = self.connect().cursor()
            cursor.execute(sql)
            self.connect().commit()
            return True
        except Exception as e:
            print(f"Exception: {str(e)}")
            self.log_error(f"Exception: {str(e)}")
            return None

    # Read API
    def get_pending_voucher_status(self, office_id):
        try:
            # Define the API endpoint URL

            office_id = office_id.strip()
            # self.log_info(f"Office: {office_id}")
            # print(f"Office: {office_id}")

            url = f'{self.pending_voucher_check_api}/{office_id}'

            # self.log_info(f"URL: {url}")
            # print(f"URL: {url}")

            # Make the API request
            response = requests.get(url)

            total_failed_vouchers = 0
            # Check if the request was successful (status code 200)
            if response.status_code == 200:

                # Read the response data
                data = response.json()  # Convert JSON response to Python dictionary

                # Access individual fields
                business_date = data['businessDate']
                failed_request_count = data['failedRequestCount']
                error_message = data['errorMessage']
                inprogress_request_count = data['inprogressRequestCount']

                total_failed_vouchers = int(failed_request_count) + int(inprogress_request_count)

                #print("API response:", data)
            else:
                # Print error message if request was not successful
                print("Error:", response.status_code, response.text)
                self.log_info("Error:", response.status_code, response.text)

            if (total_failed_vouchers>0):
                return True
            else:
                return False    

        except Exception as e:
            print(f"Exception: {str(e)}")
            self.log_error(f"Exception: {str(e)}")
            return None

    # Loop through office, Return office status
    def read_office(self, office_list_str):
        try:
            status = False
            office_list = office_list_str.split(',')
            for office_id in office_list:
                office_id=office_id.strip()
                status = self.get_pending_voucher_status(office_id)
                if status is True:
                    break
            
            if (status is True):
                self.has_pending_voucher="YES"
            else:
                self.has_pending_voucher="NO"

            return status
        
        except Exception as e:
            # self.has_pending_voucher="ERROR"
            print(f"Exception: {str(e)}")
            self.log_error(f"Exception: {str(e)}")
            return None
        
# Operation Details
class OperationDetailData(Logger):
    def __init__(self, logfile, OperationDetailObj):
        super().__init__(logfile)
        self.db = get_variables().AUTOMATION_DB
        self.operation_id =  OperationDetailObj.operation_id
        self.task_id=OperationDetailObj.task_id
        self.task_name = OperationDetailObj.task_name
        self.task_description = OperationDetailObj.task_description
        self.task_start_datetime = OperationDetailObj.task_start_datetime
        self.task_end_datetime = OperationDetailObj.task_end_datetime
        self.task_duration = OperationDetailObj.task_duration
        self.task_status = OperationDetailObj.task_status
        self.remarks = OperationDetailObj.remarks
        
    def connect(self):
        try:
            connection = sqlite3.connect(self.db)

            connection.isolation_level = None

            return connection  # Success
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error
             
    #@staticmethod
    def create(self):
        try:
            sql = f"""INSERT INTO operation_details (operation_id, task_id, task_name, task_description, task_start_datetime, task_end_datetime, task_duration, task_status, remarks)
              VALUES  ('{self.operation_id}', {self.task_id}, '{self.task_name}', '{self.task_description}', '{self.task_start_datetime}', '{self.task_end_datetime}', '{self.task_duration}', '{self.task_status}', '{self.remarks}');
"""
            #print(sql)
            cursor = self.connect().cursor()
            cursor.execute(sql)
            self.connect().commit()

            return True
        except Exception as e:
            print(f"Exception: {str(e)}")
            self.log_error(f"Exception: {str(e)}")
            return False
    
    #@staticmethod
    def read_all(self):
        try:
            cursor = self.connect().cursor()
            cursor.execute("SELECT * FROM operation_details")
            rows = cursor.fetchall()
            return [OperationDetail(*row) for row in rows]
        except Exception as e:
            print(f"Exception: {str(e)}")
            self.log_error(f"Exception: {str(e)}")
            return None
    
    #@staticmethod
    def read_by_id(self, id):
        try:
            cursor = self.connect().cursor()
            cursor.execute(f"SELECT * FROM operation_details WHERE operation_id='{id}'")
            rows = cursor.fetchall()
            return [OperationDetail(*row) for row in rows]
        except Exception as e:
            print(f"Exception: {str(e)}")
            self.log_error(f"Exception: {str(e)}")
            return None
        
    #@staticmethod
    def update(self):
        try:
            if (self.operation_id==None):
                raise Exception("Id should be empty!")
            
            sql_upd_col = "UPDATE operation_details SET "
            sql_where = f" WHERE operation_id='{self.operation_id}' AND tasK_id='{self.task_id}';"
            col_no = 0

            if (self.task_id!=None):
                if (col_no==0):
                    sql_upd_col = sql_upd_col + f" task_id={self.task_id}"
                else:
                    sql_upd_col = sql_upd_col + f", task_id={self.task_id}"
                col_no = col_no +1

            if (self.task_name!=None):
                if (col_no==0):
                    sql_upd_col = sql_upd_col + f" task_name='{self.task_name}'"
                else:
                    sql_upd_col = sql_upd_col + f",  task_name='{self.task_name}'"
                col_no = col_no +1

            if (self.task_description!=None):
                if (col_no==0):
                    sql_upd_col = sql_upd_col + f" task_description='{self.task_description}'"
                else:
                    sql_upd_col = sql_upd_col + f", task_description='{self.task_description}'"
                col_no = col_no +1

            if (self.task_start_datetime!=None):
                if (col_no==0):
                    sql_upd_col = sql_upd_col + f" task_start_datetime='{self.task_start_datetime}'"
                else:
                    sql_upd_col = sql_upd_col + f", task_start_datetime='{self.task_start_datetime}'"
                col_no = col_no +1

            if (self.task_end_datetime!=None):
                if (col_no==0):
                    sql_upd_col = sql_upd_col + f" task_end_datetime='{self.task_end_datetime}'"
                else:
                    sql_upd_col = sql_upd_col + f", task_end_datetime='{self.task_end_datetime}'"
                col_no = col_no +1

            if (self.task_duration!=None):
                if (col_no==0):
                    sql_upd_col = sql_upd_col + f" task_duration='{self.task_duration}'"
                else:
                    sql_upd_col = sql_upd_col + f", task_duration='{self.task_duration}'"
                col_no = col_no +1

            if (self.task_status!=None):
                if (col_no==0):
                    sql_upd_col = sql_upd_col + f" task_status='{self.task_status}'"
                else:
                    sql_upd_col = sql_upd_col + f", task_status='{self.task_status}'"
                col_no = col_no +1

            if (self.remarks!=None):
                if (col_no==0):
                    sql_upd_col = sql_upd_col + f" remarks='{self.remarks}'"
                else:
                    sql_upd_col = sql_upd_col + f", remarks='{self.remarks}'"
                col_no = col_no +1

            # concat
            sql = sql_upd_col +" "+ sql_where
            #print(sql)
            #self.log_info(sql)
            
            cursor = self.connect().cursor()
            cursor.execute(sql)

            return True
        except Exception as e:
            print(f"Exception: {str(e)}")
            self.log_error(f"Exception: {str(e)}")
            return None

    #@staticmethod
    def delete(self):
        try:
            cursor = self.connect().cursor()
            sql = f"DELETE FROM operation_details WHERE operation_id='{self.operation_id}' AND tasK_id='{self.task_id}';"
            cursor.execute(sql)
            self.connect().commit()
            return True
        except Exception as e:
            print(f"Exception: {str(e)}")
            self.log_error(f"Exception: {str(e)}")
            return None

#Read Operation DB ******************************************************************************
class read_operation_db:
    def __init__(self, operation_id) -> None:
        self.operation_id = operation_id
        self.db = get_variables().AUTOMATION_DB

    def connect(self):
        try:
            connection = sqlite3.connect(self.db)
            connection.isolation_level = None

            print(f"Connected to {self.db}")

            return connection  # Success
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error
        
    # Read operation master info
    def read_operation_master(self):
        try:
            sql = f"SELECT * FROM operation WHERE operation_id='{self.operation_id}'"
            cursor = self.connect().cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            return [OperationMaster(*row) for row in rows]
        except Exception as e:
            print(f"Exception: {str(e)}")
            #self.log_error(f"Exception: {str(e)}")
            return None

    # read operation detail info
    def read_operation_detail(self):
        try:
            cursor = self.connect().cursor()
            cursor.execute(f"SELECT * FROM operation_details WHERE operation_id='{self.operation_id}'")
            rows = cursor.fetchall()
            return [OperationDetail(*row) for row in rows]
        except Exception as e:
            print(f"Exception: {str(e)}")
            #self.log_error(f"Exception: {str(e)}")
            return None

# Operation DB Wraper Class **************************************************************************
class operation_db:
    def __init__(self, operation_log, operation_master, task_lst):
        self.operation_log = operation_log
        self.operation_master = operation_master 
        self.task_lst = task_lst
        self.operation_detail_lst = []

    # Initialize operation database
    def setup_operation_database(self):
        try:
            current_datetime = self.operation_master.start_datetime
            
            operation_id=self.operation_master.operation_id
            #operation_obj = OperationMaster(operation_id=self.operation_id, operation_log=self.operation_log, self.start_datetime=current_datetime, end_datetime=None, total_duration=None, operation_status="In-Progress", source_database_vm_name=source_vm_name, source_database_vm_ip=source_database_vm_ip, total_tasks=total_tasks, total_passed_tasks=0, output_dump_file=None, output_data_disk_snapshot=None)
            operation_master_data = OperationMasterData(logfile=self.operation_log, 
                                                        OperationMasterObj=self.operation_master)
            result= operation_master_data.create()

            if (result==False):
                raise Exception("Unable to save operational master information into database!")
            

            for task in self.task_lst:
                operation_detail_obj = OperationDetail(operation_id=operation_id, 
                                                       task_id=task.task_no, 
                                                       task_name=task.task_name, 
                                                       task_description=task.task_description, 
                                                       task_start_datetime=None, 
                                                       task_end_datetime=None, 
                                                       task_duration=None, 
                                                       task_status="Not Started", 
                                                       remarks=None)
                # Append into List
                self.operation_detail_lst.append(operation_detail_obj)

                operation_detail_data = OperationDetailData(logfile=self.operation_log, 
                                                            OperationDetailObj=operation_detail_obj)
                result= operation_detail_data.create()
                if (result==False):
                    raise Exception("Unable to save operational detail information into database!")
            
            return True
        except Exception as e:
            print(f"Exception: {str(e)}")
            return None

    def update_operation_master(self):
        try:
            operation_master_data = OperationMasterData(logfile=self.operation_log, 
                                                        OperationMasterObj=self.operation_master)
            status = operation_master_data.update()
            
            return status
        except Exception as e:
            print(f"Exception: {str(e)}")
            return None
    
    def has_pending_voucher(self, office_list):
        try:
            operation_master_data = OperationMasterData(logfile=self.operation_log, 
                                                        OperationMasterObj=self.operation_master)
            
            status = operation_master_data.read_office(office_list_str=office_list)
            
            return status
        except Exception as e:
            print(f"Exception: {str(e)}")
            return None
        
    def update_operation_detail(self, OperationDetail):
        try:
            operation_detail_data = OperationDetailData(
                logfile=self.operation_log,
                OperationDetailObj=OperationDetail
            )
            status = operation_detail_data.update()
            
            return status
        except Exception as e:
            print(f"Exception: {str(e)}")
            return None
        
# if __name__ == "__main__":

#     operation_log="logs\log_5af7fd65-8edd-444d-ab1d-68909f8bda0a_2023-10-23_19-18-34.log"

    # Defind SQL execution Instance
    # operation_obj = OperationMaster(operation_id="5af7fd65-8edd-444d-ab1d-68909f8bda0a", start_datetime="2023-10-23 23:19:34", end_datetime="2023-10-23 23:19:34", total_duration="", operation_status="Inprogress", source_database_vm_name="VM1", source_database_vm_ip="10.10.01.01", total_tasks=26, total_passed_tasks="", output_dump_file="", output_data_disk_snapshot="")
    # operation_master_data = OperationMasterData(logfile=operation_log, OperationMasterObj=operation_obj)
    # result= operation_master_data.update(id="5af7fd65-8edd-444d-ab1d-68909f8bda0a", end_datetime="2023-11-08 17:00:00", total_duration="08:00:10")
    # print(result)
    # result = operation_master_data.read_by_id(id="5af7fd65-8edd-444d-ab1d-68909f8bda0a")
    # for r in result:
    #     print(r.total_duration)

    # result= operation_master_data.delete(id="5af7fd65-8edd-444d-ab1d-68909f8bda0a")
                                         
    # operation_detail_obj = OperationDetail(operation_id="5af7fd65-8edd-444d-ab1d-68909f8bda0a", task_id="1", task_name="Connect_Google_Cloud", task_description="Test Google cloud connection", task_start_datetime="2023-11-08 28:00:00", task_end_datetime="2023-11-08 28:00:00", task_duration="09:00:00", task_status="Pedning", remarks=None)
    # operation_detail_data = OperationDetailData(logfile=operation_log, OperationDetailObj=operation_detail_obj)
    # result=operation_detail_data.update(id="5af7fd65-8edd-444d-ab1d-68909f8bda0a", task_name="Connect_Google_Cloud2")
    # print(result)
    # result = operation_detail_data.read_by_id(id="5af7fd65-8edd-444d-ab1d-68909f8bda0a")
    # for r in result:
    #     print(r.task_name)

