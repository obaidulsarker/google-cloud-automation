import psycopg2
from psycopg2 import OperationalError
import xml.etree.ElementTree as ET
from init_variables import get_variables
from logger import *
from task import *
import time
from shell_execute import *

class DatabaseExecutor(Logger):
    def __init__(self, logfile):
        super().__init__(logfile)
        self.database = get_variables().DB_NAME
        self.user = get_variables().DB_USER
        self.password = get_variables().DB_USER_PASS
        self.secret_value = get_variables().SECRET_VALUE
        self.host = get_variables().FIXED_ID
        self.port = get_variables().DB_PORT

        self.sql_xml_file = get_variables().SQL_XML_FILE
        self.task_xml_file = get_variables().TASK_XML_FILE
        self.pg_conf_xml_file = get_variables().PG_CONF_XML_FILE
        self.operation_log = logfile

    def connect(self):
        try:
            connection = psycopg2.connect(
                database=self.database,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                connect_timeout = 3600
            )
            connection.autocommit=True

            return connection  # Success
        except OperationalError as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error

    def test_connection(self):
        try:
            connection = psycopg2.connect(
                database=self.database,
                user=self.user,
                password=self.secret_value,
                host=self.host,
                port=self.port
            )
            return True  # Success
        except OperationalError as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return False  # Error
        
    def disconnect(self):
        if self.connect():
            self.connect().close()

    def execute_ddl(self, query):
        try:
            print(f"Executing : {query}")
            self.log_info(f"Executing : {query}")
            cursor = self.connect().cursor()
            cursor.execute(query)
            #self.connection.commit()
            cursor.close()
            self.log_info(f"Executed : {query}")
            print(f"Executed : {query}")
            return True  # Success
        except OperationalError as e:
            print(f"DDL Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return False  # Error

    def execute_dml(self, query):
        try:
            self.log_info(f"Executing : {query}")
            print(f"Executing : {query}")
            cursor = self.connect().cursor()
            cursor.execute(query)
            #self.connection.commit()
            cursor.close()
            self.log_info(f"Executed : {query}")
            print(f"Executed : {query}")
            return True  # Success
        except OperationalError as e:
            print(f"DML Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return False  # Error

    def execute_procedure(self, query):
        try:
            self.log_info(f"Executing : {query}")
            print(f"Executing : {query}")
            cursor = self.connect().cursor()
            cursor.execute(query)
            #result = cursor.fetchone()
            #self.connection.commit()
            cursor.close()
            self.log_info(f"Executed : {query}")
            print(f"Executed : {query}")
            # Print the result if applicable
            # if result:
            #     self.log_info(result[0])

            return True  # Success
        except OperationalError as e:
            print(f"Procedure Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return False  # Error
        
    def execute_function(self, query):
        try:
            self.log_info(f"Executing : {query}")
            print(f"Executing : {query}")
            cursor = self.connect().cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            #self.connection.commit()
            cursor.close()
            self.log_info(f"Executed : {query}")
            print(f"Executed : {query}")
            # Print the result if applicable
            if result:
                self.log_info(result[0])

            return True  # Success
        except OperationalError as e:
            print(f"Function Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return False  # Error
                
    def get_schemas_to_drop(self):
        try:
            cursor = self.connect().cursor()
            # Get a list of all user schemas
            sql ="""
                SELECT schema_name FROM information_schema.schemata 
                WHERE schema_name NOT LIKE 'pg_%' 
                AND schema_name NOT IN ('sys','information_schema','pg_catalog','public', 'archive') 
                """
            cursor.execute(sql)
            schemas = cursor.fetchall()

            # Extract schema names from the result
            schema_list = [row[0] for row in schemas]

            # Close the cursor and the connection
            cursor.close()

            # Return schema list
            return schema_list
        
        except OperationalError as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return []

    def drop_schemas(self):
        try:
            # Connect to the PostgreSQL database
            cursor = self.connect().cursor()

            # Schema List
            schemas_to_drop = self.get_schemas_to_drop()

            # Drop the specified schemas
            for schema in schemas_to_drop:
                #print(schema)
                sql = f"DROP SCHEMA IF EXISTS {schema} CASCADE;"
                self.log_info(f"Executing : {sql}")
                print(f"Executing : {sql}")
                cursor.execute(sql)
                self.log_info(f"Executed: {sql}")
                print(f"Executed: {sql}")
                time.sleep(5)
                
            # Commit the transaction
            #self.connection.commit()

            # Close the cursor and the connection
            cursor.close()

            return True  # Success

        except OperationalError as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return False  # Error
    
    # Return Table counts
    def count_tables_in_schema(self, schema_name):
        try:
            # Connect to the PostgreSQL database
            cursor = self.connect().cursor()

            # SQL query to count tables in the specified schema
            query = f"""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = %s
            """
            
            cursor.execute(query, (schema_name,))
            count = int(cursor.fetchone()[0])

            print(f" # of tables in {schema_name} schema: {count}")
            self.log_info(f" # of tables in {schema_name} schema: {count}")

            return count

        except Exception as e:
            print(f"Error: {str(e)}")
            self.log_error(f"Error: {str(e)}")

            return None


    # Execute sql command in shell
    def execute_sql_shell(self, query):
        try:
            print(f"Executing : {query}")
            self.log_info(f"Executing : {query}")
            # Shell Executor
            sh = ShellOperation(self.operation_log)
            result = sh.execute_stored_procedure_function(sql_exec_command=query)
            print(f"Executed : {query}")
            self.log_info(f"Executed : {query}")

            return result
        
        except OperationalError as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return False  # Error

    # Execute shell command
    def execute_shell_command(self, shell_command):
        try:
            # Shell Executor
            sh = ShellOperation(self.operation_log)
            time.sleep(300)
            result = sh.shell_command(command=shell_command)
            time.sleep(120)
            return result
        
        except OperationalError as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return False  # Error    

    # Execute all SQL statements from XML - DDL, DML, Procedure, Functions
    def execute_sql_xml(self):

        xml_file=self.sql_xml_file

        # Parse the XML file
        tree = ET.parse(xml_file)
        root = tree.getroot()
        status = True # 0 = Success, 1 = Failed

        # Iterate through the queries and execute them
        for query in root.findall(".//query"):
            query_name = query.get("name").strip()
            query_type = query.get("exec_type").strip()
            sql = query.text.strip()

            status = None
            if query_name=="dml":
                try:
                    if (query_type=="sql"):
                        status = self.execute_dml(sql)
                    else:
                        status = self.execute_sql_shell(sql)

                except Exception as e:
                    self.log_error(f"Exception: {str(e)}")
                    status=False
            elif query_name=="ddl":
                try:
                    if (query_type=="sql"):
                        status = self.execute_dml(sql)
                    else:
                        status = self.execute_sql_shell(sql)
                except Exception as e:
                    self.log_error(f"Exception: {str(e)}")
                    status=False
            elif query_name=="procedure":
                try:
                    time.sleep(900) # 15 minutes waiting
                    if (query_type=="sql"):
                        status = self.execute_dml(sql)
                    else:
                        status = self.execute_sql_shell(sql)
                except Exception as e:
                    self.log_error(f"Exception: {str(e)}")
                    status=False
            elif query_name=="function":
                try:
                    time.sleep(120)
                    if (query_type=="sql"):
                        status = self.execute_dml(sql)
                    else:
                        status = self.execute_sql_shell(sql)
                except Exception as e:
                    self.log_error(f"Exception: {str(e)}")
                    status=False

            elif query_name=="shell":
                try:
                    time.sleep(120)
                    status = self.execute_shell_command(sql)
                except Exception as e:
                    self.log_error(f"Exception: {str(e)}")
                    status=False

            time.sleep(30)

        return status
    
    # Disable Archive mode and Sync
    def disable_archive_mode(self):
        try:
            sql =f"ALTER SYSTEM SET archive_mode = 'off';"
            status = self.execute_ddl(sql)
            sql =f"ALTER SYSTEM SET synchronous_standby_names = '';"
            status = self.execute_ddl(sql)
            sql =f"ALTER SYSTEM SET synchronous_commit = 'off';"
            status = self.execute_ddl(sql)

            return status
        except Exception as e:
            print(f"Exception: {str(e)}")
            self.log_error(f"Exception: {str(e)}")
            return False

    # Set Paramters in postgresql.conf file
    def execute_db_configuration_xml(self, server_mem_size_gb, server_cpu_core):
        xml_file=self.pg_conf_xml_file

        # Parse the XML file
        tree = ET.parse(xml_file)
        root = tree.getroot()
        status = -1 # 0 = Success, 1 = Failed

        # Read the parameters and values from the XML file
        # Memory dependent parameters
        for param_elem in root.findall(".//variables/memory/param"):
            param_name = param_elem.get("name").strip()
            param_value = param_elem.text.strip()
            print(f"parma_name={param_name}, param value={param_value}")
            self.log_info(f"parma_name={param_name}, param value={param_value}")
            percent_value = float(param_value)

            param_value_int=int(percent_value * server_mem_size_gb)
            param_value_db = f"{param_value_int}GB"
            sql =f"ALTER SYSTEM SET {param_name} = '{param_value_db}';"  
            try:
               status = self.execute_ddl(sql)
            except Exception as e:
                self.log_error(f"Exception: {str(e)}")    
                status=False

        # CPU dependent parameters
        for param_elem in root.findall(".//variables/cpu/param"):
            param_name = param_elem.get("name").strip()
            param_value = param_elem.text.strip()
            print(f"parma_name={param_name}, param value={param_value}")
            self.log_info(f"parma_name={param_name}, param value={param_value}")
            percent_value = float(param_value)

            param_value_int=int(percent_value * server_cpu_core)
            param_value_db = f"{param_value_int}"
            sql =f"ALTER SYSTEM SET {param_name} = '{param_value_db}';"  
            try:
               status = self.execute_ddl(sql)
            except Exception as e:
                self.log_error(f"Exception: {str(e)}")
                status=False

        # Fixed parameters
        for param_elem in root.findall(".//fixed/param"):
            param_name = param_elem.get("name").strip()
            param_value = param_elem.text.strip()
            print(f"parma_name={param_name}, param value={param_value}")
            self.log_info(f"parma_name={param_name}, param value={param_value}")

            sql =f"ALTER SYSTEM SET {param_name} = '{param_value}';"  
            try:
               status = self.execute_ddl(sql)
            except Exception as e:
                self.log_error(f"Exception: {str(e)}")
                status=False

        return status
    
    # Read all tasks from xml file and load into task LIST and return a list
    def get_task_list(self):
        try:
            xml_file=self.task_xml_file

            #print(xml_file)

            # Parse the XML file
            tree = ET.parse(xml_file)
            root = tree.getroot()
            

            # Create a list to store Task objects
            task_list = []

            # Iterate through the queries and execute them
            for query in root.findall(".//task"):
                task_no = query.get("taskno")
                task_name = query.get("name")
                task_status = query.get("status")
                task_description = query.text.strip()
                task = Task(taskno=task_no,taskname=task_name,status=task_status,task_description=task_description)
                task_list.extend([task])
                #print (task)
  
            return task_list
        
        except Exception as e:
            self.log_error(f"Exception: {str(e)}")
            print (f"Exception: {str(e)}")
            return None

# if __name__ == "__main__":

#     operation_log="logs\log_5af7fd65-8edd-444d-ab1d-68909f8bda0a_2023-10-23_19-18-34.log"

#     # Defind SQL execution Instance
#     sql_instance = DatabaseExecutor(operation_log)

#     result = sql_instance.test_connection()
#     print(f"Result = {result}")
