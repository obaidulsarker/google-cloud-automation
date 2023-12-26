import subprocess

# Define the command to execute
command = [
    'pg_dump',  # Command to execute
    '--host=remote_host',  # Remote host address
    '--port=5432',  # Port number (default is 5432)
    '--username=your_username',  # Your PostgreSQL username
    '--dbname=your_database',  # Database name to backup
    '--file=backup_file.sql',  # File to save the backup to
]

# Execute the command
process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

# Wait for the process to finish
stdout, stderr = process.communicate()

# Get the return code
return_code = process.returncode

# Print the output and error
print("Standard Output:", stdout.decode('utf-8'))
print("Standard Error:", stderr.decode('utf-8'))
print("Return Code:", return_code)

