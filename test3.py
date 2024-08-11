from execute_sql import *
from shell_execute import *
from vm_operation import *
from gcs_operation import *
from init_variables import get_variables
from timeit import default_timer as timer

operation_log = "logs\log_cd5ddbc6-6161-4a70-ac6b-952e4643e12c_2024-05-06_13-42-03.log"

# Google cloud compute Instance
gcp_compute_instance = VMoperation(operation_log)

snapshot_prefix=f"{get_variables().SNAPSHOT_PREFIX}-smalldb"
snapshot_retention_days = get_variables().SNAPSHOT_RETENTION_DAYS
disk_snapshot_deletion_status=gcp_compute_instance.delete_old_snapshots(prefix=snapshot_prefix, days_old=snapshot_retention_days)
if (disk_snapshot_deletion_status==True):
    print("Older snapshot has been removed.")
else:
    print("Unable to remove disk snapshots!")