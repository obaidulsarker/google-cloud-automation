class Task:
    def __init__(self, taskno, taskname, status, task_description):
        self.task_no = taskno
        self.task_name = taskname
        self.task_status = status
        self.task_description = task_description

    def __str__(self):
        return f"TaskNo: {self.task_no}, TaskName: {self.task_name}, Status: {self.task_status}, Description: {self.task_description}"