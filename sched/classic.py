import sys

def avg(L):
    return sum(L) / len(L)

class ClassicBound(object):
    def __init__(self, task_set):
        self.task_set = task_set
        self.core_num = 1
        self.critical_workload = 0.0
        self.total_workload = 0.0

    def __str__(self):
        return ''

    def calculate_critical_path(self):
        pass

    def calculate_bound(self):
        self.calculate_critical_path()
        return self.critical_workload + (self.total_workload - self.critical_workload) / self.core_num