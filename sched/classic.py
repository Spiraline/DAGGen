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
        """
            calculate critical path and update 'self.total_workload' and 'self.critical_workload' 
        """
        distance = [0,] * len(self.task_set)
        indegree = [0,] * len(self.task_set)
        task_queue = []

        for i in range(len(self.task_set)): # directly appending index 0 node will be okay, but for generality
            if self.task_set[i].level == 0 :
                task_queue.append(self.task_set[0])
                distance[i] = self.task_set[i].exec_t

        for i, v in enumerate(self.task_set) :
            self.total_workload += v.exec_t 
            indegree[i] = len(v.parent)

        while task_queue :
            vertex = task_queue.pop(0)
            for v in vertex.child :
                distance[v] = max(self.task_set[v].exec_t + distance[vertex.tid], distance[v]) 
                indegree[v] -= 1
                if indegree[v] == 0 :
                    task_queue.append(self.task_set[v])    

        self.critical_workload = max(distance)

    def calculate_bound(self):
        self.calculate_critical_path()
        return self.critical_workload + (self.total_workload - self.critical_workload) / self.core_num