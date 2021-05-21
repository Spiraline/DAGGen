import sys
import math

def avg(L):
    return sum(L) / len(L)

def argmax(value_list, index_list=None):
    if index_list is None :
        index_list = list(range(len(value_list)))
    max_index, max_value = [index_list[0], value_list[index_list[0]]]
    for i in index_list :
        if value_list[i] > max_value :
            max_index = i
            max_value = value_list[i]
    return max_index

class ClassicBound(object):
    def __init__(self, task_set, core_num=1):
        self.task_set = task_set
        self.core_num = core_num
        self.critical_workload = 0.0
        self.total_workload = 0.0
        self.critical_path = self.calculate_critical_path()

    def __str__(self):
        return ''

    def calculate_critical_path(self):
        """
            calculate critical path and update 'self.total_workload' and 'self.critical_workload' 
        """
        distance = [0,] * len(self.task_set)
        indegree = [0,] * len(self.task_set)
        task_queue = []

        for i in range(len(self.task_set)) :
            if self.task_set[i].level == 0 :
                task_queue.append(self.task_set[i])
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

        cp = []
        cv = argmax(distance)

        while True :
            cp.append(cv)
            if len(self.task_set[cv].parent) == 0 :
                break
            cv = argmax(distance, self.task_set[cv].parent)
        
        cp.reverse()
        return cp

    def calculate_bound(self):
        critical_workload = 0 ; total_workload = 0

        for i in range(len(self.task_set)) :
            total_workload += self.task_set[i].exec_t
            if i in self.critical_path :
                critical_workload += self.task_set[i].exec_t

        return critical_workload + math.floor((total_workload - critical_workload) / self.core_num)
    
    def calculate_budget(self, sl_idx, deadline, cpu_num) : 
        critical_workload = 0 ; total_workload = 0

        for i in range(len(self.task_set)) :
            if i == sl_idx :
                continue
            total_workload += self.task_set[i].exec_t
            if i in self.critical_path :
                critical_workload += self.task_set[i].exec_t

        return deadline - critical_workload - (total_workload-critical_workload) / cpu_num

class ClassicBackup(ClassicBound) :
    def __init__(self, dag, core_num=1):
        self.dag = dag
        self.task_set = dag.task_set
        self.core_num = core_num
        self.critical_workload = 0.0
        self.total_workload = 0.0
        self.critical_path = []
        critical_path = self.calculate_critical_path()

        for c in critical_path :
            if c not in dag.dangling_dag :
                self.critical_path.append(c)
        self.critical_path.append(len(dag.task_set))

    def calculate_bound(self) :
        critical_workload = self.dag.backup ; total_workload = self.dag.backup

        for i in range(len(self.task_set)) :
            if i in self.dag.dangling_dag :
                continue
            total_workload += self.task_set[i].exec_t
            if i in self.dag.critical_path :
                critical_workload += self.task_set[i].exec_t

        return critical_workload + math.floor((total_workload - critical_workload) / self.core_num)

    def calculate_budget(self, sl_idx, deadline, cpu_num) :
        critical_workload = 0 ; total_workload = 0
        for i in range(len(self.task_set)) :
            if i in self.dag.dangling_dag or i == sl_idx :
                continue
            total_workload += self.task_set[i].exec_t
            if i in self.dag.critical_path :
                critical_workload += self.task_set[i].exec_t

        return deadline - critical_workload - (total_workload-critical_workload)/cpu_num - self.dag.backup