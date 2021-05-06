import sys

def argmax(value_list, index_list=None):
    if index_list is None :
        index_list = list(range(len(value_list)))
    max_index, max_value = [index_list[0], value_list[index_list[0]]]
    for i in index_list :
        if value_list[i] > max_value :
            max_index = i
            max_value = value_list[i]
    return max_index

def inter(original_list, new_list) :
    return list(set(original_list) & set(new_list))

class NaiveBound(object) :
    def __init__(self, task_set, core_num=1):
        self.task_set = task_set
        self.core_num = core_num
        self.critical_workload = 0.0
        self.total_workload = 0.0

    def __str__(self):
        return ''

    def calculate_critical_path(self, vl):
        distance = [0,] * len(self.task_set)
        indegree = [0,] * len(self.task_set)
        task_queue = []

        for i, v in enumerate(self.task_set) :
            indegree[i] = len(inter(v.parent, vl))

        for i in vl:
            if indegree[i] == 0 :
                task_queue.append(self.task_set[i])
                distance[i] = self.task_set[i].exec_t

        while task_queue :
            vertex = task_queue.pop(0)
            for v in inter(vertex.child, vl) :
                distance[v] = max(self.task_set[v].exec_t + distance[vertex.tid], distance[v]) 
                indegree[v] -= 1
                if indegree[v] == 0 :
                    task_queue.append(self.task_set[v])    
        cp = []
        cv = argmax(distance)

        while True :
            cp.append(cv)
            if len(inter(self.task_set[cv].parent, vl)) == 0 :
               break
            cv = argmax(distance, inter(self.task_set[cv].parent, vl))

        cp.reverse()
        return cp

    def set_exec_range(self, cp, vl) :
        task_len = len(self.task_set)
        cp_len = len(cp)
        queue = []
        
        # init
        # exec_range = [[2, cp_len-1] for _ in range(task_len)]
        exec_range = [[1, cp_len] for _ in range(task_len)]
        for i, idx in enumerate(cp) :
            exec_range[idx] = [i+1, i+1]

        # start time
        for i in range(1, cp_len-1) : 
            queue.clear()
            visited = [False for _ in range(task_len)]

            queue.extend(inter(self.task_set[cp[i-1]].child, vl))
            while queue :
                vertex = queue.pop(0)
                exec_range[vertex][0] = i+1
                visited[vertex] = True
                for v in inter(self.task_set[vertex].child, vl) :
                    if not visited[v] :
                        queue.append(v)

        # finish time
        for i in range(cp_len, 1, -1) :
            queue.clear()
            visited = [False for _ in range(task_len)]

            queue.extend(inter(self.task_set[cp[i-1]].parent, vl))
            while queue :
                vertex = queue.pop(0)
                exec_range[vertex][1] = i-1
                visited[vertex] = True
                for v in inter(self.task_set[vertex].parent, vl) :
                    if not visited[v] :
                        queue.append(v)

        for i, idx in enumerate(cp) :
            exec_range[idx] = [i+1, i+1]


        # resolve dependency
        queue.clear()
        for v in vl :
            if len(inter(self.task_set[v].parent, vl)) == 0 :
                queue.append(v)
        
        while queue :
            p = queue.pop(0)
            for c in inter(self.task_set[p].child, vl) :
                ps, pe = exec_range[p]
                cs, ce = exec_range[c]
                if (ps!=cs or pe!=ce) and ((ps <= cs) and (pe <= ce)) and (pe >= cs) and (p not in cp and c not in cp) :
                    exec_range[c][0] = exec_range[p][1]
                queue.append(c)

        # assign task to matrix
        group = [[[] for _ in range(cp_len+1)] for _ in range(cp_len+1)]

        for v in vl :
            if v not in cp :
                start, end = exec_range[v]
                group[start][end].append(v)
        
        return group

    def recursive_bound(self, vertex_list, core_num) :
        if (len(vertex_list) <= 1) or (core_num <= 1): # base case
            return sum([self.task_set[v].exec_t for v in vertex_list])
            
        critical_path = self.calculate_critical_path(vertex_list)
        # print(vertex_list, critical_path)
        group = self.set_exec_range(critical_path, vertex_list)

        response_time = 0
        remain = [self.task_set[cp].exec_t for cp in critical_path]
        assigned = [[] for _ in critical_path]
        remain.insert(0, 0) ; assigned.insert(0, [])

        # recursively compute all group's response time with m-1 core
        group_rt = [[0 for _ in range(len(critical_path)+1)] for _ in range(len(critical_path)+1)]
        for i in range(len(critical_path)+1) :
            for j in range(i, len(critical_path)+1) :
                if len(group[i][j]) != 0 :
                    group_rt[i][j] = self.recursive_bound(group[i][j], core_num-1)

        for theta in range(1, len(critical_path)+1) :
            # sholud assign here
            for start in range(theta, 0, -1) :
                if group_rt[start][theta] != 0 :
                    remain[theta] -= group_rt[start][theta]
                    assigned[theta].append(group_rt[start][theta])
                    group_rt[start][theta] = 0

            # could be assigned
            for start, end in [(x, y) for y in range(theta+1, len(critical_path)) for x in range(len(critical_path), theta, -1)] :
                if (group_rt[start][end] == 0) :
                    continue

                if (group_rt[start][end] > remain[theta]) : # break 시키는게 맞을지 ?
                    break

                remain[theta] -= group_rt[start][end]
                assigned[theta].append(group_rt[start][end])
                group_rt[start][end] = 0
        
        # unassigned group
        for i in range(len(critical_path)+1) :
            for j in range(i, len(critical_path)+1) :
                if group_rt[i][j] != 0 :
                    bestfit = argmax(remain[i:j+1])
                    remain[bestfit+i] -= group_rt[i][j]
                    assigned[bestfit+i].append(group_rt[i][j])
                    group_rt[i][j] = 0

        # accumulate response time for all theta
        for i, c in enumerate(critical_path) :
            # print(self.task_set[c].exec_t, assigned[i])
            response_time += max(self.task_set[c].exec_t, sum(assigned[i])) # theta value or hidden terms

        return response_time

    def calculate_bound(self, vertex_list=None):
        return self.recursive_bound(list(range(0, len(self.task_set))), self.core_num)
