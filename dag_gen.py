from random import randint
import sys
import os
import csv

# get [mean, stdev] and return mean +- stdev random number
def rand_uniform(arr):
    if arr[1] < 0:
        raise ValueError('should pass positive stdev : %d, %d' % (arr[0], arr[1]))

    return randint(int(arr[0] - arr[1]), int(arr[0] + arr[1]))

class Task(object):
    idx = 0
    def __init__(self, **kwargs):
        self.tid = Task.idx
        Task.idx += 1
        self.name = kwargs.get('name', '')
        self.exec_t = kwargs.get('exec_t', 30.0)

        # Assigned after DAGGen
        self.parent = []
        self.child = []
        self.isLeaf = False
        self.deadline = -1
        self.level = 0

    def __str__(self):
        # res = "%-9s exec_t : %.1f, parent : %20s child : %30s" \
        #     % ('[' + self.name + ']', self.exec_t, self.parent, self.child)

        # if self.isLeaf:
        #     res += " DL : %s" % (self.deadline)

        res = "%-9s %-5.1f %40s %40s" \
            % ('[' + self.name + ']', self.exec_t, self.parent, self.child)

        if self.isLeaf:
            res += "%7s" % (self.deadline)
        else:
            res += "   ---  "

        return res

    def new_task_set(self):
        Task.idx = 0

class DAGGen(object):
    def __init__(self, **kwargs):
        self.task_num = kwargs.get('task_num', [10, 3])
        self.depth = kwargs.get('depth', [3.5, 0.5])
        self.exec_t = kwargs.get('exec_t', [50.0, 30.0])
        self.start_node = kwargs.get('start_node', [2, 1])
        self.edge_num_constraint = kwargs.get('edge_constraint', False)

        # Use when edge_num_constraint is True
        # self.inbound_num = kwargs.get('inbound_num', [2, 0])
        self.outbound_num = kwargs.get('outbound_num', [3, 0])

        # Use when edge_num_constraint is False
        self.extra_arc_ratio = kwargs.get('extra_arc_ratio', 0.1)

        self.task_set = []

        ### 1. Initialize Task
        task_num = rand_uniform(self.task_num)
        for i in range(task_num):
            task_param = {
                "name" : "node" + str(i),
                "exec_t" : rand_uniform(self.exec_t)
            }

            self.task_set.append(Task(**task_param))

        depth = rand_uniform(self.depth)

        extra_arc_num = int(task_num * self.extra_arc_ratio)

        ### 2. Classify Task by randomly-select level
        level_arr = []
        for i in range(depth):
            level_arr.append([])

        # put start nodes in level 0
        start_node_num = rand_uniform(self.start_node)
        for i in range(start_node_num):
            level_arr[0].append(i)
            self.task_set[i].level = 0
        
        # Each level must have at least one node
        for i in range(1, depth):
            level_arr[i].append(start_node_num + i - 1)
            self.task_set[start_node_num+i-1].level = i

        # put other nodes in other level randomly
        for i in range(start_node_num + depth - 1, task_num):
            level = randint(1, depth-1)
            self.task_set[i].level = level
            level_arr[level].append(i)

        ### 3-(A). When edge_num_constraint is True
        if self.edge_num_constraint:
            ### make arc for first level
            for level in range(0, depth-1):
                for task_idx in level_arr[level]:
                    ob_num = rand_uniform(self.outbound_num)

                    child_idx_list = []

                    # if desired outbound edge number is larger than the number of next level nodes, select every node
                    if ob_num >= len(level_arr[level + 1]):
                        child_idx_list = level_arr[level + 1]
                    else:
                        while len(child_idx_list) < ob_num:
                            child_idx = level_arr[level+1][randint(0, len(level_arr[level + 1])-1)]
                            if child_idx not in child_idx_list:
                                child_idx_list.append(child_idx)
                    
                    for child_idx in child_idx_list:
                        self.task_set[task_idx].child.append(child_idx)
                        self.task_set[child_idx].parent.append(task_idx)

        ### 3-(B). When edge_num_constraint is False
        else:
            ### make arc from last level
            for level in range(depth-1, 0, -1):
                for task_idx in level_arr[level]:
                    parent_idx = level_arr[level-1][randint(0, len(level_arr[level - 1])-1)]

                    self.task_set[parent_idx].child.append(task_idx)
                    self.task_set[task_idx].parent.append(parent_idx)

            ### make extra arc
            for i in range(extra_arc_num):
                arc_added_flag = False
                while not arc_added_flag:
                    task1_idx = randint(0, task_num-1)
                    task2_idx = randint(0, task_num-1)

                    if self.task_set[task1_idx].level < self.task_set[task2_idx].level:
                        self.task_set[task1_idx].child.append(task2_idx)
                        self.task_set[task2_idx].parent.append(task1_idx)
                        arc_added_flag = True
                    elif self.task_set[task1_idx].level > self.task_set[task2_idx].level:
                        self.task_set[task2_idx].child.append(task1_idx)
                        self.task_set[task1_idx].parent.append(task2_idx)
                        arc_added_flag = True
        
        ### 5. set deadline ( exec_t avg * (level + 1)) * 2
        for task in self.task_set:
            if len(task.child) == 0:
                task.isLeaf = True
                task.deadline = self.exec_t[0] * (task.level+1) * 2

    def __str__(self):
        print("%-9s %-5s %39s %40s %8s" % ('name', 'exec_t', 'parent node', 'child node', 'deadline'))
        for task in self.task_set:
            print(task)
        
        return ''

if __name__ == "__main__":
    dag_param_1 = {
        "task_num" : [20, 0],
        "depth" : [4.5, 0.5],
        "exec_t" : [50.0, 30.0],
        "start_node" : [2, 1],
        "extra_arc_ratio" : 0.4
    }

    dag_param_2 = {
        "task_num" : [20, 0],
        "depth" : [4.5, 0.5],
        "exec_t" : [50.0, 30.0],
        "start_node" : [2, 0],
        "edge_constraint" : True,
        "outbound_num" : [2, 0]
    }

    dag = DAGGen(**dag_param_1)

    print(dag)

