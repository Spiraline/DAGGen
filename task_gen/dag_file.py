from random import randint
import sys
import os
import csv

from .dag_gen import Task

class DAGFile(object):
    def __init__(self, input_path):

        f = open(input_path, "r")
        self.node_num = int(f.readline())
        exec_t = [float(l) for l in f.readline().split(' ')]

        self.task_set = []
        self.start_node = []
        self.end_node = []

        ### 1. Initialize Task
        for i in range(self.node_num):
            task_param = {
                "name" : "node" + str(i),
                "exec_t" : exec_t[i]
            }

            self.task_set.append(Task(**task_param))

        ### 2. Add edge
        for i in range(self.node_num) :
            line = f.readline()
            if not line: break
            edge = line.strip().split(' ')
            for j, e in enumerate(edge) :
                if e == '1' :
                    self.task_set[i].child.append(j)
                    self.task_set[j].parent.append(i)
            # print(i, self.task_set[i].child)

        ### 3. Add some attribute
        for i, task in enumerate(self.task_set) :
            if len(task.parent) == 0 :
                self.start_node.append(i)
                self.task_set[i].level = 0

            if len(task.child) == 0 :
                self.task_set[i].isLeaf = True
                self.end_node.append(i)
                self.task_set[i].deadline = 0

        ### 4. Assign level (depth from source to sink)
        queue = self.start_node.copy()
        while not len(queue) == 0 :
            q = queue.pop(0)
            for qq in self.task_set[q].child :
                self.task_set[qq].level = max(self.task_set[qq].level, self.task_set[q].level+1)
                queue.append(qq)

    def __str__(self):
        print("%-9s %-5s %39s %40s %8s" % ('name', 'exec_t', 'parent node', 'child node', 'deadline'))
        for task in self.task_set:
            print(task)
        
        return ''

if __name__ == "__main__":
    dag = DAGFile("../input/input_debug_20.txt")

    print(dag)