import argparse
import os

from task_gen.dag_gen import Task, DAGGen
from sched.classic import ClassicBound

BATCH_SIZE = 1

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    for i in range(BATCH_SIZE):
        ## Generate Graph for one DAG
        dag_param_1 = {
            "task_num" : [20, 0],
            "depth" : [5.0, 1.0],
            "exec_t" : [50.0, 30.0],
            "start_node" : [1, 0],
            "extra_arc_ratio" : 0.2
        }

        Task.idx = 0
        DAG = DAGGen(**dag_param_1)

        # TODO : Implement class bound algorithm
        classic = ClassicBound(DAG.task_set)
        classic_bound = classic.calculate_bound()        

        print(DAG)
        print(classic_bound)
