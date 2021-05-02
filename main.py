import argparse
import os
import csv

from task_gen.dag_gen import Task, DAGGen
# from sched.classic import ClassicBound()

BATCH_SIZE = 1

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--log', '-l', help='log index', default=-1)
    parser.add_argument('--debug', '-d', help='debug index', default=-1)
    args = parser.parse_args()

    for i in range(BATCH_SIZE):
        ## 2. Generate Graph for one DAG
        dag_param_1 = {
            "task_num" : [20, 0],
            "depth" : [4.5, 0.5],
            "exec_t" : [50.0, 30.0],
            "start_node" : [2, 1],
            "extra_arc_ratio" : 0.4
        }

        Task.idx = 0
        DAG = DAGGen(**dag_param_1)
