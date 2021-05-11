import argparse
import os

from task_gen.dag_gen import Task, DAGGen
from task_gen.dag_file import DAGFile
from sched.classic import ClassicBound
from sched.cpc import CPCBound
from sched.naive import NaiveBound

BATCH_SIZE = 1

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    for i in range(BATCH_SIZE):
        ## Generate Graph for one DAG
        dag_param_1 = {
            "node_num": [20, 0],
            "depth": [10.0, 1.0],
            "exec_t": [50.0, 30.0],
            "start_node": [1, 0],
            "end_node": [1, 0],
            "extra_arc_ratio": 0.5
        }

        Task.idx = 0
        # DAG = DAGGen(**dag_param_1)
        DAG = DAGFile('./input2.txt')
        print(DAG)

        classic = ClassicBound(DAG.task_set, core_num=3)
        cpc_model = CPCBound(DAG.task_set, core_num=3)

        classic_bound = classic.calculate_bound()
        cpc_bound = cpc_model.calculate_bound()

        print("classic bound: ", classic_bound)
        print("cpc bound: ", cpc_bound)