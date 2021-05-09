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
        DAG = DAGGen(**dag_param_1)
        # DAG = DAGFile('./input1.txt')

        classic = ClassicBound(DAG.task_set, core_num=4)
        naive = NaiveBound(DAG.task_set, core_num=4)
        # print(DAG)

        classic_bound = classic.calculate_bound()
        naive_bound = naive.calculate_bound()

        # print("classic bound: ", classic_bound)
        # print("naive bound: ", naive_bound)

        # Generate Graph for CPC Model
        dag_param_cpc = {
            "node_num": [20, 0],
            "depth": [5.0, 0],
            "exec_t": [50.0, 30.0],
            "start_node": [1, 0],
            "end_node": [1, 0],
            "extra_arc_ratio": 0.2
        }
        CPC_DAG = DAGGen(**dag_param_cpc)
        print(CPC_DAG)
        cpc_model = CPCBound(CPC_DAG.task_set, 2)

        # cpc_model.calculate_cpc_based_response_time_bound()
