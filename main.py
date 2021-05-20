import argparse
import os
import math
# from sched.priority import assign_priority

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
        DAG = DAGFile('input/success.txt')
        Task.idx = 0
        bDAG = DAGFile('input/fail.txt')
        print(DAG)
        print(bDAG)

        sl_idx = 4
        sl_exec_t = DAG.task_set[sl_idx].exec_t
        DAG.task_set[sl_idx].exec_t = 999
        bDAG.task_set[sl_idx].exec_t = 999

        classic = ClassicBound(DAG.task_set, core_num=4)
        cpc = CPCBound(DAG.task_set, core_num=4)

        classic_b = ClassicBound(bDAG.task_set, core_num=4)
        cpc_b = CPCBound(bDAG.task_set, core_num=4)

        cpc.setting_theta(sl_idx)
        cpc_b.setting_theta(sl_idx)

        deadline = 125
        DAG.task_set[sl_idx].exec_t = 0
        max_budget = (deadline - sum(DAG.task_set[c].exec_t for c in cpc.critical_path)) / sl_exec_t

        print(classic.critical_path)
        print(classic_b.critical_path)
        print(cpc.critical_path)
        print(cpc_b.critical_path)


        check = [False, False]
        loop_count = [0, 0]

        for i in range(1, math.ceil(max_budget)) :
            if not check[0] :
                classic.task_set[sl_idx].exec_t = i * sl_exec_t
                classic_bound = classic.calculate_bound()

                classic_b.task_set[sl_idx].exec_t = i * sl_exec_t
                classic_bbound = classic_b.calculate_bound()
                
                if deadline < max(classic_bound, classic_bbound) :
                    check[0] = True
                else :
                    loop_count[0] = i

            if not check[1] :
                cpc.node_set[sl_idx].exec_t = i * sl_exec_t
                cpc.generate_interference_group()
                cpc.generate_finish_time_bound()
                cpc.get_alpha_beta()
                cpc_bound = cpc.calculate_bound()

                cpc_b.node_set[sl_idx].exec_t = i * sl_exec_t
                cpc_b.generate_interference_group()
                cpc_b.generate_finish_time_bound()
                cpc_b.get_alpha_beta()
                cpc_bbound = cpc_b.calculate_bound()

                if deadline < max(cpc_bound, cpc_bbound) :
                    check[1] = True
                else :
                    loop_count[1] = i

            # print(max(classic_bound, classic_bbound), max(cpc_bound, cpc_bbound), " | ", classic_bound, classic_bbound, cpc_bound, cpc_bbound)
            # print(loop_count, '\n')

            if all(check) :
                break

        print(loop_count)