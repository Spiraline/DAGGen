import argparse
import math
from random import uniform
from numpy.random import normal

from task_gen.self_looping import SelfLoopingDag, calculate_critical_path
from task_gen.dag_gen import DAGGen, Task
from task_gen.dag_file import DAGFile

from sched.classic import ClassicBound
from sched.cpc import CPCBound

if __name__ == '__main__' :
    ### TODO : add some arguments description
    parser = argparse.ArgumentParser(description='argparse for test')
    parser.add_argument('--test_size', type=int, help='#test case', default=100)
    parser.add_argument('--cpu_num', type=int, help='#cpu', default=4)
    parser.add_argument('--node_num', type=int, help='#node number in DAG', default=10)
    parser.add_argument('--dag_depth', type=int, help='depth of DAG', default=5)

    parser.add_argument('--node_avg', type=int, help='WCET average of nodes', default=50)
    parser.add_argument('--node_std', type=int, help='WCET std of nodes', default=10)
    parser.add_argument('--backup', type=int, help='WCET of backup task', default=100)

    parser.add_argument('--function', type=str, help='function type for score', default='log')
    parser.add_argument('--function_std', type=float, help='variance for score function', default=0.1)

    parser.add_argument('--base', type=list, help='list for value of base [small, middle, large]', default=[5, 10, 15])
    parser.add_argument('--utilization', type=float, help='', default=0.3)
    # parser.add_argument('', type=, help= , default=)
    args = parser.parse_args()

    ### experiments argument
    test_size = args.test_size
    cpu_num = args.cpu_num
    utilization = args.utilization

    ### TODO: Implement more function type and set proper acceptance bar value.
    func_std = args.function_std
    if args.function == 'log' :
        def func_iter2score(x) :
            return math.log(x+1) * normal(0, func_std, 1)
        acceptance_bar = 4
    else :
        raise NotImplementedError

    base_small, base_middle, base_large = args.base

    ### test
    dag_param = {
        "node_num": [args.node_num, 0],
        "depth": [args.dag_depth, 0],
        "exec_t": [args.node_avg, args.node_std],
        "start_node": [1, 0],
        "end_node": [1, 0],
        "extra_arc_ratio": 0.5 # TODO: parameterize or not ?
    }
    score = [0 for i in range(5)] # Classic, CPC, S, M, L
    miss = [0 for i in range(5)]

    for i in range(test_size) :
        Task.idx = 0
        ## generate random dag and transform it to self-looping dag
        dag, cp, sl_idx = SelfLoopingDag(dag_param)
        # TODO make classic instance
        cpc = CPCBound(dag.task_set, cpu_num)
        dag.task_set[sl_idx].exec_t = 0

        # dag, cp, sl_idx = SelfLoopingDag('input/input1.txt')
        # print(dag, sl_idx, dag.dangling_dag)
        deadline = (args.node_avg * args.node_num) / (cpu_num * utilization)
        print("deadline: ", deadline)
        
        ## check failure of every method(classic, CPC, 3 base)
        ### iteratively check succeed / failure of classic
        

        ### iteratively check succeed / failure of CPC
        max_budget = int(deadline) - sum(dag.task_set[c].exec_t for c in cp)
        
        print(dag)
        print("cp: ", cp)
        print("deadline: ", deadline)
        print("max_budget: ", max_budget)
        for i in range(max_budget, -1, -10) :
            dag.task_set[sl_idx].exec_t = i

            cpc.generate_interference_group()
            cpc.generate_finish_time_bound()
            cpc.get_alpha_beta()
            bound = cpc.calculate_bound()
            print("{}th bound: {}".format(i, bound))

        ### check 3 base value
        # dag.task_set[sl_idx].exec_t = func_iter2score()

    ## sum up all result