import sys
import argparse
import math
from random import uniform
from numpy.random import normal

from task_gen.self_looping import SelfLoopingDag
from task_gen.dag_gen import DAGGen, Task
from task_gen.dag_file import DAGFile

from sched.classic import ClassicBound, ClassicBackup
from sched.cpc import CPCBound, CPCBackup
from sched.priority import calculate_makespan

def check_count(dag, count, acceptance, deadline, cpu_num, backup=True) :
    score = 0 ; miss = 0
    dag.task_set[sl_idx].exec_t = 2 * count

    if func_count2score(count) > acceptance :
        makespan = calculate_makespan(dag, cpu_num, False)
    else :
        score = 1
        if backup :
            makespan = calculate_makespan(dag, cpu_num, True)
        else :
            makespan = calculate_makespan(dag, cpu_num, False)
            
    if makespan > deadline :
        miss = 1
    return score, miss


if __name__ == '__main__' :
    parser = argparse.ArgumentParser(description='argparse for test')
    parser.add_argument('--test_size', type=int, help='#test case', default=100)
    parser.add_argument('--cpu_num', type=int, help='#cpu', default=4)
    parser.add_argument('--node_num', type=int, help='#node number in DAG', default=40)
    parser.add_argument('--dag_depth', type=float, help='depth of DAG', default=6.5)

    parser.add_argument('--node_avg', type=int, help='WCET average of nodes', default=40)
    parser.add_argument('--node_std', type=int, help='WCET std of nodes', default=10)

    parser.add_argument('--function', type=str, help='function type for score', default='e')
    parser.add_argument('--function_std', type=float, help='variance for score function', default=0.1)
    parser.add_argument('--acceptance', type=float, help='Acceptance bar for score function', default=0.9)

    parser.add_argument('--base', type=str, help='list for value of base [small, middle, large]', default='100,200,300')
    parser.add_argument('--utilization', type=float, help='(avg execution time * node #) / (deadline * cpu #)', default=0.3)
    parser.add_argument('--dangling', type=float, help='dangling DAG node # / total node #', default=0.1)
    args = parser.parse_args()

    ### experiments argument
    test_size = args.test_size
    cpu_num = args.cpu_num
    utilization = args.utilization

    ### TODO: Implement more function type and set proper acceptance bar value.
    func_std = args.function_std
    if args.function == 'log' :
        def func_count2score(x) :
            return math.log(x+1) * normal(0, func_std, 1)
    elif args.function == 'e' :
        def func_count2score(x) :
            delta = normal(0, func_std, 1)
            return max(1 - pow(math.e, -x/50) - math.fabs(delta), 0)
    else :
        raise NotImplementedError

    base_small, base_middle, base_large = [int(b) for b in args.base.split(",")]

    ### test
    dag_param = {
        "node_num": [args.node_num, 10],
        "depth": [args.dag_depth, 1.5],
        "exec_t": [args.node_avg, args.node_std],
        "start_node": [1, 0],
        "end_node": [1, 0],
        "extra_arc_ratio": 0.0
    }
    dangling_num = math.ceil(args.node_num * args.dangling)
    ### TODO: add variance to backup node
    acceptance = args.acceptance

    score = [0 for i in range(5)] # Classic, CPC, S, M, L
    miss = [0 for i in range(5)]
    critical = [0 for i in range(5)]
    continued = 0

    # file_name = "ratio_{}.txt".format(int(float(args.dangling)*100))
    # file_name = "utilization_{}.txt".format(int(float(args.utilization)*100))
    file_name = 'cpc_test.txt'
    f = open(file_name, 'w')

    j = 0
    while j < test_size :
        try :
            Task.idx = 0
            
            dag, cp, sl_idx = SelfLoopingDag(dag_param, dangling_num)
            dag.backup = args.node_avg * math.ceil(len(dag.dangling_dag)*0.8)

            classic = ClassicBound(dag.task_set, cpu_num)
            classic_b = ClassicBackup(dag, cpu_num)
            cpc = CPCBound(dag.task_set, cpu_num)
            cpc_b = CPCBackup(dag, cpu_num)
            
            cpc.setting_theta(sl_idx)
            cpc_b.setting_theta(cpc_b.cvt(sl_idx))
            sl_exec_t = 2.0

            deadline = int((args.node_avg * args.node_num) / (cpu_num * utilization))
            
            ## check failure of every method(classic, CPC, 3 base)
            ### iteratively check succeed / failure of classic and CPC        
            loop_count = [0, 0]
            # cpc_min_exect = deadline - dag.backup - sum(dag.task_set[c].exec_t for c in classic_b.critical_path if c <len(dag.task_set))
            cpc_min_exect = deadline

            classic_budget = classic.calculate_budget(sl_idx, deadline, cpu_num)
            classic_bbudget = classic_b.calculate_budget(sl_idx, deadline, cpu_num)

            loop_count[0] = math.floor(min(classic_budget, classic_bbudget) / 2)

            start = 0
            end = math.floor(cpc_min_exect / sl_exec_t)

            while start < end :
                mid = int((start+end-1)/2)

                cpc.node_set[sl_idx].exec_t = mid * sl_exec_t
                cpc_bound = cpc.calculate_bound()

                if deadline < cpc_bound :
                    end = mid
                else :
                    start = mid+1

            norm = end

            start = 0
            end = math.floor(cpc_min_exect / sl_exec_t)

            while start < end :
                mid = int((start+end-1)/2)

                cpc_b.node_set[cpc_b.cvt(sl_idx)].exec_t = mid * sl_exec_t
                cpc_bbound = cpc_b.calculate_bound()

                if deadline < cpc_bbound :
                    end = mid
                else :
                    start = mid+1

            err = end

            loop_count[1] = min(norm, err)

            if any([l <= 1 for l in loop_count]) :
                print("Countinued - Non-feasible CPC({}, {})".format(norm, err))
                continue

            print(">[{}] {} {} - cpc({},{}) | deadline: {}".format(j, loop_count[0], loop_count[1], norm, err, deadline))

            loop_count[1] = max(loop_count)
            f.write("{},{},{},{},{}\n".format(func_count2score(loop_count[0]), func_count2score(loop_count[1]), func_count2score(base_small), func_count2score(base_middle), func_count2score(base_large)))

            ### makespan for classic and CPC
            # s0, m0 = check_count(dag, loop_count[0], acceptance, deadline, cpu_num, True)
            # s1, m1 = check_count(dag, loop_count[1], acceptance, deadline, cpu_num, True)
            # s2, m2 = check_count(dag, base_small, acceptance, deadline, cpu_num, False)
            # s3, m3 = check_count(dag, base_middle, acceptance, deadline, cpu_num, False)
            # s4, m4 = check_count(dag, base_large, acceptance, deadline, cpu_num, False)

            # score[0] += s0 ; miss[0] += m0 ; critical[0] += 1 if s0==1 and m0==1 else 0
            # score[1] += s1 ; miss[1] += m1 ; critical[1] += 1 if s1==1 and m1==1 else 0
            # score[2] += s2 ; miss[2] += m2 ; critical[2] += 1 if s2==1 or m2==1 else 0
            # score[3] += s3 ; miss[3] += m3 ; critical[3] += 1 if s3==1 or m3==1 else 0
            # score[4] += s4 ; miss[4] += m4 ; critical[4] += 1 if s4==1 or m4==1 else 0

            # f.write("{},{},{},{},{},{},{},{},{},{}\n".format(s0, m0, s1, m1, s2, m2, s3, m3, s4, m4))

            j += 1
        except KeyboardInterrupt :
            # f.close()
            sys.exit()
        # except Exception as e:
        #     print('Continued: ', e)
        #     continued += 1
        
    ## sum up all result
    print("Error: ", continued)
    print("Unacceptable: ", score)
    print("critical: ", critical)
    # f.write("{},{},{},{},{}".format(*score))
    # f.write("{},{},{},{},{}".format(*miss))
    f.close()