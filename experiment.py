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
from sched.priority import assign_priority, assign_priority_backup

def check_budget(dag, budget_list, acceptance, deadline, cpu_num, iter_size, sl_unit) :
    unacceptable = [0, 0, 0, 0]
    miss_deadline = [0, 0, 0, 0]
    both_fail = [0, 0, 0, 0]
    total_critical_failure = [0, 0, 0, 0]
    makespan = [0, 0, 0, 0]

    default_count = math.floor(score2count(acceptance))
    acceptable_count = max(budget_list) + 1
    
    iterative = 0
    while iterative < iter_size :
        # find unacceptable or miss deadline cases for iter_size times

        for j in range(default_count, max(budget_list)+1) : # find first fit
            score = count2score(j)
            if score > acceptance :
                acceptable_count = j
                break

        # check makespan
        dag.task_set[sl_idx].exec_t = sl_unit * min(budget_list[0], acceptable_count)
        if budget_list[0] >= acceptable_count : # success case
            makespan[0] = calculate_makespan(dag, cpu_num, False)
        else : # failure case
            makespan[0] = calculate_makespan(dag, cpu_num, True)

        dag.task_set[sl_idx].exec_t = sl_unit * min(budget_list[1], acceptable_count)
        if budget_list[1] >= acceptable_count : # success case
            makespan[1] = calculate_makespan(dag, cpu_num, False)
        else : # failure case
            makespan[1] = calculate_makespan(dag, cpu_num, True)

        dag.task_set[sl_idx].exec_t = sl_unit * min(budget_list[2], acceptable_count)
        makespan[2] = calculate_makespan(dag, cpu_num, False)

        dag.task_set[sl_idx].exec_t = sl_unit * min(budget_list[3], acceptable_count)
        makespan[3] = calculate_makespan(dag, cpu_num, False)

        iterative += 1
        # accumulate things
        for idx, (m, b) in enumerate(zip(makespan, budget_list)) :
            if b < acceptable_count and m <= deadline:
                unacceptable[idx] += 1
            elif b >= acceptable_count and  m > deadline:
                miss_deadline[idx] += 1
            elif b < acceptable_count and  m > deadline:
                both_fail[idx] += 1
            
            if idx > 1 and (b < acceptable_count or m > deadline) :
                total_critical_failure[idx] += 1

    return [u/iter_size for u in unacceptable], [d/iter_size for d in miss_deadline], [b/iter_size for b in both_fail], [c/iter_size for c in total_critical_failure]

if __name__ == '__main__' :
    parser = argparse.ArgumentParser(description='argparse for test')
    parser.add_argument('--dag_num', type=int, help='Test DAG number', default=100)
    parser.add_argument('--iter_size', type=int, help='#iterative per 1 DAG', default=100)

    parser.add_argument('--cpu_num', type=int, help='#cpu', default=4)
    parser.add_argument('--node_num', type=int, help='#node number in DAG', default=40)
    parser.add_argument('--dag_depth', type=float, help='depth of DAG', default=6.5)
    parser.add_argument('--backup', type=float, help='Backup node execution time rate', default=0.8)
    parser.add_argument('--sl_unit', type=float, help='SL node execution unit time', default=2.0)

    parser.add_argument('--node_avg', type=int, help='WCET average of nodes', default=40)
    parser.add_argument('--node_std', type=int, help='WCET std of nodes', default=10)

    parser.add_argument('--function', type=str, help='function type for score', default='e')
    parser.add_argument('--function_std', type=float, help='variance for score function', default=0.05)
    parser.add_argument('--acceptance', type=float, help='Acceptance bar for score function', default=0.85)

    parser.add_argument('--base', type=str, help='list for value of base [small, large]', default='100,200')
    parser.add_argument('--density', type=float, help='(avg execution time * node #) / (deadline * cpu #)', default=0.3)
    parser.add_argument('--dangling', type=float, help='dangling DAG node # / total node #', default=0.2)
    parser.add_argument('--SL_exp', type=int, help='exponential of SL node', default=30)

    parser.add_argument('--experiments', type=str, help='experiments guide', default='None')
    args = parser.parse_args()

    ### experiments argument
    dag_num = args.dag_num
    iter_size = args.iter_size
    cpu_num = args.cpu_num
    density = args.density
    sl_unit = args.sl_unit

    ### TODO: Implement more function type and set proper acceptance bar value.
    func_std = args.function_std
    def get_noise() :
        return normal(0, func_std, 1)

    if args.function == 'log' :
        def count2score(x) :
            return math.log(x+1) * get_noise()
    elif args.function == 'e' :
        def count2score(x, std=True) :
            delta = get_noise()
            if std :
                return max(1 - pow(math.e, -x/args.SL_exp) - math.fabs(delta), 0)
            else :
                return 1 - pow(math.e, -x/args.SL_exp)

        def score2count(score) :
            return (-args.SL_exp) * math.log(-score+1)
    else :
        raise NotImplementedError

    base_small, base_large = [int(b) for b in args.base.split(",")]

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
    acceptance = args.acceptance

    score = [0 for i in range(4)] # Classic, CPC, S, L
    miss = [0 for i in range(4)]
    both = [0 for i in range(4)]
    critical = [0 for i in range(4)]
    continued = 0

    if args.experiments not in ['None', 'acc', 'density', 'std'] :
        raise NotImplementedError

    if args.experiments in ['acc'] :
        file_name = "accuracy_out.txt"
    elif args.experiments in ['density'] :
        file_name = "density_{}.txt".format(int(float(args.density)*100))
    elif args.experiments in ['std'] :
        file_name = "std_{}.txt".format(int(float(args.function_std)*100))

    if args.experiments in ['acc', 'density', 'std'] :
        f = open(file_name, 'w')

    j = 0
    while j < dag_num :
        try :
            Task.idx = 0
            dag, cp, sl_idx = SelfLoopingDag(dag_param, dangling_num)
            dag.backup = args.node_avg * math.ceil(len(dag.dangling_dag)*args.backup)

            classic = ClassicBound(dag.task_set, cpu_num)
            classic_b = ClassicBackup(dag, cpu_num)
            cpc = CPCBound(dag.task_set, cpu_num)
            cpc_b = CPCBackup(dag, cpu_num)

            priority_list = assign_priority(dag)
            bound_priority = cpc.update_with_priority(priority_list)

            backup_priority_list = assign_priority_backup(cpc_b)
            bound_priority_backup = cpc_b.update_with_priority(backup_priority_list)
            sl_exec_t = sl_unit

            deadline = int((args.node_avg * args.node_num) / (cpu_num * density))

            ## check failure of every method(classic, CPC, 3 base)
            loop_count = [0, 0]
            cpc_min_exect = deadline

            classic_budget = classic.calculate_budget(sl_idx, deadline, cpu_num)
            classic_bbudget = classic_b.calculate_budget(sl_idx, deadline, cpu_num)

            loop_count[0] = math.floor(min(classic_budget, classic_bbudget) / sl_exec_t)

            start = 0
            end = math.floor(cpc_min_exect / sl_exec_t)

            while start < end :
                mid = int((start+end-1)/2)
                cpc.node_set[sl_idx].exec_t = mid * sl_exec_t
                cpc_bound = cpc.update_with_priority()

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
                cpc_bbound = cpc_b.update_with_priority()

                if deadline < cpc_bbound :
                    end = mid
                else :
                    start = mid+1

            err = end

            loop_count[1] = min(norm, err)

            if any([l <= 1 for l in loop_count]) :
                print("Continued - Non-feasible CPC({}, {})".format(norm, err))
                continue

            print(">[{}] {} {} - cpc({},{}) | deadline: {}".format(j, loop_count[0], loop_count[1], norm, err, deadline))

            loop_count[1] = max(loop_count)
            
            if args.experiments in ['acc'] :
                f.write("{},{},{},{}\n".format(count2score(loop_count[0]), count2score(loop_count[1]), count2score(base_small), count2score(base_large)))

            ### makespan for classic and CPC
            budget_list = [loop_count[0], loop_count[1], base_small, base_large]
            unacceptable, miss_deadline, both_fail, total_critical_failure = check_budget(dag, budget_list, acceptance, deadline, cpu_num, iter_size, sl_unit)
            s0, s1, s2, s3 = unacceptable
            m0, m1, m2, m3 = miss_deadline
            b0, b1, b2, b3 = both_fail
            c0, c1, c2, c3 = total_critical_failure

            score[0] += s0 ; miss[0] += m0 ; critical[0] += c0 ; both[0] += b0
            score[1] += s1 ; miss[1] += m1 ; critical[1] += c1 ; both[1] += b1
            score[2] += s2 ; miss[2] += m2 ; critical[2] += c2 ; both[2] += b2
            score[3] += s3 ; miss[3] += m3 ; critical[3] += c3 ; both[3] += b3

            j += 1

        except KeyboardInterrupt :
            if args.experiments in ['acc', 'density', 'std'] :
                f.close()
            sys.exit()
        except Exception as e:
            print('Continued: ', e)
            continued += 1
    
    score = [round(s, 1) for s in score]
    miss = [round(s, 1) for s in miss]
    both = [round(s, 1) for s in both]
    critical = [round(s, 1) for s in critical]
        
    ## sum up all result
    print("Error: ", continued)
    print("Unacceptable: ", score)
    print("Miss deadline: ", miss)
    print("Both: ", both)
    print("critical: ", critical)

    if args.experiments in ['density', 'std'] :
        f.write("{},{},{},{}\n".format(*score))
        f.write("{},{},{},{}\n".format(*miss))
        f.write("{},{},{},{}\n".format(*both))
        f.write("{},{},{},{}\n".format(*critical))

    if args.experiments in ['acc', 'density', 'std'] :
        f.close()
