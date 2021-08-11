import copy
from sys import path
from queue import PriorityQueue

path.insert(0, '..')

from task_gen.dag_file import DAGFile
from task_gen.dag_gen import Task, DAGGen
from task_gen.self_looping import argmax

from .cpc import CPCBound
from .naive import inter

def make_subDAG(dag, node_list) :
    newDAG = copy.deepcopy(dag)
    newDAG.start_node = []
    newDAG.end_node = []
    newDAG.depth = 0
    
    for node in newDAG.task_set :
        node.parent = []
        node.child = []

    for node in node_list :
        for n in inter(dag.task_set[node].child, node_list) :
            newDAG.task_set[node].child.append(n)
            newDAG.task_set[n].parent.append(node)

    for node in node_list :
        if len(newDAG.task_set[node].parent)==0 :
            newDAG.start_node.append(node)
        if len(newDAG.task_set[node].child)==0 :
            newDAG.end_node.append(node)

    return newDAG

def calculate_critical_path(dag, nl) :
    if len(nl) == 1 :
        return nl
    distance = [0,] * len(dag.task_set)
    indegree = [0,] * len(dag.task_set)
    task_queue = []

    for n in nl :
        if inter(dag.task_set[n].parent, nl) == [] :
            task_queue.append(dag.task_set[n])
            distance[n] = dag.task_set[n].exec_t

    for i, v in enumerate(dag.task_set) :
        indegree[i] = len(v.parent)

    while task_queue :
        vertex = task_queue.pop(0)
        for v in inter(vertex.child, nl) :
            distance[v] = max(dag.task_set[v].exec_t + distance[vertex.tid], distance[v]) 
            indegree[v] -= 1
            if indegree[v] == 0 :
                task_queue.append(dag.task_set[v])    

    cp = []
    cv = argmax(distance)

    while True :
        cp.append(cv)
        if len(inter(dag.task_set[cv].parent, nl)) == 0 :
            break
        cv = argmax(distance, inter(dag.task_set[cv].parent, nl))
    cp.reverse()
    return cp

def calculate_F(dag, nl, cp) :
    thetas = []
    theta_candidate = []
    for c in cp :
        parent_candidate = list(set(nl)-set(cp))
        if len(inter(dag.task_set[c].parent, parent_candidate)) != 0  :
            thetas.append(theta_candidate)
            theta_candidate = [c]
        else :
            theta_candidate.append(c)
    if len(theta_candidate) != 0 :
        thetas.append(theta_candidate)

    F = [[] for i in range(len(thetas))]
    available = list(set(nl) - set(cp))

    for i in range(len(thetas)-1) :
        next_theta = thetas[i+1]
        new_F = []
        visited = [False for i in range(len(dag.task_set))]
        queue = []
        for t in next_theta :
            for p in inter(dag.task_set[t].parent, nl) :
                visited[p] = True
                queue.append(p)

        while queue :
            q = queue.pop(0)
            new_F.append(q)
            for p in inter(dag.task_set[q].parent, nl) :
                if not visited[p] :
                    visited[p] = True
                    queue.append(p)

        new_F = inter(new_F, available)
        F[i] = new_F.copy()
        for f in new_F :
            available.remove(f)
    
    for a in available :
        F[-1].append(a)
    
    return F


def assign_priority(dag, nl=None, priority_list=None, priority=100) :
    dag_len = len(dag.task_set)
    if nl is None :
        nl = [i for i in range(dag_len)]
    if priority_list is None :
        priority_list = [0 for i in range(dag_len)]
        
    cp = calculate_critical_path(dag, nl)
    F = calculate_F(dag, nl, cp)
    for critical_node in cp :
        priority_list[critical_node] = priority
    priority -= 1

    for i, theta_ in enumerate(F) :
        theta = theta_.copy()
        while len(theta) != 0 :
            critical_path = calculate_critical_path(dag, theta)
            recursively = False
            for c in critical_path :
                if len(inter(dag.task_set[c].parent, theta)) > 1 :
                    recursively = True
                    break

            if recursively :
                new_dag = make_subDAG(dag, theta)
                priority_list = assign_priority(new_dag, theta, priority_list, priority)
                break
            else :
                for c in critical_path :
                    priority_list[c] = priority
                    if c in theta :
                        theta.remove(c)
                priority -= 1
    
    return priority_list

def find_minimum_nonzero(input) :
    for idx, i in enumerate(input) :
        if i > 0.0 :
            min_val = i
            min_idx = idx
            break

    for idx, i in enumerate(input) :
        if min_val > i and i != 0 :
            min_val = i
            min_idx = idx

    return min_val, min_idx


def calculate_makespan(dag, core_num, priority_list_extern, backup_priority_list_extern, backup=False):
    if backup :
        return makespan_backup(dag, core_num, priority_list_extern)
    else :
        return makespan(dag, core_num, priority_list_extern)

def makespan(dag, core_num, priority_list_extern):
    dag_len = len(dag.task_set)
    # priority_list = assign_priority(dag, [i for i in range(dag_len)], [i for i in range(dag_len)])
    priority_list = priority_list_extern
    # print("MakeSpan Priority List: " + str(priority_list))
    priority_pair = []
    waiting = []
    queue = PriorityQueue()
    
    visited = [False for i in range(len(priority_list))]  # priority pair + back
    available = [0 for i in range(core_num)]
    execute = [-1 for i in range(core_num)]
    time = 0

    for i, p in enumerate(priority_list):
        priority_pair.append((p*(-1), i))
        waiting.append(len(dag.task_set[i].parent))
        if waiting[i] == 0:
            visited[i] = True
            queue.put(priority_pair[i])

    while True:
        if queue.empty():
            if all([e == -1 for e in execute]):  # all done
                break
            elif any([e == -1 for e in execute]):  # some available
                min_val, loc = find_minimum_nonzero(available)
            else:  # all busy
                min_val = min(available)
                loc = available.index(min_val)

            time += min_val
            available = [max(a - min_val, 0) for a in available]

            for child in dag.task_set[execute[loc]].child :
                waiting[child] -= 1
                if waiting[child] == 0:
                    visited[child] = True
                    queue.put(priority_pair[child])
            
            execute[loc] = -1

        else:
            if any([e == -1 for e in execute]):  # some available (1 or all)
                min_val = min(available)
                loc = available.index(min_val)

                _, i = queue.get()

                available[loc] = dag.task_set[i].exec_t
                execute[loc] = i
                # print("time: ", time, "Min_val: ", min_val, "Loc: ", loc, "Available ", available, "Execute ", execute)

            else:  # all busy
                # print("time: ", time, "Min_val: ", min_val, "Loc: ", loc, "Available ", available, "Execute ", execute)
                min_val = min(available)
                loc = available.index(min_val)

                time += min_val
                available = [a-min_val for a in available]
                # print("time: ", time, "Min_val: ", min_val, "Loc: ", loc, "Available ", available, "Execute ", execute)
                for child in dag.task_set[execute[loc]].child:
                    waiting[child] -= 1
                    if waiting[child] == 0:
                        visited[child] = True
                        queue.put(priority_pair[child])

                execute[loc] = -1

        for i in range(core_num):
            if execute[i] != -1 and available[i] == 0.0:
                for child in dag.task_set[execute[i]].child:
                    waiting[child] -= 1
                    if waiting[child] == 0:
                        visited[child] = True
                        queue.put(priority_pair[child])
                execute[i] = -1
    # print(visited)
    return time


def makespan_backup(dag, core_num, priority_list_extern):
    dag_len = len(dag.task_set)
    # print(dag_len)
    priority_list = assign_priority(dag, [i for i in range(dag_len)], [i for i in range(dag_len)])
    # print(len(priority_list))
    # print(len(priority_list_extern))
    #priority_list = priority_list_extern
    priority_pair = []
    waiting = []
    queue = PriorityQueue()
    
    visited = [False for i in range(len(priority_list)+1)] # priority pair + back
    available = [0 for i in range(core_num)]
    execute = [-1 for i in range(core_num)]
    time = 0

    for i, p in enumerate(priority_list):
        priority_pair.append((p*(-1), i))
        waiting.append(len(dag.task_set[i].parent))
        if waiting[i]==0 :
            visited[i] = True
            queue.put(priority_pair[i])

    priority_list.append(max(priority_list))
    priority_pair.append((priority_list[-1]*(-1), dag_len))
    waiting.append(0)
    for t in dag.task_set:
        if len(inter(t.child, dag.dangling_dag)) != 0 and t.tid not in dag.dangling_dag:
            waiting[-1] += 1
    
    critical_path = calculate_critical_path(dag, [i for i in range(dag_len)])
    next_level = max([dag.task_set[d].level for d in dag.dangling_dag])
    if next_level == len(critical_path) - 1:
        next_level = -1

    while True :
        if queue.empty() :
            if all([e==-1 for e in execute]) : # all done
                break
            elif any([e==-1 for e in execute]) : # some available
                min_val, loc = find_minimum_nonzero(available)
            else : # all busy
                min_val = min(available)
                loc = available.index(min_val)

            time += min_val
            available = [max(a-min_val, 0) for a in available]

            if execute[loc] == dag_len : # backup
                if next_level != -1 :
                    child = critical_path[next_level+1]
                    waiting[child] -= 1
                    if waiting[child] == 0 :
                        queue.put(priority_pair[child])
            else :
                for child in dag.task_set[execute[loc]].child :
                    if child in dag.dangling_dag :
                        child = -1
                    waiting[child] -= 1
                    if waiting[child] == 0 :
                        queue.put(priority_pair[child])
            
            execute[loc] = -1

        else :
            if any([e==-1 for e in execute]) : # some available (1 or all)
                min_val = min(available)
                loc = available.index(min_val)

                _, i = queue.get()
                
                if i == dag_len  :
                    available[loc] = dag.backup
                else :
                    available[loc] = dag.task_set[i].exec_t
                
                execute[loc] = i

            else : # all busy
                min_val = min(available)
                loc = available.index(min_val)

                time += min_val
                available = [a-min_val for a in available]
                
                if execute[loc] == dag_len : # backup
                    if next_level != -1 :
                        child = critical_path[next_level+1]
                        waiting[child] -= 1
                        if waiting[child] == 0 :
                            queue.put(priority_pair[child])
                else :
                    for child in dag.task_set[execute[loc]].child :
                        if child in dag.dangling_dag :
                            child = -1
                        waiting[child] -= 1
                        if waiting[child] == 0 :
                            queue.put(priority_pair[child])

                execute[loc] = -1
            
        for i in range(core_num) :
            if execute[i] != -1 and available[i]==0.0 :
                if execute[i] != dag_len :
                    for child in dag.task_set[execute[i]].child :
                        if child in dag.dangling_dag :
                            child = -1
                        waiting[child] -= 1
                        if waiting[child] == 0 :
                            queue.put(priority_pair[child])
                else :
                    for child in dag.backup_child :
                        waiting[child] -= 1
                        if waiting[child] == 0 :
                            queue.put(priority_pair[child])
                execute[i] = -1
        # print('time: ', time, ' | waiting num: ', waiting[:-1], waiting[-1], " | avaliable: ", ' - '.join(["{} ({})".format(*z) for z in zip(available, execute)]))
    return time


def calculate_critical_path_backup(dag, nl) :
    if len(nl) == 1 :
        return nl
    distance = [0,] * len(dag.node_set)
    indegree = [0,] * len(dag.node_set)
    task_queue = []

    for n in nl :
        if inter(dag.node_set[n].pred, nl) == [] :
            task_queue.append(dag.node_set[n])
            distance[n] = dag.node_set[n].exec_t

    for i, v in enumerate(dag.node_set) :
        indegree[i] = len(v.pred)

    while task_queue :
        vertex = task_queue.pop(0)
        for v in inter(vertex.succ, nl) :
            distance[v] = max(dag.node_set[v].exec_t + distance[int(vertex.vid)], distance[v]) 
            indegree[v] -= 1
            if indegree[v] == 0 :
                task_queue.append(dag.node_set[v])    

    cp = []
    cv = argmax(distance)

    while True :
        cp.append(cv)
        if len(inter(dag.node_set[cv].pred, nl)) == 0 :
            break
        cv = argmax(distance, inter(dag.node_set[cv].pred, nl))
    cp.reverse()
    return cp

def calculate_F_backup(dag, nl, cp) :
    thetas = []
    theta_candidate = []
    for c in cp :
        parent_candidate = list(set(nl)-set(cp))
        if len(inter(dag.node_set[c].pred, parent_candidate)) != 0  :
            thetas.append(theta_candidate)
            theta_candidate = [c]
        else :
            theta_candidate.append(c)
    if len(theta_candidate) != 0 :
        thetas.append(theta_candidate)

    F = [[] for i in range(len(thetas))]
    available = list(set(nl) - set(cp))

    for i in range(len(thetas)-1) :
        next_theta = thetas[i+1]
        new_F = []
        visited = [False for i in range(len(dag.node_set))]
        queue = []
        for t in next_theta :
            for p in inter(dag.node_set[t].pred, nl) :
                visited[p] = True
                queue.append(p)

        while queue :
            q = queue.pop(0)
            new_F.append(q)
            for p in inter(dag.node_set[q].pred, nl) :
                if not visited[p] :
                    visited[p] = True
                    queue.append(p)

        new_F = inter(new_F, available)
        F[i] = new_F.copy()
        for f in new_F :
            available.remove(f)
    
    for a in available :
        F[-1].append(a)
    
    return F


def assign_priority_backup(dag, nl=None, priority_list=None, priority=100) :
    dag_len = len(dag.node_set)
    if nl is None :
        nl = [i for i in range(dag_len)]
    if priority_list is None :
        priority_list = [0 for i in range(dag_len)]
        
    cp = calculate_critical_path_backup(dag, nl)
    F = calculate_F_backup(dag, nl, cp)
    for critical_node in cp :
        priority_list[critical_node] = priority
    priority -= 1

    for i, theta_ in enumerate(F) :
        theta = theta_.copy()
        while len(theta) != 0 :
            critical_path = calculate_critical_path_backup(dag, theta)
            recursively = False
            for c in critical_path :
                if len(inter(dag.node_set[c].pred, theta)) > 1 :
                    recursively = True
                    break

            if recursively :
                new_node_set = make_subDAG(dag, theta)
                priority_list = assign_priority_backup(new_node_set, theta, priority_list, priority)
                break
            else :
                for c in critical_path :
                    priority_list[c] = priority
                    if c in theta :
                        theta.remove(c)
                priority -= 1
    
    return priority_list


if __name__ == '__main__' :
    DAG, cp, sl_idx = SelfLoopingDag('../input/input4.txt', 4)
    # DAG = DAGFile('../input/input4.txt')
    print(DAG)

    dag_len = len(DAG.task_set)
    priority_list = assign_priority(DAG)
    print(priority_list, DAG.dangling_dag)

    DAG.backup=20
    makespan = calculate_makespan(DAG, 2, True)
    print(makespan)
    print(DAG.backup_parent, DAG.backup_child)
    for task in DAG.task_set :
        print(task.tid, task.parent_b, task.child_b)