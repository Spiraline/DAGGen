from .dag_file import DAGFile
from .dag_gen import DAGGen
from random import randint, random

def assign_looping(dag, cp, dangling_num) :
    sl_idx = randint(1, len(cp)-2) # TODO: check invalidness (1 <= len(cp)-2 always..)
    dag.sl_idx = sl_idx

    #print("CP: " + str(cp))
    sl = cp[sl_idx]
    visited = [False for i in range(len(dag.task_set))]

    assigned_list = [ ]
    assigned_num = 0

    queue = [sl]
    visited[sl] = True

    for t in dag.task_set :
        if t.level == 0 :
            start_node = t.tid
        if t.level == len(cp)-1 :
            end_node = t.tid

    ## check and change outgoing edge - Dangling DAG
    while assigned_num < dangling_num :
        if len(queue) == 0 :
            break

        q = queue.pop(0)
        assigned_list.append(q)
        assigned_num += 1

        if len(queue) < dangling_num - assigned_num : # assign all
            if q not in cp or True: # TODO: Check..
                for qq in dag.task_set[q].child :
                    if not visited[qq] :
                        visited[qq] = True
                        queue.append(qq)

        else : # enough to use only node in queue
            for qq in dag.task_set[q].child :
                if not visited[qq] : 
                    dag.task_set[qq].parent.remove(q)
                    dag.task_set[q].child.remove(qq)

                    if len(dag.task_set[qq].parent) == 0 and qq != start_node :
                        dag.task_set[qq].parent.append(start_node)
                        dag.task_set[start_node].child.append(qq)

                    if len(dag.task_set[q].child) == 0 and q != end_node :
                        dag.task_set[end_node].parent.append(q)
                        dag.task_set[q].child.append(end_node)

    assigned_list.remove(sl)
    dag.dangling_dag = assigned_list
    return dag, sl

def argmax(value_list, index_list=None):
    if index_list is None :
        index_list = list(range(len(value_list)))
    max_index, max_value = [index_list[0], value_list[index_list[0]]]
    for i in index_list :
        if value_list[i] > max_value :
            max_index = i
            max_value = value_list[i]
    return max_index

def calculate_critical_path(dag) :
    distance = [0,] * len(dag.task_set)
    indegree = [0,] * len(dag.task_set)
    task_queue = []
    # print(dag)
    for i in range(len(dag.task_set)):
        if dag.task_set[i].level == 0 :
            task_queue.append(dag.task_set[i])
            distance[i] = dag.task_set[i].exec_t

    for i, v in enumerate(dag.task_set):
        indegree[i] = len(v.parent)

    while task_queue:
        vertex = task_queue.pop(0)
        for v in vertex.child:
            distance[v] = max(dag.task_set[v].exec_t + distance[vertex.tid], distance[v]) 
            indegree[v] -= 1
            if indegree[v] == 0:
                task_queue.append(dag.task_set[v])    

    cp = []
    cv = argmax(distance)

    while True :
        cp.append(cv)
        if len(dag.task_set[cv].parent) == 0 :
            break
        cv = argmax(distance, dag.task_set[cv].parent)

    cp.reverse()
    return cp


def SelfLoopingDag(dag_input, dangling_num) :
    """
        Make self-looping node's WCET as -1
        and Return DAG and self-looping node index.
        Guarante dangling graph do not have out-going edge.
    """
    if type(dag_input) == type('str') :
        dag = DAGFile(dag_input)
    else :
        dag = DAGGen(**dag_input)
    
    dag.critical_path = calculate_critical_path(dag)
    dag, sl = assign_looping(dag, dag.critical_path, dangling_num)

    ## Add parent / child dependency for backup
    dag.backup_parent = []
    dag.backup_child = []
    dag_len = len(dag.task_set)

    for i, task in enumerate(dag.task_set) :
        for child in task.child :
            if i not in dag.dangling_dag : # Non dangling
                if child in dag.dangling_dag :  # nd -> d
                    dag.backup_parent.append(i)
                    dag.task_set[i].child_b.append(dag_len)
                else : # nd -> nd
                    dag.task_set[i].child_b.append(child) 
                    dag.task_set[child].parent_b.append(i)
            else : 
                if child not in dag.dangling_dag : # d -> nd
                    dag.backup_child.append(child)
                    dag.task_set[child].parent_b.append(dag_len)

    dag.backup_parent.append(dag.sl_idx)
    dag.task_set[dag.sl_idx].child_b.append(dag_len)

    for i in range(len(dag.task_set)) :
        dag.task_set[i].parent_b = list(set(dag.task_set[i].parent_b))
        dag.task_set[i].child_b = list(set(dag.task_set[i].child_b))

    dag.backup_parent = list(set(dag.backup_parent))
    dag.backup_child = list(set(dag.backup_child))

    return dag, dag.critical_path, sl
