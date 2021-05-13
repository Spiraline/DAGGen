from .dag_file import DAGFile
from .dag_gen import DAGGen
from random import randint, random

def assign_looping(dag, cp) :
    sl_idx = randint(1, len(cp)-2) # TODO: check invalidness (1 <= len(cp)-2 always..)
    sl = cp[sl_idx]

    ### check and change outgoing edge - Dangling DAG
    # while True :
    #     queue = [sl]
    #     dangling = [[] for i in range(1, len(cp)-1)]
    #     while len(queue) is not 0 :
    #        q = queue.pop(0)
    #         l = dag.task_set[q].level

    #         # if q not in cp :
    #         print("index: ", l, sl_idx, l-sl_idx)
    #         dangling[l-sl_idx].append(q)
            
    #         for qq in dag.task_set[q].child :
    #             queue.append(qq)

    #     if [] in dangling :
    #         break 

    #     for i in range(len(dangling)-1) :
    #         p = dangling[i] ; c = dangling[i+1]
    #         rand_p = p[randint(0, len(p)-1)] ; rand_c = c[randint(0, len(c)-1)]
    #         new_child = list(set(dag.task_set[rand_p].child) & set().union(*dangling[i+1:]))
    #         p_c = new_child[randint(0, len(new_child)-1)]

    #         if rand_p in cp and p_c in cp :
    #             continue
    #         elif len(dag.task_set[rand_p].child) is 1 :
    #             dag.task_set[rand_p].child.append(rand_c)
    #             dag.task_set[rand_c].parent.append(rand_p)
    #         elif len(dag.task_set[p_c].parent) is not 1 :
    #             dag.task_set[rand_p].child.pop(p_c)
    #             dag.task_set[p_c].parent.pop(rand_p)

    dag.dangling_dag = [sl]
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

    for i in range(len(dag.task_set)) :
        if dag.task_set[i].level == 0 :
            task_queue.append(dag.task_set[i])
            distance[i] = dag.task_set[i].exec_t

    for i, v in enumerate(dag.task_set) :
        indegree[i] = len(v.parent)

    while task_queue :
        vertex = task_queue.pop(0)
        for v in vertex.child :
            distance[v] = max(dag.task_set[v].exec_t + distance[vertex.tid], distance[v]) 
            indegree[v] -= 1
            if indegree[v] == 0 :
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


def SelfLoopingDag(dag_input) :
    """
        Make self-looping node's WCET as -1
        and Return DAG and self-looping node index.
        Guarante dangling graph do not have out-going edge.
    """
    if type(dag_input) == type('str') :
        dag = DAGFile(dag_input)
    else :
        dag = DAGGen(**dag_input)

    critical_path = calculate_critical_path(dag)
    dag, sl = assign_looping(dag, critical_path)
    return dag, critical_path, sl
