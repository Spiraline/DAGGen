import math

class Node(object):
    def __init__(self, **kwargs):
        # make a node with a task information
        self.vid = kwargs.get('vid', 0)  # v_j
        self.exec_t = kwargs.get('exec_t', 30.0)

        # Node information
        self.priority = -1
        self.pred = kwargs.get('pred', [])  # v_j's predecessors
        self.succ = kwargs.get('succ', [])  # v_j's successors
        self.anc = []
        self.desc = []
        self.alpha = 0
        self.beta = 0

        self.isProvider = False
        self.consumer_group = []  # F(theta_i)
        self.consumer_next_provider_group = []  # G(theta_i)
        self.interference_group = []
        self.concurrent_group = []
        self.non_critical_group = []
        self.isSink = kwargs.get('isLeaf', False)
        self.isSource = False
        self.finish_time = -1
        self.deadline = kwargs.get('deadline', -1)  # v_j's deadline
        self.level = kwargs.get('level', -1)  # v_j's deadline
        if self.level == 0:
            self.isSource = True

    def __str__(self):
        return str(self.vid) + "\t" + str(self.level) + "\t" + str(self.exec_t) + "\t" + str(self.pred) + "\t" + str(self.succ)


class CPCBound:
    def __init__(self, task_set, core_num=4):
        self.self_looping_idx = -1
        self.task_set = task_set # G = (V,E)
        self.node_set =[]
        self.core_num = core_num # m
        self.non_critical_nodes = []
        self.critical_path = [] # Theta_i
        self.complete_paths = [] #
        self.provider_group = []
        self.consumer_group = []
        self.local_path_group = []
        self.next_provider_consumer_group = []
        self.total_workload = [] # W_i
        self.finish_time_provider_arr = []
        self.finish_time_consumer_arr = []
        self.alpha_arr = []
        self.beta_arr = []
        self.generate_node_set()
        self.get_critical_path()
        self.construct_cpc_model()
        self.generate_interference_group()
        self.generate_finish_time_bound()
        self.get_alpha_beta()

        # print("--------------------------------")
        # print("provider_group: ", self.provider_group)
        # print("consumer_group (F): ", self.consumer_group)
        # print("next_provider_consumer_group (G): ", self.next_provider_consumer_group)
        # print("critical_path", self.critical_path)

        # print("finish_time_provider_arr: ", self.finish_time_provider_arr)
        # print("finish_time_consumer_arr: ", self.finish_time_consumer_arr)

        # print("alpha_arr: ", self.alpha_arr)
        # print("beta_arr: ", self.beta_arr)
        # print("--------------------------------")
        self.calculate_bound()

    def __str__(self):
        return ''

    def cvt(self, i) :
        return i

    def generate_node_set(self):
        for i in range(len(self.task_set)):
            node_param = {
                "vid": self.task_set[i].name[4:],
                "level": self.task_set[i].level,
                "exec_t": self.task_set[i].exec_t,
                "pred": self.task_set[i].parent,
                "succ": self.task_set[i].child,
                "deadline": self.task_set[i].deadline,
                "isLeaf": self.task_set[i].isLeaf
            }

            node = Node(**node_param)
            self.node_set.append(node)

        # find the ancestors and descendants of v_j
        for v_j in self.node_set:
            v_j.anc = self.find_ancestor(v_j.pred)
            v_j.desc = self.find_descendant(v_j.succ)

    def find_ancestor(self, pred):
        pred_arr = []
        if pred:
            for i in pred:
                if self.node_set[i].pred:
                    tmp_arr = self.find_ancestor(self.node_set[i].pred)
                    if tmp_arr:
                        pred_arr = pred_arr + tmp_arr

        if pred_arr:
            ancestor_tmp = pred + pred_arr
            ancestor = []
            for i in ancestor_tmp:
                if i not in ancestor:
                    ancestor.append(i)
            return ancestor
        else:
            return pred

    def find_descendant(self, succ):
        succ_arr = []
        if succ:
            for i in succ:
                if self.node_set[i].succ:
                    tmp_arr = self.find_descendant(self.node_set[i].succ)
                    if tmp_arr:
                        succ_arr = succ_arr + tmp_arr

        if succ_arr:
            descendant_tmp = succ + succ_arr
            descendant = []
            for i in descendant_tmp:
                if i not in descendant:
                    descendant.append(i)
            return descendant
        else:
            return succ

    def find_path(self, path, group, path_list):
        current_node = self.node_set[path[-1]]
        if current_node.succ:
            for suc in current_node.succ:
                if suc in group:
                    sub_path = []
                    sub_path += path
                    sub_path.append(suc)
                    self.find_path(sub_path, group, path_list)
                else:
                    path_list.append(path)
        else:
            path_list.append(path)

    def get_critical_path(self):
        # set critical path first node as node with level 0
        init_path = []
        for i in self.node_set:
            if i.isSource:
                init_path.append(int(i.vid))

        self.find_complete_path(init_path)

        max_exec_t = 0
        for i in self.complete_paths:
            len_exec_t = 0
            for j in i:
                len_exec_t += self.node_set[j].exec_t
            if len_exec_t > max_exec_t:
                max_exec_t = len_exec_t
                self.critical_path = i

        for node in self.node_set:
            if int(node.vid) not in self.critical_path:
                self.non_critical_nodes.append(int(node.vid))

    def find_complete_path(self, path):
        current_node = self.node_set[path[-1]]
        succ_num = len(current_node.succ)

        if succ_num == 0 and current_node.isSink:
            self.complete_paths.append(path)
        elif succ_num > 0:
            for i in current_node.succ:
                sub_path = []
                sub_path += path
                sub_path.append(i)
                self.find_complete_path(sub_path)
        
    def construct_cpc_model(self):
        self.provider_group = []
        self.consumer_group = []
        self.next_provider_consumer_group = []

        non_critical_nodes = self.non_critical_nodes.copy()
        theta = []
        for idx in range(len(self.critical_path)):
            if idx < len(self.critical_path) - 1:
                theta.append(self.critical_path[idx])
                if len(self.node_set[self.critical_path[idx + 1]].pred) == 1:
                    continue
                else:
                    self.provider_group.append(theta)
                    theta = []
            else:
                theta.append(self.critical_path[idx])
                self.provider_group.append(theta)
        # print(self.critical_path)
        # print(non_critical_nodes)
        # print(self.provider_group)
        for idx in range(len(self.provider_group)):
            if idx < len(self.provider_group) - 1:
                provider = self.provider_group[idx]
                next_provider = self.provider_group[idx + 1]
                self.node_set[provider[0]].isProvider = True
                self.node_set[provider[0]].consumer_group = list(set(self.node_set[next_provider[0]].anc) & set(non_critical_nodes))
                self.consumer_group.append(self.node_set[provider[0]].consumer_group.copy())
                next_provider_consumer_group = []
                # for getting G(theta)
                for v_j in self.provider_group[idx]:
                    node = self.node_set[v_j]
                    union = list(set(node.anc) | set(node.desc))
                    node_set = []
                    for node in self.node_set:
                        if int(node.vid) not in node_set and int(node.vid) != v_j:
                            node_set.append(int(node.vid))
                    concurrent_nodes = list(set(node_set) - set(union))
                    intersection = list(set(concurrent_nodes) & set(non_critical_nodes))
                    difference = list(set(intersection) - set(self.node_set[provider[0]].consumer_group))
                    next_provider_consumer_group = list(set(next_provider_consumer_group) | set(intersection))
                    # print(v_j," ", union, concurrent_nodes, intersection, next_provider_consumer_group)
                next_provider_consumer_group = list(set(next_provider_consumer_group) - set(self.node_set[provider[0]].consumer_group))
                self.node_set[provider[0]].consumer_next_provider_group = next_provider_consumer_group.copy()
                non_critical_nodes = list(set(non_critical_nodes) - set(self.node_set[provider[0]].consumer_group))
                self.next_provider_consumer_group.append(next_provider_consumer_group.copy())
                # print("F(theta_i)", self.node_set[provider[0]].consumer_group)
                # print("G(theta_i)", self.node_set[provider[0]].consumer_next_provider_group)
            else:
                self.consumer_group.append([])
                self.next_provider_consumer_group.append([])

    def generate_interference_group(self):
        non_critical_group = self.non_critical_nodes.copy()
        node_set_sort = sorted(self.node_set, key=lambda node : node.level)

        for node in node_set_sort:
            node.interference_group = []
            union = list(set(node.anc) | set(node.desc))
            node_set = []
            for inter in self.node_set:
                if int(inter.vid) not in node_set and int(inter.vid) != int(node.vid):
                    node_set.append(int(inter.vid))
            node.concurrent_group = list(set(node_set) - set(union))
            node.non_critical_group = list(set(non_critical_group) & set(node.concurrent_group))
            # print(node.vid, node.concurrent_group, node.non_critical_group)
            anc_interference_group = []
            for idx in node.anc:
                anc_interference_group = list(set(anc_interference_group) | set(self.node_set[idx].interference_group))
                #print(node.vid, node.anc, idx, anc_interference_group, set(self.node_set[idx].interference_group))

            non_anc_interference_group = list(set(node.concurrent_group) - set(anc_interference_group))
            node.interference_group = list(set(non_critical_group) & set(non_anc_interference_group))
            # print(node.vid, node.concurrent_group, anc_interference_group, non_anc_interference_group, node.interference_group)

    def generate_finish_time_bound(self):
        # source node's finish time is 0 + its exec_t
        for node in self.node_set:
            if node.level == 0:
                node.finish_time = node.exec_t

        node_set_sort = sorted(self.node_set, key=lambda node: node.level)
        for node in node_set_sort:
            pred_finish = []
            for idx in node.pred:
                pred_finish.append(self.node_set[idx].finish_time)
            interference = 0
            non_critical_concurrent_group = list(set(node.concurrent_group) - set(self.critical_path))
            all_path_group = self.get_all_path_of_group(non_critical_concurrent_group)
            # print(node.vid, all_path_group)
            if (int(node.vid) in self.critical_path) or len(all_path_group) < self.core_num - 1:
                interference = 0
            else:
                sum = 0
                for idx in node.interference_group:
                    sum += self.node_set[idx].exec_t
                sum /= (self.core_num - 1)
                interference = math.ceil(sum)

            if pred_finish:
                node.finish_time = node.exec_t + max(pred_finish) + interference
            # print(node.vid, node.finish_time)

    def get_alpha_beta(self):
        self.finish_time_provider_arr = []
        self.alpha_arr = []
        self.beta_arr = []

        for provider in self.provider_group:
            provider_finish = []
            for idx in provider:
                provider_finish.append(self.node_set[idx].finish_time)
            provider_finish_time = max(provider_finish)
            self.finish_time_provider_arr.append(provider_finish_time)

        self.finish_time_consumer_arr = []
        for consumer in self.consumer_group:
            consumer_finish = []
            for idx in consumer:
                consumer_finish.append(self.node_set[idx].finish_time)
            if consumer_finish:
                consumer_finish_time = max(consumer_finish)
            else:
                consumer_finish_time = 0
            self.finish_time_consumer_arr.append(consumer_finish_time)

        for theta_i in range(len(self.provider_group)):
            list_f_g_union = list(set(self.consumer_group[theta_i]) | set(self.next_provider_consumer_group[theta_i]))
            graph_a = []
            graph_b = []
            for v_j in list_f_g_union:
                # print(self.node_set[v_j].finish_time, self.finish_time_provider_arr[theta_i])
                if self.node_set[v_j].finish_time <= self.finish_time_provider_arr[theta_i]:
                    graph_a.append(v_j)
                elif self.node_set[v_j].finish_time > self.finish_time_provider_arr[theta_i] and self.node_set[v_j].finish_time - self.node_set[v_j].exec_t < self.finish_time_provider_arr[theta_i]:
                    graph_b.append(v_j)

            # print(theta_i,graph_a, graph_b)
            sum_a = 0
            sum_b = 0
            for a in graph_a:
                sum_a += self.node_set[a].exec_t

            for b in graph_b:
                sum_b += self.finish_time_provider_arr[theta_i] - (self.node_set[b].finish_time - self.node_set[b].exec_t)
            alpha_i = sum_a + sum_b
            # print(alpha_i)
            self.alpha_arr.append(alpha_i)

            # recursively search predecessor node which contribute to the longest path
            longest_local_path = []
            # print(self.consumer_group)
            for idx in self.consumer_group[theta_i]:
                # print(idx, self.node_set[idx].finish_time, self.finish_time_consumer_arr[theta_i])
                if self.node_set[idx].finish_time == self.finish_time_consumer_arr[theta_i]:
                    longest_local_path.append(idx)
            if longest_local_path:
                longest_local_path = self.get_longest_path(longest_local_path, theta_i, self.finish_time_provider_arr[theta_i])
            # print(theta_i, longest_local_path)

            beta_i = 0
            for idx in longest_local_path:
                if self.node_set[idx].finish_time - self.node_set[idx].exec_t >= self.finish_time_provider_arr[theta_i]:
                    beta_i += self.node_set[idx].exec_t
                else:
                    beta_i += self.node_set[idx].finish_time - self.finish_time_provider_arr[theta_i]
            self.beta_arr.append(beta_i)

    def get_all_path_of_group(self, group):
        all_path = []
        for idx in group:
            path_list = []
            path = []
            path.append(idx)
            self.find_path(path, group, path_list)
            if path_list:
                # print(idx, path_list)
                for p in path_list:
                    if all_path:
                        is_sub = False
                        for a in all_path:
                            if self.is_sub_list(p, a):
                                is_sub = True
                                break
                        if not is_sub:
                            all_path.append(p)
                    else:
                        all_path.append(p)
        return all_path

    def is_sub_list(self, sub_list, super_list):
        unique_elements = set(sub_list)
        for e in unique_elements:
            if sub_list.count(e) > super_list.count(e):
                return False
        # It is sublist
        return True

    def get_local_path(self, consumer_group):
        # a provider has local path in its consumer group F(theta_i)
        # print(consumer_group)
        pass

    def get_longest_path(self, longest_local_path, i, f_theta_i):
        all_local_path = self.get_all_path_of_group(self.consumer_group[i])
        # print(i, all_local_path)
        pred_candidate = []
        if longest_local_path:
            end_node_idx = longest_local_path[0]

            # print(i, self.node_set[end_node_idx].pred)
            for pred_idx in self.node_set[end_node_idx].pred:
                if self.node_set[pred_idx].finish_time > f_theta_i:
                    pred_candidate.append(pred_idx)

        loc_path_after = []
        local_path = []
        if pred_candidate:
            # print(pred_candidate)
            max_f = 0
            max_i = 0
            for i in pred_candidate:
                if self.node_set[i].finish_time > max_f:
                    max_f = self.node_set[i].finish_time
                    max_i = i

            for path in all_local_path:
                if max_i in path:
                    local_path.append(path)
            max_dist = 0
            max_path = []

            for path in local_path:
                if len(path) > 1:
                    dist = 0
                    for idx in path:
                        dist += self.node_set[idx].exec_t
                    if dist > max_dist:
                        max_dist = dist
                        max_path = path
                else:
                    max_dist = self.node_set[path[0]].exec_t
                    max_path = path
            for idx in max_path:
                if self.node_set[idx].finish_time > f_theta_i:
                    loc_path_after.append(idx)
            if end_node_idx in loc_path_after:
                longest_local_path = loc_path_after.copy()
            else:
                longest_local_path = loc_path_after.copy()
                longest_local_path.append(end_node_idx)
        if self.node_set[end_node_idx].finish_time <= f_theta_i:
            longest_local_path = []
        return longest_local_path

    def assign_priority(self, graph):
        for i in range(len(self.task_set)):
            print(self.task_set[i])

    def get_initial_budget_bound(self, ):
        print(self.task_set)

    def budget_bound_analysis(self):
        print(self.self_looping_idx)

    def check_budget_bound(self, budget, wcet):
        print(self.task_set)

    def calculate_bound(self):
        sum_response_time = 0
        for theta_i in range(len(self.provider_group)):
            length_i = 0
            for idx in self.provider_group[theta_i]:
                length_i += self.node_set[idx].exec_t
            workload_i = 0
            union = list(set(self.provider_group[theta_i]) | set(self.consumer_group[theta_i]) | set(self.next_provider_consumer_group[theta_i]))
            for idx in union:
                workload_i += self.node_set[idx].exec_t
            alpha_i = self.alpha_arr[theta_i]
            beta_i = self.beta_arr[theta_i]

            response_time_i = length_i + math.ceil((workload_i - length_i - alpha_i - beta_i)/self.core_num) + beta_i
            sum_response_time += response_time_i
        return sum_response_time

    def setting_theta(self, self_looping_idx):
        self.self_looping_idx = self_looping_idx
        self.get_critical_path() 
        idx_in_provider = 0
        splited = []
        for provider in self.provider_group:
            if self.self_looping_idx in provider:
                idx_in_provider = provider.index(self.self_looping_idx)
                if self_looping_idx == provider[-1]:
                    # do not slice the provider, we do not have to
                    break
                else:
                    self_looping_provider = provider[:idx_in_provider + 1]
                    later_provider = provider[idx_in_provider + 1:]
                    splited.append(self_looping_provider)
                    splited.append(later_provider)
                    p_idx = self.provider_group.index(provider)
                    self.provider_group.remove(provider)
                    self.provider_group.insert(p_idx, later_provider)
                    self.provider_group.insert(p_idx, self_looping_provider)
        self.update_cpc_model()
        self.generate_interference_group()
        self.generate_finish_time_bound()
        self.get_alpha_beta()
        self.calculate_bound()
        # print("provider_group", self.provider_group)
        # print("consumer_group (F)", self.consumer_group)
        # print("next_provider_consumer_group (G)", self.next_provider_consumer_group)

    def update_cpc_model(self):
        non_critical_nodes = self.non_critical_nodes.copy()
        self.consumer_group = []
        self.next_provider_consumer_group = []
        for idx in range(len(self.provider_group)):
            if idx < len(self.provider_group) - 1:
                provider = self.provider_group[idx]
                next_provider = self.provider_group[idx + 1]
                self.node_set[provider[0]].isProvider = True
                self.node_set[provider[0]].consumer_group = list(set(self.node_set[next_provider[0]].anc) & set(non_critical_nodes))
                self.consumer_group.append(self.node_set[provider[0]].consumer_group.copy())
                next_provider_consumer_group = []
                for v_j in self.node_set[provider[0]].consumer_group:
                    node = self.node_set[v_j]
                    union = list(set(node.anc) | set(node.desc))
                    node_set = []
                    for node in self.node_set:
                        if int(node.vid) not in node_set and int(node.vid) != v_j:
                            node_set.append(int(node.vid))
                    concurrent_nodes = list(set(node_set) - set(union))
                    intersection = list(set(concurrent_nodes) & set(non_critical_nodes))
                    next_provider_consumer_group = list(set(next_provider_consumer_group) | set(intersection))
                    # print(v_j," ", union, concurrent_nodes, intersection, next_provider_consumer_group)
                next_provider_consumer_group = list(set(next_provider_consumer_group) - set(self.node_set[provider[0]].consumer_group))
                self.node_set[provider[0]].consumer_next_provider_group = next_provider_consumer_group.copy()
                non_critical_nodes = list(set(non_critical_nodes) - set(self.node_set[provider[0]].consumer_group))
                self.next_provider_consumer_group.append(next_provider_consumer_group.copy())
                # print("F(theta_i)", self.node_set[provider[0]].consumer_group)
                # print("G(theta_i)", self.node_set[provider[0]].consumer_next_provider_group)
            else:
                self.consumer_group.append([])
                self.next_provider_consumer_group.append([])


class CPCBackup(CPCBound) :
    def __init__(self, dag, core_num=4):
        self.dag = dag
        self.dangling_dag = dag.dangling_dag
        self.backup = dag.backup
        self.backup_parent = dag.backup_parent
        self.backup_child = dag.backup_child

        self.self_looping_idx = -1
        self.task_set = dag.task_set # G = (V,E)
        self.node_set =[]
        self.core_num = core_num # m
        self.non_critical_nodes = []
        self.critical_path = [] # Theta_i
        self.complete_paths = [] #
        self.provider_group = []
        self.consumer_group = []
        self.local_path_group = []
        self.next_provider_consumer_group = []
        self.total_workload = [] # W_i
        self.finish_time_provider_arr = []
        self.finish_time_consumer_arr = []
        self.alpha_arr = []
        self.beta_arr = []

        self.generate_node_set()
        self.get_critical_path()
        self.construct_cpc_model()
        self.generate_interference_group()
        self.generate_finish_time_bound()
        self.get_alpha_beta()

        # print("provider_group: ", self.provider_group)
        # print("consumer_group (F): ", self.consumer_group)
        # print("next_provider_consumer_group (G): ", self.next_provider_consumer_group)
        
        # print("critical_path", self.critical_path)
        # print("non-critical", self.non_critical_nodes)
        # print("finish_time_provider_arr: ", self.finish_time_provider_arr)
        # print("finish_time_consumer_arr: ", self.finish_time_consumer_arr)

        # print("alpha_arr: ", self.alpha_arr)
        # print("beta_arr: ", self.beta_arr)
        self.calculate_bound()

    def generate_node_set(self):
        node_dict = {}
        self.critical_path = []
        new_num = 0
        for i in range(len(self.task_set)) :
            if i not in self.dangling_dag :
                node_dict[i] = new_num
                new_num += 1
        node_dict[len(self.task_set)] = new_num
        self.node_dict = node_dict

        for i in range(len(self.task_set)):
            if i not in self.dangling_dag :
                idx = node_dict[i]
                node_param = {
                    "vid": str(idx),
                    "level": self.task_set[i].level,
                    "exec_t": self.task_set[i].exec_t,
                    "pred": [node_dict[p] for p in self.task_set[i].parent_b],
                    "succ": [node_dict[c] for c in self.task_set[i].child_b],
                    "deadline": self.task_set[i].deadline,
                }

                node = Node(**node_param)
                self.node_set.append(node)
                if i in self.dag.critical_path :
                    self.critical_path.append(node_dict[i])

        new_level = max([self.task_set[d].level for d in self.dangling_dag])
        node_param = {
            "vid": str(new_num),
            "level": new_level,
            "exec_t": self.backup,

            "pred": [node_dict[p] for p in self.backup_parent],
            "succ": [node_dict[c] for c in self.backup_child],
            
            "deadline": 0,
        }
        node = Node(**node_param)
        self.node_set.append(node)

        # find the ancestors and descendants of v_j
        for i, v_j in enumerate(self.node_set) :
            if len(v_j.pred) == 0 :
                self.node_set[i].isSource = True
            elif len(v_j.succ) == 0 :
                self.node_set[i].isSink = True

            v_j.anc = self.find_ancestor(v_j.pred)
            v_j.desc = self.find_descendant(v_j.succ)

    def cvt(self, i) :
        return self.node_dict[i]

    def get_critical_path(self) :
        self.non_critical_nodes = []
        for n in range(len(self.node_set)) :
            if n not in self.critical_path :
                self.non_critical_nodes.append(n)