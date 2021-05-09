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
    def __init__(self, task_set, core_num):
        self.task_set = task_set # G = (V,E)
        self.node_set =[]
        self.core_num = core_num # m
        self.non_critical_nodes = []
        self.critical_path = [] # Theta_i
        self.complete_paths = [] #
        self.provider_group = []
        self.consumer_group = []
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
        self.get_response_time_bound()

    def __str__(self):
        return ''

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

    def generate_interference_group(self):
        non_critical_group = self.non_critical_nodes.copy()
        node_set_sort = sorted(self.node_set, key=lambda node : node.level)

        for node in node_set_sort:
            node.interference_group = []
            union = list(set(node.anc) | set(node.desc))
            node_set = []
            for inter in self.node_set:
                if int(inter.vid) not in node_set and int(inter.vid) != int(node.vid):
                    node_set.append(int(node.vid))
            node.concurrent_group = list(set(node_set) - set(union))
            non_critical_group = list(set(non_critical_group) & set(node.concurrent_group))
            anc_interference_group = []
            for idx in node.anc:
                anc_interference_group = list(set(anc_interference_group) | set(self.node_set[idx].interference_group))
            node.interference_group = list(set(non_critical_group) & set(anc_interference_group))

    def generate_finish_time_bound(self):
        for node in self.node_set:
            if node.level == 0:
                node.finish_time = node.exec_t

        node_set_sort = sorted(self.node_set, key=lambda node: node.level)
        for node in node_set_sort:
            pred_finish = []
            for idx in node.pred:
                pred_finish.append(self.node_set[idx].finish_time)
            interference = 0
            concurrent_size = len(list(set(node.concurrent_group) - set(self.critical_path)))
            if int(node.vid) in self.critical_path or concurrent_size < self.core_num - 1:
                interference = 0
            else:
                sum = 0
                for idx in node.interference_group:
                    sum += self.node_set[idx].exec_t
                sum /= (self.core_num - 1)
                interference = math.ceil(sum)

            if pred_finish:
                node.finish_time = node.exec_t + max(pred_finish) + interference

    def get_alpha_beta(self):
        self.finish_time_provider_arr = []
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
                if self.node_set[v_j].finish_time <= self.finish_time_provider_arr[theta_i]:
                    graph_a.append(v_j)
                elif self.node_set[v_j].finish_time > self.finish_time_provider_arr[theta_i] and self.node_set[v_j].finish_time - self.node_set[v_j].exec_t < self.finish_time_provider_arr[theta_i]:
                    graph_b.append(v_j)

            sum_a = 0
            sum_b = 0
            for a in graph_a:
                sum_a += self.node_set[a].exec_t

            for b in graph_b:
                sum_b += self.finish_time_provider_arr[theta_i] - (self.node_set[b].finish_time - self.node_set[b].exec_t)
            alpha_i = sum_a + sum_b
            self.alpha_arr.append(alpha_i)

            # recursively search predecessor node which contribute to the longest path
            longest_local_path = []
            for idx in self.consumer_group[theta_i]:
                if self.node_set[idx].finish_time == self.finish_time_consumer_arr[theta_i]:
                    longest_local_path.append(idx)
            self.get_longest_path(longest_local_path, self.finish_time_provider_arr[theta_i])


            beta_i = 0
            for idx in longest_local_path:
                if self.node_set[idx].finish_time - self.node_set[idx].exec_t >= self.finish_time_provider_arr[theta_i]:
                    beta_i += self.node_set[idx].exec_t
                else:
                    beta_i += self.node_set[idx].finish_time - self.finish_time_provider_arr[theta_i]
            self.beta_arr.append(beta_i)

    def get_longest_path(self, path, f_theta_i):
        pred_candidate = []
        if path:
            idx = path[0]
            for pred_idx in self.node_set[idx].pred:
                if self.node_set[pred_idx].finish_time > f_theta_i:
                    pred_candidate.append(pred_idx)

        if pred_candidate:
            max_f = 0
            max_i = 0
            for i in pred_candidate:
                if self.node_set[i].finish_time > max_f:
                    max_f = self.node_set[i].finish_time
                    max_i = i
            path.append(max_i)
            self.get_longest_path(path, f_theta_i)


    def assign_priority(self, graph):
        for i in range(len(self.task_set)):
            print(self.task_set[i])

    def get_initial_budget_bound(self, ):
        print(self.task_set)

    def budget_bound_analysis(self):
        print(self.task_set)

    def check_budget_bound(self, budget, wcet):
        print(self.task_set)

    def get_response_time_bound(self):
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
