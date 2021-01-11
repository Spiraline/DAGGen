from random import randint
import sys
import os
import csv

# get [mean, stdev] and return mean +- stdev random number
def rand_uniform(arr):
    ### TODO : raise exception when invalid value
    return randint(int(arr[0] - arr[1]), int(arr[0] + arr[1]))

class Task(object):
    idx = 0
    def __init__(self, **kwargs):
        self.tid = Task.idx
        Task.idx += 1
        self.name = kwargs.get('name', '')
        self.exec_t = kwargs.get('exec_t', 30.0)

        # Assigned after DAGGen
        self.parent = []
        self.child = []
        self.isLeaf = False
        self.deadline = -1
        self.level = 0

    def __str__(self):
        res = "[%s] exec_t : %.1f parent : %15s child : %15s" \
            % (self.name, self.exec_t, self.parent, self.child)

        if self.isLeaf:
            res += " deadline : %s" % (self.deadline)

        if self.laxity != sys.maxsize:
            res += "\n\tlaxity : %.1f" % (self.laxity)

            if self.superiority != 0:
                res += " sup : %d" % self.superiority

        return res

    def new_task_set(self):
        Task.idx = 0

class DAGGen(object):
    def __init__(self, **kwargs):
        self.task_num = kwargs.get('task_num', [10, 3])
        self.depth = kwargs.get('depth', [3.5, 0.5])
        self.exec_t = kwargs.get('exec_t', [50.0, 30.0])
        self.start_node = kwargs.get('start_node', [2, 1])
        self.extra_arc_ratio = kwargs.get('extra_arc_ratio', 0.1)

        self.task_set = []

        ### 1. Initialize Task
        task_num = rand_uniform(self.task_num)
        for i in range(task_num):
            task_param = {
                "name" : "node" + str(i),
                "exec_t" : rand_uniform(self.exec_t)
            }

            self.task_set.append(Task(**task_param))

        depth = rand_uniform(self.depth)

        extra_arc_num = int(task_num * self.extra_arc_ratio)

        ### 2. Classify Task by randomly-select level
        level_arr = []
        for i in range(depth):
            level_arr.append([])

        # put start nodes in level 0
        start_node_num = rand_uniform(self.start_node)
        for i in range(start_node_num):
            level_arr[0].append(i)
            self.task_set[i].level = 0
        
        # Each level must have at least one node
        for i in range(1, depth):
            level_arr[i].append(start_node_num + i - 1)
            self.task_set[start_node_num+i-1].level = i

        # put other nodes in other level randomly
        for i in range(start_node_num + depth - 1, task_num):
            level = randint(1, depth-1)
            self.task_set[i].level = level
            level_arr[level].append(i)

        ### 3. make arc from last level
        for level in range(depth-1, 0, -1):
            for task_idx in level_arr[level]:
                parent_idx = level_arr[level-1][randint(0, len(level_arr[level - 1])-1)]

                self.task_set[parent_idx].child.append(task_idx)
                self.task_set[task_idx].parent.append(parent_idx)

        ### 4. make extra arc
        for i in range(extra_arc_num):
            arc_added_flag = False
            while not arc_added_flag:
                task1_idx = randint(0, task_num-1)
                task2_idx = randint(0, task_num-1)

                if self.task_set[task1_idx].level < self.task_set[task2_idx].level:
                    self.task_set[task1_idx].child.append(task2_idx)
                    self.task_set[task2_idx].parent.append(task1_idx)
                    arc_added_flag = True
                elif self.task_set[task1_idx].level > self.task_set[task2_idx].level:
                    self.task_set[task2_idx].child.append(task1_idx)
                    self.task_set[task1_idx].parent.append(task2_idx)
                    arc_added_flag = True
        
        ### 5. set deadline ( exec_t avg * (level + 1)) * 2
        for task in self.task_set:
            if len(task.child) == 0:
                task.isLeaf = True
                task.deadline = self.exec_t[0] * (task.level+1) * 2

    def __str__(self):
        for task in self.task_set:
            print(task)
        
        return ''

def configure_dag(dag):
    ### Calculate Superiority
    task_queue = []

    # Enqueue leaf tasks
    for task in dag.task_set:
        if len(task.child) == 0:
            task_queue.insert(0, task)

    while len(task_queue) > 0:
        task = task_queue.pop()
        
        if len(task.child) == 0:
            task.superiority = 1

        else:
            # if all child superiority are not calculated, enqueue again
            canCalculate = True
            for child_idx in task.child:
                if dag.task_set[child_idx].superiority == 0:
                    canCalculate = False
                    break

            if not canCalculate:
                task_queue.insert(0, task)
                continue
            
            # calculate superiority
            
            for child_idx in task.child:
                task.superiority += dag.task_set[child_idx].superiority
            task.superiority += 1
        
        # After calculating, enqueue parent nodes
        for parent_idx in task.parent:
            parent_task = dag.task_set[parent_idx]
            if parent_task not in task_queue:
                task_queue.insert(0, parent_task)

    ### Assign priority and affinity
    sup_queue = sorted([task for task in dag.task_set],
            key= lambda x : x.superiority)

    curr_priority = 41
    curr_run_core = 0
    prev_sup = -1

    for task in sup_queue:
        if prev_sup != task.superiority:
            curr_priority -= 1
        task.priority = curr_priority
        task.run_core = curr_run_core
        curr_run_core = (curr_run_core + 1) % CORE_NUM

def launch_gen(dag, isCFS):
    rospack = rospkg.RosPack()
    pack_path = rospack.get_path('ros-dagsim')

    with open(pack_path + '/launch/tmp.launch', 'w') as f:
        f.write('<launch>\n')

        # Add dummy task
        for i in range(DUMMY_NUM):
            f.write('\t<node pkg="ros-dagsim" type="middle_node" name="dummy' + str(i) +'">\n')
            f.write('\t\t<param name="waste_time" value="90" />\n')
            f.write('\t\t<param name="period" value="100" />\n')
            f.write('\t</node>\n')

        for task in dag.task_set:
            # entry node
            if len(task.parent) == 0:
                f.write('\t<node pkg="ros-dagsim" type="entry_node" name="' + task.name + '">\n')
                f.write('\t\t<rosparam param="child_idx">' + str(task.child) + '</rosparam>\n')
                f.write('\t\t<param name="enable_debug" value="true" />\n')
                if isCFS:
                    f.write('\t\t<param name="isCFS" value="true" />\n')
                else:
                    f.write('\t\t<param name="isCFS" value="false" />\n')
                f.write('\t\t<param name="waste_time" value="' + str(task.exec_t) + '" />\n')
                f.write('\t\t<param name="period" value="' + str(PERIOD) + '" />\n')
                f.write('\t</node>\n')
            # leaf node
            elif len(task.child) == 0:
                f.write('\t<node pkg="ros-dagsim" type="leaf_node" name="' + task.name + '">\n')
                f.write('\t\t<rosparam param="parent_idx">' + str(task.parent) + '</rosparam>\n')
                f.write('\t\t<param name="waste_time" value="' + str(task.exec_t) + '" />\n')
                if isCFS:
                    f.write('\t\t<param name="isCFS" value="true" />\n')
                else:
                    f.write('\t\t<param name="isCFS" value="false" />\n')

                f.write('\t\t<param name="period" value="' + str(PERIOD) + '" />\n')
                f.write('\t</node>\n')
            # middle node
            else:
                f.write('\t<node pkg="ros-dagsim" type="middle_node" name="' + task.name + '">\n')
                f.write('\t\t<rosparam param="parent_idx">' + str(task.parent) + '</rosparam>\n')
                f.write('\t\t<rosparam param="child_idx">' + str(task.child) + '</rosparam>\n')
                f.write('\t\t<param name="waste_time" value="' + str(task.exec_t) + '" />\n')
                f.write('\t\t<param name="period" value="' + str(PERIOD) + '" />\n')
                f.write('\t</node>\n')
        
        f.write('</launch>\n')


if __name__ == "__main__":
    dag_param_1 = {
        "task_num" : [20, 0],
        "depth" : [4.5, 0.5],
        "exec_t" : [50.0, 30.0],
        "start_node" : [1, 0],
        "extra_arc_ratio" : 0.4,
    }

    rospack = rospkg.RosPack()
    pack_path = rospack.get_path('ros-dagsim')

    os.system('source devel/setup.bash')

    with open('res.csv', 'w') as f:
        writer = csv.writer(f, delimiter=',')
        first_row = ['batch_idx', 'node_name', 'CFS_res_t', 'RT_res_t']
        writer.writerow(first_row)

    for i in range(BATCH_SIZE):
        ### Generate random DAG and set priority, affinity
        dag = DAGGen(**dag_param_1)
        configure_dag(dag)

        print(dag)

        ### Make launch file for CFS
        launch_gen(dag, isCFS=True)

        ### Start launch file
        uuid = roslaunch.rlutil.get_or_generate_uuid(None, False)
        roslaunch.configure_logging(uuid)
        launch = roslaunch.parent.ROSLaunchParent(uuid, ["src/ros-dagsim/launch/tmp.launch"])
        launch.start()

        ### RUN with CFS for 20 sec
        core_str = "0-" + str(CORE_NUM - 1)

        # 1. set dummy task
        for i in range(DUMMY_NUM):
            ps_cmd = "ps -ef | grep __name:=dummy" + str(i)
            ps_res = os.popen(ps_cmd).readline()
            main_pid = int(ps_res.split()[1])

            taskset_cmd = "taskset -pc " + core_str + " " + str(main_pid)
            os.system(taskset_cmd)

        # 2. set priority and affinity for other tasks
        for task in dag.task_set:
            ps_cmd = "ps -ef | grep __name:=" + task.name
            ps_res = os.popen(ps_cmd).readline()
            main_pid = int(ps_res.split()[1])

            taskset_cmd = "taskset -pc " + core_str + " " + str(main_pid)
            os.system(taskset_cmd)

        rospy.sleep(20)
        launch.shutdown()

        ### RUN with Ours for 20 sec       
        launch_gen(dag, isCFS=False)

        ### Start launch file
        uuid = roslaunch.rlutil.get_or_generate_uuid(None, False)
        roslaunch.configure_logging(uuid)
        launch = roslaunch.parent.ROSLaunchParent(uuid, ["src/ros-dagsim/launch/tmp.launch"])
        launch.start()

        # 1. set dummy task
        ps_cmd = "ps -ef | grep __name:=dummy"
        ps_res = os.popen(ps_cmd).readline()
        main_pid = int(ps_res.split()[1])

        taskset_cmd = "taskset -pc " + core_str + " " + str(main_pid)
        os.system(taskset_cmd)

        # 2. set priority and affinity for other tasks
        for task in dag.task_set:
            ps_cmd = "ps -ef | grep __name:=" + task.name
            ps_res = os.popen(ps_cmd).readline()
            main_pid = int(ps_res.split()[1])

            core_str = "0-" + str(CORE_NUM - 1)

            chrt_cmd = "sudo chrt -f -a -p " + str(task.priority) + " " + str(main_pid)
            os.system(chrt_cmd)
            
            # taskset_cmd = "taskset -pc " + str(task.run_core) + " " + str(main_pid)
            taskset_cmd = "taskset -pc " + core_str + " " + str(main_pid)
            os.system(taskset_cmd)

        rospy.sleep(20)
        launch.shutdown()

        ### Collect result
        with open('res.csv', 'a') as f:
            writer = csv.writer(f, delimiter=',')

            entry_cfs_file_path = pack_path + "/" + "entry_cfs.res"
            entry_rt_file_path = pack_path + "/" + "entry_rt.res"

            entry_cfs_start_time = {}
            entry_rt_start_time = {}

            with open(entry_cfs_file_path) as e_cfs:
                for line in e_cfs.readlines()[3:-3]:
                    entry_cfs_start_time[int(line.split()[0])] = \
                        float(line.split()[1])

            with open(entry_rt_file_path) as e_rt:
                for line in e_rt.readlines()[3:-3]:
                    entry_rt_start_time[int(line.split()[0])] = \
                        float(line.split()[1])
            
            os.remove(entry_cfs_file_path)
            os.remove(entry_rt_file_path)

            for task in dag.task_set:
                # Only leaf node return response time
                if len(task.child) == 0 and len(task.parent) > 0:
                    cfs_file_path = pack_path + "/" + task.name + "_cfs.res"
                    rt_file_path = pack_path + "/" + task.name + "_rt.res"

                    num = 0
                    cfs_res = 0
                    rt_res = 0

                    with open(cfs_file_path) as cfs:
                        for line in cfs.readlines()[3:-3]:
                            if int(line.split()[0]) in entry_cfs_start_time:
                                cfs_res += float(line.split()[1]) - \
                                    entry_cfs_start_time[int(line.split()[0])]
                                num += 1
                        
                        cfs_res = cfs_res / num

                    num = 0

                    with open(rt_file_path) as rt:
                        for line in rt.readlines()[3:-3]:
                            if int(line.split()[0]) in entry_rt_start_time:
                                rt_res += float(line.split()[1]) - \
                                    entry_rt_start_time[int(line.split()[0])]
                                num += 1
                        
                        rt_res = rt_res / num

                    os.remove(cfs_file_path)
                    os.remove(rt_file_path)

                    writer.writerow([i, task.name, cfs_res, rt_res])

    os.remove(pack_path + '/launch/tmp.launch')



