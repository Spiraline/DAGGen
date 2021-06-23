# DAGGen
Random DAG Generator

### Usage

```
python3 dag_gen.py
```

### Parameter

* `node_num` ([mean, dev]): set the number of nodes between `[mean-dev, mean+dev]`
* `depth` ([mean, dev]): set the depth of DAG between `[mean-dev, mean+dev]`
* `start_node` ([mean, dev]): set the number of start node (entry node) between `[mean-dev, mean+dev]`
* `exec_t` ([mean, dev]): set the execution time of task between `[mean-dev, mean+dev]`
* `edge_constraint` (bool): If True, use outbound_num as the number of outbound edge. Else, use extra_arc_ratio to 
* `outbound_num` ([mean, dev]): set the number of outbound edge (entry node) between `[mean-dev, mean+dev]`.
* `extra_arc_ratio` (float): make `(task_num) * (extra_arc_ratio)` of extra arc

Deadline of leaf node is set as `level+1 * (exec_t mean) * 2`


---

### Experimental Usage

```
python experiment.py --dag_num=1000 --iter_size=100 --sl_unit=2 --base='100,200,300' --experiments='None'
```

### Experimental Parameter

* `dag_num` (integer): How many dags to generate and experiment with.
* `iter_size` (integer): How many iteration to test per 1 DAG.
* `base` ([int, int]): Maximum loop count of base method.`[base small, base large]`
* `experiment` (str): Save experiment result or not. (`acc`, `std`, `density`, `None`)
* `sl_unit` (float): Execution unit time of Self-Looping node.
* Please refer experiment.py file to more detailed parameter.