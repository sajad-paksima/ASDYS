import numpy as np
from myheapq import heap_push, heap_pop

INFINITY = np.iinfo(np.int32).max


class Function:
    def __init__(self, processors: list, n, criticality, communication_cost, computation_time, edges, arrival_time, deadline):
        self.processors = processors
        self.communication_cost = communication_cost
        self.computation_cost = computation_time
        self.edges = edges
        self.task_priority_queue = []  # <rank_u, criticality, task number, function number, AST, AFT, processor>
        self.criticality = criticality
        self.upward_rank = np.zeros(n)
        self.arrival_time = arrival_time
        self.deadline = deadline
        self.task_lower_bound = [(0, 0) for _ in range(n)]
        self.AFT = np.zeros(n)
        self.assigned_processor = -np.ones(n)
        self.lower_bound = 0

    def get_EFT(self, task, k):
        max_pred_constraint = self.find_max_AFT_plus_comcost(task, k)
        arrival = self.arrival_time if task == 0 else 0
        start_time = max_pred_constraint + arrival
        for task_record in self.processors[k].task_allocation_queue:
            available_slack = task_record[5] - task_record[3].computation_cost[task_record[2]][k] - start_time
            if available_slack >= self.computation_cost[task][k]:
                break
            start_time = max(task_record[5], max_pred_constraint + arrival)
        if self.computation_cost[task][k] == INFINITY:
            return INFINITY
        return start_time + self.computation_cost[task][k]

    def get_EST(self, task, k):
        return max(self.processors[k].avail, self.find_max_AFT_plus_comcost(task, k)) + self.arrival_time

    def find_max_AFT_plus_comcost(self, task, k):
        candidate_max = 0
        for pred_task in self.get_predecessor(task):
            comcost = self.communication_cost[pred_task][task] if self.assigned_processor[pred_task] != k else 0
            if candidate_max < self.AFT[pred_task] + comcost:
                candidate_max = self.AFT[pred_task] + comcost
        return candidate_max

    def get_abs_deadline(self):
        return self.arrival_time + self.deadline

    def get_task_abs_deadline(self, task):
        return self.arrival_time + self.calculate_lower_bound(task)[0] + self.get_deadline_slack()

    def get_deadline_slack(self):
        return self.deadline - self.lower_bound

    def calculate_lower_bound(self, task):
        if self.task_lower_bound[task][0] != 0:
            return self.task_lower_bound[task]
        if task == 0:
            self.task_lower_bound[task] = (np.min(self.computation_cost[task]), np.argmin(self.computation_cost[task]))
            return self.task_lower_bound[task]
        previous_task_lower_bound = self.calculate_lower_bound(task - 1)
        if self.communication_cost[task - 1][task] == INFINITY:
            min_next_task = self.computation_cost[task][previous_task_lower_bound[1]]
        else:
            min_next_task = min(self.computation_cost[task][previous_task_lower_bound[1]], self.communication_cost[task - 1][task] + np.min(self.computation_cost[task]))
        if min_next_task == self.computation_cost[task][previous_task_lower_bound[1]]:
            min_processor = previous_task_lower_bound[1]
        else:
            min_processor = np.argmin(self.computation_cost[task])
        self.task_lower_bound[task] = (previous_task_lower_bound[0] + min_next_task, min_processor)
        return self.task_lower_bound[task]

    def get_successor(self, task):
        return list(np.where(self.edges[task] == 1)[0])

    def get_predecessor(self, task):
        return list(np.where(np.transpose(self.edges)[task] == 1)[0])

    def get_average_computation_time(self, task):
        avg = 0
        count = 0
        for wcet in self.computation_cost[task]:
            if wcet != INFINITY:
                avg = (avg * count + wcet) / (count + 1)
                count += 1
        return avg

    def calculate_rank(self, task):
        if self.upward_rank[task] != 0:
            return self.upward_rank[task]

        _max = self.find_max_successor_computation_time(task)

        self.upward_rank[task] = self.get_average_computation_time(task) + _max
        return self.upward_rank[task]

    def find_max_successor_computation_time(self, task):
        _max = 0
        for successor_task in self.get_successor(task):
            candidate_max = self.communication_cost[task][successor_task] + self.calculate_rank(successor_task)
            if candidate_max > _max:
                _max = candidate_max
        return _max

    def add_to_priority_queue(self):
        for task_num in range(len(self.upward_rank)):
            heap_push(self.task_priority_queue, [self.upward_rank[task_num], self.criticality, task_num, self, None, None, None])
            # self.task_priority_queue.put((self.upward_rank[task_num], self.criticality, task_num, function_num))

    @staticmethod
    def get_n_max(criticality_slack):
        return criticality_slack + 1
