import numpy as np
from processor import Processor
from ms import MS
from function import Function
from myheapq import heap_push, heap_pop

INFINITY = np.iinfo(np.int32).max


def find_new_arrived_functions(functions_list):
    global system_time, step
    new_arrived_functions = []
    for function_record in functions_list:
        if 0 <= system_time - function_record.arrival_time < step:
            new_arrived_functions.append(function_record)
    return new_arrived_functions


def task_priority_queues_is_empty(total_system: MS):
    for func in total_system.functions:
        if len(func.task_priority_queue) != 0:
            return False
    return True


def has_lower_criticality_tasks(max_criticality, processor_list: list):
    for pr in processor_list:
        pr: Processor
        for item in pr.task_allocation_queue:
            if item[1] < max_criticality:
                return True
    return False


processor_1 = Processor()
processor_2 = Processor()
processor_3 = Processor()
processors_list = [processor_1, processor_2, processor_3]

communication_cost_1 = np.array([[0, 10, 2, 7, INFINITY, INFINITY, INFINITY],
                                 [10, 0, INFINITY, INFINITY, 5, 6, INFINITY],
                                 [2, INFINITY, 0, INFINITY, 6, 9, INFINITY],
                                 [7, INFINITY, INFINITY, 0, INFINITY, 4, INFINITY],
                                 [INFINITY, 5, 6, INFINITY, 0, INFINITY, 2],
                                 [INFINITY, 6, 9, 4, INFINITY, 0, 7],
                                 [INFINITY, INFINITY, INFINITY, INFINITY, 2, 7, 0]])
computation_cost_1 = np.array([[18, 5, 11],
                               [5, 12, 20],
                               [23, 20, 20],
                               [13, 19, 11],
                               [5, 9, 19],
                               [15, 22, 19],
                               [11, 19, 24]])
edges_1 = np.array([[0, 1, 1, 1, 0, 0, 0],
                    [0, 0, 0, 0, 1, 1, 0],
                    [0, 0, 0, 0, 1, 1, 0],
                    [0, 0, 0, 0, 0, 1, 0],
                    [0, 0, 0, 0, 0, 0, 1],
                    [0, 0, 0, 0, 0, 0, 1],
                    [0, 0, 0, 0, 0, 0, 0]])

communication_cost_2 = np.array([[0, 8, 2, INFINITY, INFINITY],
                                 [8, 0, INFINITY, 6, 7],
                                 [2, INFINITY, 0, INFINITY, 4],
                                 [INFINITY, 6, INFINITY, 0, INFINITY],
                                 [INFINITY, 7, 4, INFINITY, 0]])
computation_cost_2 = np.array([[22, 7, 7],
                               [24, 23, 12],
                               [9, 10, 9],
                               [19, 22, 23],
                               [5, 7, 21]])
edges_2 = np.array([[0, 1, 1, 0, 0],
                    [0, 0, 0, 1, 1],
                    [0, 0, 0, 0, 1],
                    [0, 0, 0, 0, 0],
                    [0, 0, 0, 0, 0]])

communication_cost_3 = np.array([[0, 4, 8, INFINITY, INFINITY, INFINITY, INFINITY],
                                 [4, 0, INFINITY, 2, 4, 9, INFINITY],
                                 [8, INFINITY, 0, INFINITY, 8, 8, INFINITY],
                                 [INFINITY, 2, INFINITY, 0, INFINITY, INFINITY, 3],
                                 [INFINITY, 4, 8, INFINITY, 0, INFINITY, 7],
                                 [INFINITY, 9, 8, INFINITY, INFINITY, 0, 5],
                                 [INFINITY, INFINITY, INFINITY, 3, 7, 5, 0]])
computation_cost_3 = np.array([[6, 16, 23],
                               [13, 13, 5],
                               [17, 14, 15],
                               [13, 14, 9],
                               [15, 19, 23],
                               [9, 23, 13],
                               [18, 21, 19]])
edges_3 = np.array([[0, 1, 1, 0, 0, 0, 0],
                    [0, 0, 0, 1, 1, 1, 0],
                    [0, 0, 0, 0, 1, 1, 0],
                    [0, 0, 0, 0, 0, 0, 1],
                    [0, 0, 0, 0, 0, 0, 1],
                    [0, 0, 0, 0, 0, 0, 1],
                    [0, 0, 0, 0, 0, 0, 0]])


# communication_cost_4 = np.array([[0, 2, 11],
#                                 [2, 0, 2],
#                                 [11, 2, 0]])
# computation_cost_4 = np.array([[8, 13, 18],
#                               [20, 7, 11],
#                               [20, 15, 8]])
# edges_4 = np.array([[0, 1, 1],
#                     [0, 0, 1],
#                     [0, 0, 0]])


f_d = -1
previous_round = []
current_round = []
system_time = 0
step = 5

function_1 = Function(processors_list, n=len(edges_1), criticality=0, communication_cost=communication_cost_1,
                      computation_time=computation_cost_1, edges=edges_1, arrival_time=0, deadline=100)
function_2 = Function(processors_list, n=len(edges_2), criticality=1, communication_cost=communication_cost_2,
                      computation_time=computation_cost_2, edges=edges_2, arrival_time=25, deadline=125)
function_3 = Function(processors_list, n=len(edges_3), criticality=2, communication_cost=communication_cost_3,
                      computation_time=computation_cost_3, edges=edges_3, arrival_time=50, deadline=150)
# function_4 = Function(processors_list, n=3, criticality=2, communication_cost=communication_cost_4,
#                       computation_time=computation_cost_4, edges=edges_4, arrival_time=20, deadline=34)
functions_list = [function_1, function_2, function_3]
ms = MS(functions=[], criticality=0)


while True:
    print(f'Time is {system_time}')
# handle new function
    new_arrived_function = find_new_arrived_functions(functions_list)
    for function_obj in new_arrived_function:
        print(f"Added new function with computation_cost {function_obj.computation_cost}")
        ms.add_function(function_obj)
# remove lower critical tasks from task_allocation_queue and common ready queue
    if len(new_arrived_function) != 0:
        max_criticality_in_new_arrived_function = max([func.criticality for func in new_arrived_function])
        if has_lower_criticality_tasks(max_criticality_in_new_arrived_function, processors_list):
            for pr in processors_list:
                pr: Processor
                print(f"processor task_allocation_queue is:\n"
                      f"{pr.task_allocation_queue}")
                candidate_items = [task for task in pr.task_allocation_queue if task[1] < max_criticality_in_new_arrived_function and task[4] >= system_time]
                print(f"candidate items from task_allocation_queue of processor {processors_list.index(pr)} to be removed is:\n"
                      f"{candidate_items}")
                for item in candidate_items:
                    pr.task_allocation_queue.remove(item)
                    item[4], item[5], item[6] = None, None, None
                    item[3].AFT[item[2]] = None
                    item[3].assigned_processor[item[2]] = -1
                    heap_push(item[3].task_priority_queue, item)
                print(f"final processor task_allocation_queue is:\n"
                      f"{pr.task_allocation_queue}")
            while len(ms.common_ready_queue) != 0:
                common_ready_queue_record = heap_pop(ms.common_ready_queue)
                print(f"pop common_ready_queue record {common_ready_queue_record} in handle new arrived")
                common_ready_queue_record[0], common_ready_queue_record[1] = common_ready_queue_record[1], common_ready_queue_record[0]
                heap_push(common_ready_queue_record[3].task_priority_queue, common_ready_queue_record)

    while not task_priority_queues_is_empty(ms):
        previous_round = current_round.copy()
        current_round = []
# fill common ready queue
        for function_obj in ms.functions:
            if function_obj.criticality < ms.criticality:
                continue
            criticality_slack = function_obj.criticality - ms.criticality
            n_max = Function.get_n_max(criticality_slack)
            cnt = n_max
            while cnt != 0 and len(function_obj.task_priority_queue) != 0:
                cnt -= 1
                tuple_record = heap_pop(function_obj.task_priority_queue)
                print(f'pop task priority queue: {tuple_record}')
                tuple_record[0], tuple_record[1] = tuple_record[1], tuple_record[0]
                heap_push(ms.common_ready_queue, tuple_record)
                print(f'ms.common_ready_queue: {ms.common_ready_queue}')
# handle common ready queue
        while len(ms.common_ready_queue) != 0:
            tuple_record = heap_pop(ms.common_ready_queue)
            task_num, function = tuple_record[2], tuple_record[3]
            min_EFT, suitable_processor = (min([function.get_EFT(task_num, k) for k in range(len(processors_list))]),
                                           np.argmin([function.get_EFT(task_num, k) for k in range(len(processors_list))]))
            tuple_record[0], tuple_record[1] = tuple_record[1], tuple_record[0]
            print(f'trying to assign common_ready_queue record {tuple_record} to processor {suitable_processor}'
                  f'function_number {ms.functions.index(function)}, task_number {task_num}, min_EFT {min_EFT}, suitable_processor {suitable_processor}')
            if min_EFT > function.get_task_abs_deadline(task_num) and function.criticality > ms.criticality:
                ms.criticality = function.criticality
                f_d = function
                print(f'function that increases system criticality: {ms.functions.index(f_d)}')
# remove from common ready queue and task allocation queue in current and previous round
                while len(ms.common_ready_queue) != 0:
                    common_ready_queue_record = heap_pop(ms.common_ready_queue)
                    print(f"pop common_ready_queue record {common_ready_queue_record} in deadline miss event")
                    common_ready_queue_record[0], common_ready_queue_record[1] = common_ready_queue_record[1], common_ready_queue_record[0]
                    heap_push(common_ready_queue_record[3].task_priority_queue, common_ready_queue_record)
                for round_record in previous_round + current_round:
                    if system_time < round_record[4] and round_record in processors_list[round_record[6]].task_allocation_queue:
                        print(f"pop {round_record} from task_allocation_queue of {round_record[6]} in deadline miss event")
                        processors_list[round_record[6]].task_allocation_queue.remove(round_record)
                        ms.functions[round_record[3]].AFT[round_record[2]] = None
                        round_record[4], round_record[5], round_record[6] = None, None, None
                        ms.functions[round_record[3]].assigned_processor[round_record[2]] = -1
                        heap_push(round_record[3].task_priority_queue, round_record)
                        print(f"push {round_record} into task_priority_queue of {round_record[3]} in deadline miss event")
                print(f"failed to assign common_ready_queue record {tuple_record} to processor {suitable_processor}")
# assign task to processor with min EFT
            else:
                tuple_record[6] = suitable_processor  # processor
                tuple_record[5] = min_EFT  # AFT
                tuple_record[4] = min_EFT - function.computation_cost[task_num][suitable_processor]  # AST
                processors_list[suitable_processor].task_allocation_queue.append(tuple_record)
                tuple_record[3].AFT[tuple_record[2]] = min_EFT
                tuple_record[3].assigned_processor[tuple_record[2]] = suitable_processor
                current_round.append(tuple_record)

                print(f"successfully assign common_ready_queue record {tuple_record} to processor {suitable_processor}")
                if ms.is_fd_empty(f_d):
                    print(f"function {f_d} which cause system criticality to rise is finished")
                    f_d = -1
                    ms.criticality = 0
    if system_time == 125:
        break
    system_time += step
