from collections import deque


class Processor:
    def __init__(self):
        self.task_allocation_queue = deque()  # <rank_u, criticality, task number, function number, AST, AFT, processor>
        self.avail = 0
