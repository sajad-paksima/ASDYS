class MS:
    def __init__(self, functions: list, criticality):
        self.functions = functions
        self.common_ready_queue = []  # <rank_u, criticality, task number, function number, AST, AFT, processor>
        self.criticality = criticality

        function_num = 0
        for function in functions:
            self.add_function(function)
            function_num += 1

    def add_function(self, function):
        function.calculate_rank(0)
        function.add_to_priority_queue()
        function.calculate_lower_bound(len(function.upward_rank) - 1)
        function.lower_bound = max(lower_bound[0] for lower_bound in function.task_lower_bound)
        self.functions.append(function)

    def is_fd_empty(self, fd):
        for common_ready_record in self.common_ready_queue:
            if common_ready_record[3] == fd:
                return False
        return len(self.functions[fd].task_priority_queue) == 0
