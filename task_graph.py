import numpy as np


class Node:
    def __init__(self, index, predecessor=None, successor=None):
        self.predecessor = predecessor
        self.successor = successor
        self.index = index


def find_successor(node: Node):
    return [success_node.index for success_node in node.successor]


# gaussian elimination algorithm (ignore setting predecessor)
def create_gaussian_elimination_task_graph(n):
    edges = np.zeros(((n + 2) * (n - 1) // 2, (n + 2) * (n - 1) // 2))
    nodes = []
    nodes.append(Node(index=0, predecessor=[], successor=[]))
    for i in range(1, n):
        nodes.append(Node(index=i, predecessor=[], successor=[]))
    nodes[0].successor.extend(nodes[1:n])
    for i in range(n - 1, 1, -1):
        candidate_nodes = nodes[-i:]
        for nod in candidate_nodes:
            nodes.append(Node(index=nod.index + i, predecessor=[], successor=[]))
            nod.successor.extend([nodes[nod.index + i]])
        nodes[-i].successor.extend(nodes[-i + 1:])
    for i in range((n + 2) * (n - 1) // 2):
        successors_list = find_successor(nodes[i])
        for j in successors_list:
            edges[i][j] = 1
    return edges


# fast fourier transform algorithm (ignore setting predecessor)
def create_FFT_task_graph(n):
    edges = np.zeros((int(2 * n - 1 + n * np.log2(n)), int(2 * n - 1 + n * np.log2(n))))
    nodes = []
    nodes.append(Node(index=0, predecessor=[], successor=[]))
    for i in range(1, 2 * n - 1):
        nodes.append(Node(index=i, predecessor=[nodes[int((i - 0.5) // 2)]], successor=[]))
    for i in range(n - 1):
        nodes[i].successor.extend(nodes[2 * i + 1:2 * i + 3])
    for i in range(2 * n - 1, int(2 * n - 1 + n * np.log2(n))):
        nodes.append(Node(index=i, predecessor=[], successor=[]))
    d = 1
    index = n - 1
    while d < n:
        for i in range(0, n // d):
            if i % 2 == 0:
                for j in range(index + i * d, index + i * d + d):
                    nodes[j].successor.extend([nodes[j + n], nodes[j + n + d]])
            else:
                for j in range(index + i * d, index + i * d + d):
                    nodes[j].successor.extend([nodes[j + n], nodes[j + n - d]])
        index += n
        d *= 2
    for i in range(int(2 * n - 1 + n * np.log2(n))):
        successors_list = find_successor(nodes[i])
        for j in successors_list:
            edges[i][j] = 1
    return edges
