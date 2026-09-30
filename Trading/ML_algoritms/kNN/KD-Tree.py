from typing import List, Tuple
from collections import deque
import heapq
from random import randint

def partition(points: List, l: int, r: int, axis: int) -> int:
    random_index = randint(l, r)
    points[random_index], points[r] = points[r], points[random_index]
    
    pivot = points[r][0][axis]
    cur = l

    for i in range(l, r):
        if points[i][0][axis] < pivot:
            points[i], points[cur] = points[cur], points[i]
            cur += 1

    points[cur], points[r] = points[r], points[cur]
    return cur

def quickselect(points: List, left: int, right: int, k: int, axis: int) -> None:

    while left <= right:

        pivot = partition(points, left, right, axis)

        if pivot == k:
            return

        elif k < pivot:
            right = pivot - 1

        else:
            left = pivot + 1

class Node:
    def __init__(self, point: Tuple[int], axis: int, label='A'):
        self.point = point
        self.axis = axis
        self.label = label
        self.left = None
        self.right = None

class KD_Tree:
    def __init__(self, k: int, points: List):
        self.k = k
        self.points = points
        
    def build(self, l: int, r: int, depth: int = 0) -> Node:
        if l > r:
            return None
        
        axis = depth % self.k
        
        mid = (r - l) // 2 + l
        quickselect(self.points, l, r, mid, axis)
        
        root = Node(self.points[mid][0], axis, self.points[mid][1])
        root.left = self.build(l, mid-1, depth+1)
        root.right = self.build(mid+1, r, depth+1)
        
        return root
    
    def dist(self, point1, point2):
        res = 0
        for i in range(self.k):
             res += (point1[i] - point2[i]) ** 2
        
        return res
    
    def find_k_nearest(self, root: Node, target: tuple, k: int) -> list:
        heap = []
    
        def search(root: Node):
            
            if not root:
                return
                    
            axis = root.axis
            cur_dist = self.dist(root.point, target)
            
            if len(heap) < k:
                heapq.heappush(heap, (-cur_dist, root.point, root.label))

            elif cur_dist < -heap[0][0]:
                heapq.heappushpop(heap, (-cur_dist, root.point, root.label))
                    
            plane_dist = (target[axis] - root.point[axis]) ** 2
            if target[axis] <= root.point[axis]:
                first = root.left
                second = root.right
            else:
                first = root.right
                second = root.left

            search(first)

            if len(heap) < k or plane_dist <= -heap[0][0]:
                search(second)
        
        search(root)
        return sorted(
            [(-d, point, label) for d, point, label in heap]
        )
            
            
    def nearest_neighbor_search(self, root: Node, 
                                target: Tuple[int],
                                min_dist: int = float('inf'),
                                nearest: Tuple[int] = (0, 0)) -> Tuple[Tuple[int], int]: 
        if not root:
            return nearest, min_dist
        
        axis = root.axis
        cur_dist = self.dist(root.point, target)
        if cur_dist < min_dist:
            min_dist = cur_dist
            nearest = root.point
        
        plane_dist = (target[axis] - root.point[axis]) ** 2
        if target[axis] <= root.point[axis]:
            nearest, min_dist = self.nearest_neighbor_search(root.left, 
                                                             target, 
                                                             min_dist, 
                                                             nearest)
            
            if plane_dist <= min_dist:
                nearest, min_dist = self.nearest_neighbor_search(root.right, 
                                                                 target, 
                                                                 min_dist, 
                                                                 nearest)

            
        else:
            nearest, min_dist = self.nearest_neighbor_search(root.right, 
                                                             target, 
                                                             min_dist, 
                                                             nearest)
            
            if plane_dist <= min_dist:
                nearest, min_dist = self.nearest_neighbor_search(root.left, 
                                                                 target, 
                                                                 min_dist, 
                                                                 nearest)
        
        return nearest, min_dist
        
        
        
    def check_tree(self, node: Node, depth: int = 0, 
               mins=None, maxs=None) -> bool:

        if node is None:
            return True

        if mins is None:
            mins = [-float('inf')] * self.k

        if maxs is None:
            maxs = [float('inf')] * self.k

        axis = node.axis

        point = node.point

        # Проверяем, что точка лежит в допустимом диапазоне
        for i in range(self.k):
            if point[i] < mins[i] or point[i] > maxs[i]:
                return False

         # Левое поддерево
        left_maxs = maxs.copy()
        left_maxs[axis] = point[axis]

        # Правое поддерево
        right_mins = mins.copy()
        right_mins[axis] = point[axis]

        return (self.check_tree(node.left, depth + 1, mins, left_maxs)
            and
            self.check_tree(
                node.right,
                depth + 1,
                right_mins,
                maxs
            )
        )
        
    def print_tree(self, root: Node) -> None:
        if not root:
            return
        
        deq = deque([(root, 0)])
        
        while deq:
            node, depth = deq.popleft()
            node: Node
                        
            print(f'координата: {node.axis}, уровень: {depth}', end='\n')
            print(node.point, end='\n')
            
            for child in (node.left, node.right):
                if child:
                    deq.append((child, depth+1))
    
 
if __name__ == '__main__':
    points = [
    ((2, 3), "A"),
    ((5, 4), "B"),
    ((9, 6), "A"),
    ((7, 4), "B"),
    ((10, 3), "B"),
    ((4, 2), "A"),
    ]
    
    Q = (6, 7)
    
    tree = KD_Tree(2, points)
    root = tree.build(0, len(points)-1)
    k_nearest = tree.find_k_nearest(root, Q, 3)
    print(f'Корректно ли дерево: {tree.check_tree(root)}', end='\n')
    print('===СТРУКТУРА===')
    tree.print_tree(root)
    print(f'===БЛИЖАЙШИЕ СОСЕДИ ДЛЯ {Q}===')
    print(f'Соседи (расстояние, точка, класс): {k_nearest}')