from abc import ABC, abstractmethod

class Solver:
    
    def __init__(self, aqp):
        self.aqp = aqp

    @abstractmethod
    def solve(self):
        pass