import time
from abc import ABC, abstractmethod
from ps.util.pg import PgManager

class Solver:
    
    def __init__(self, aqp):
        self.aqp = aqp

    @abstractmethod
    def solving(self):
        pass

    def solve(self):
        start_time = time.time()
        results = self.solving()
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"{self.__class__.__name__} solves function took {elapsed_time:.6f} seconds to complete.")
        return results

class Naive(Solver):

    def __init__(self, aqp):
        super().__init__(aqp)

    def solving(self):
        with PgManager() as pg:
            pg.cur.execute("SET max_parallel_workers_per_gather = 0;")
            pg.cur.execute(self.aqp.query)
            results = []
            for row in pg.cur:
                results.append(row)
            return results