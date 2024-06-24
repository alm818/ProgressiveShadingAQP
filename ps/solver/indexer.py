from ps.solver.naive import Solver
from ps.util.pg import PgManager

class Indexer(Solver):
    
    def __init__(self, aqp):
        super().__init__(aqp)

    def solve(self):
        with PgManager() as pg:
            for name in self.aqp.where_predicate.variable_names:
                pg.create_index(*self.aqp.identify(name))