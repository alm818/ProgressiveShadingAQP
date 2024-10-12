import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ps.core.aqp import AQP
from ps.util.pg import PgManager
from ps.util.debug import deb
from ps.solver.indexer import Indexer
from ps.solver.progressive import Progressive
from ps.solver.naive import Naive

def main():
    query = "SELECT l_orderkey, l_partkey, l_suppkey FROM lineitem WHERE l_quantity < 20.1 AND l_extendedprice > 41970"
    aqp = AQP(query)

    solver = Progressive(aqp)
    sol = solver.solve(leaf_size=100)
    deb(sol)
    
    # solver = Indexer(aqp)
    # sol = solver.solve()
    # deb(sol)

    # solver = Naive(aqp)
    # sol = solver.solve()
    # deb(sol)

if __name__ == "__main__":
    main()
