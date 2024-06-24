import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ps.core.aqp import AQP
from ps.util.pg import PgManager
from ps.util.debug import deb
from ps.solver.indexer import Indexer

def main():
    query = "SELECT l_orderkey,l_partkey,l_suppkey FROM lineitem WHERE l_quantity < 20.1 AND l_extendedprice > 41800;"
    aqp = AQP(query)
    # aqp.print()
    with PgManager() as pg:
        solver = Indexer(aqp)
        solver.solve()

if __name__ == "__main__":
    main()
