import sys, os
# import numpy as np
# from spn.structure.Base import Context
# from spn.structure.StatisticalTypes import MetaType
# from spn.algorithms.LearningWrappers import learn_mspn
# from spn.algorithms.Inference import log_likelihood

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ps.core.aqp import AQP
from ps.util.pg import PgManager
from ps.util.debug import deb
from ps.solver.indexer import Indexer
from ps.solver.progressive import Progressive
from ps.solver.naive import Naive

def main():
    # np.random.seed(42)
    # n_rows = 1000
    # n_cols = 3
    # data = np.empty((n_rows, n_cols))

    # subset_size = n_rows // 10

    # for i in range(10):
    #     start_idx = i * subset_size
    #     end_idx = (i + 1) * subset_size
    #     x_values = np.random.normal(loc=i, scale=1, size=subset_size)
    #     i_values = np.full(subset_size, i)
    #     y_values = np.random.normal(loc=-i, scale=1, size=subset_size)
    #     data[start_idx:end_idx] = np.column_stack((x_values, i_values, y_values))

    # np.random.shuffle(data)
    # a = np.random.randint(2, size=1000).reshape(-1, 1)
    # b = np.random.randint(3, size=1000).reshape(-1, 1)
    # c = np.r_[np.random.normal(10, 5, (300, 1)), np.random.normal(20, 10, (700, 1))]
    # d = 5 * a + 3 * b + c
    # data = np.c_[a, b, c, d]
    # print(data.shape)

    # ds_context = Context(meta_types=[MetaType.DISCRETE, MetaType.DISCRETE, MetaType.REAL, MetaType.REAL])
    # ds_context = Context(meta_types=[MetaType.REAL, MetaType.DISCRETE, MetaType.REAL])
    # ds_context.add_domains(data)
    # mspn = learn_mspn(data, ds_context, min_instances_slice=5)
    # print("Finished learning")
    # cc = 1
    # vvx = 0.5
    # vvy = -0.5
    # test_data = np.array([vvx, cc, vvy]).reshape(-1, 3)
    # ll = log_likelihood(mspn, test_data)
    # print(n_rows*np.exp(ll))
    # count = 0
    # for vx, c, vy in data:
    #     if vx <= vvx and c == cc and vy <= vvy:
    #         count += 1
    # print(count)

    query = "SELECT l_orderkey, l_partkey, l_suppkey FROM lineitem WHERE l_quantity < 20.1 AND l_extendedprice > 41800"
    aqp = AQP(query)

    solver = Progressive(aqp)
    sol = solver.solve(leaf_size=1000)
    deb(sol)
    
    # solver = Indexer(aqp)
    # sol = solver.solve()
    # deb(sol)

    # solver = Naive(aqp)
    # sol = solver.solve()
    # deb(sol)

if __name__ == "__main__":
    main()
