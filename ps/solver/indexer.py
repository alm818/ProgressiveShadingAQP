import multiprocessing as mp, psycopg, ctypes, numpy, pickle, math, io, bisect
from tdigest import TDigest
from multiprocessing import shared_memory
from ps.solver.naive import Solver
from ps.util.pg import PgManager
from ps.util.debug import deb, logger
from ps.util.misc import divide_range, upload
from ps.util.declare import *
from ps.core.predicate import BinaryOp, Variable, Constant

class Indexer(Solver):
    
    def __init__(self, aqp):
        super().__init__(aqp)
        with PgManager() as pg:
            for variable in self.aqp.where_predicate.variable_names:
                table_name, column_name = self.aqp.identify(variable)
                if pg.get_unique_column(table_name) is None:
                    logger.info(f"Start creating a unique id on table {table_name}...")
                    pg.cur.execute(f"ALTER TABLE {table_name} ADD COLUMN {table_name}_id BIGSERIAL UNIQUE;")
                    logger.info(f"Finish creating a unique id on table {table_name}")
                    pg.init()
                pg.cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {TDIGEST_TABLE} (
                    table_name TEXT NOT NULL,
                    column_name TEXT NOT NULL,
                    data BYTEA NOT NULL);
                """)
                pg.conn.commit()
                pk_name = pg.get_unique_column(table_name)
                index_name = f"{table_name}_{column_name}"
                if pg.config.getboolean("setup", "rebuild_btree"):
                    pg.cur.execute(f"DELETE FROM {TDIGEST_TABLE} WHERE table_name='{table_name}' AND column_name='{column_name}';")
                    pg.drop_table(index_name)
                if not pg.exist_table(index_name):
                    process_count = min(mp.cpu_count(), pg.get_max_connections()-1)
                    logger.info(f"Start creating BTree++ index {index_name} with {process_count} processes...")
                    intervals = divide_range(pg.get_block_count(table_name), process_count)
                    tuple_count = mp.Value(ctypes.c_longlong, 0)
                    shms = [shared_memory.SharedMemory(name=f"shm{pid}", create=True, size=SHM_MAX_SIZE) for pid in range(process_count)]
                    shm_sizes = [mp.Value('i', 0) for pid in range(process_count)]

                    def fetch_values(pid):
                        local_digest = TDigest()
                        with psycopg.connect(pg.url) as conn:
                            with psycopg.ServerCursor(conn, name="ServerCursor") as cur:
                                cur.arraysize = PG_ARRAY_SIZE
                                cur.execute(f"SELECT {column_name} FROM {table_name} WHERE ctid BETWEEN '({intervals[pid]+1},0)' AND '({intervals[pid+1]},0)';")
                                rows = cur.fetchmany()
                                while len(rows) > 0:
                                    for (val, ) in rows:
                                        local_digest.update(float(val))
                                    with tuple_count.get_lock():
                                        tuple_count.value += len(rows)
                                    rows = cur.fetchmany()
                        _, shm_sizes[pid].value = upload(shms[pid].name, local_digest)

                    processes = [mp.Process(target=fetch_values, args=(pid,)) for pid in range(process_count)]
                    [p.start() for p in processes]
                    [p.join() for p in processes]

                    logger.info("Second stage")
                    digest = TDigest(delta=TDIGEST_DELTA, K=TDIGEST_K)
                    for pid, shm in enumerate(shms):
                        digest += TDigest().update_from_dict(pickle.loads(shm.buf[:shm_sizes[pid].value]))
                    pg.cur.execute(
                        f"INSERT INTO {TDIGEST_TABLE} (table_name, column_name, data) VALUES (%s, %s, %s)",
                        (table_name, column_name, pickle.dumps(digest.to_dict()))
                    )
                    percentile_distance = TID_ARRAY_SIZE / tuple_count.value
                    bucket_count = int(math.ceil(1.0 / percentile_distance))
                    ps = [i*percentile_distance*digest.n for i in range(1, bucket_count)]
                    idx = 0
                    c_i = None
                    t = 0
                    for i, key in enumerate(digest.C.keys()):
                        c_i_plus_one = digest.C[key]
                        if i == 0:
                            k = c_i_plus_one.count / 2.
                        else:
                            k = (c_i_plus_one.count + c_i.count) / 2.
                            while idx < len(ps) and ps[idx] <= t+k:
                                z1 = ps[idx] - t
                                z2 = t + k - ps[idx]
                                ps[idx] = (c_i.mean * z2 + c_i_plus_one.mean * z1) / k
                                idx += 1
                        c_i = c_i_plus_one
                        t += k
                    while idx < len(ps):
                        ps[idx] = digest.C.max_item()[1].mean
                        idx += 1
                    shm_name = "shm"
                    shm, digest_size = upload(shm_name, digest, True)
                    tids = [[] for _ in range(bucket_count)]
                    values = [[] for _ in range(bucket_count)]
                    def fetch_tids(pid):
                        shm = shared_memory.SharedMemory(name=shm_name)
                        local_digest = TDigest().update_from_dict(pickle.loads(shm.buf[:digest_size]))
                        shm.close()
                        local_tids = [[] for _ in range(bucket_count)]
                        local_values = [[] for _ in range(bucket_count)]
                        with psycopg.connect(pg.url) as conn:
                            with psycopg.ServerCursor(conn, name="ServerCursor") as cur:
                                cur.arraysize = PG_ARRAY_SIZE
                                cur.execute(f"SELECT {pk_name},{column_name} FROM {table_name} WHERE ctid BETWEEN '({intervals[pid]+1},0)' AND '({intervals[pid+1]},0)';")
                                rows = cur.fetchmany()
                                while len(rows) > 0:
                                    for tid, val in rows:
                                        idx = bisect.bisect(ps, val)
                                        local_tids[idx].append(tid)
                                        local_values[idx].append(val)
                                    rows = cur.fetchmany()
                        _, shm_sizes[pid].value = upload(shms[pid].name, [local_tids, local_values])

                    processes = [mp.Process(target=fetch_tids, args=(pid,)) for pid in range(process_count)]
                    [p.start() for p in processes]
                    [p.join() for p in processes]
                    shm.unlink()
                    for pid, shm in enumerate(shms):
                        local_tids, local_values = pickle.loads(shm.buf[:shm_sizes[pid].value])
                        for i in range(bucket_count):
                            if len(local_tids[i]) > 0:
                                tids[i].extend(local_tids[i])
                                values[i].extend(local_values[i])
                        shm.close()
                        shm.unlink()

                    logger.info("Third stage")
                    pg.cur.execute(f"""
                        CREATE TABLE {index_name} (
                            id int,
                            tids int[],
                            values double precision[]
                    );""")
                    pg.conn.commit()
                    intervals = divide_range(bucket_count, process_count)
                    def write_tids(pid):
                        with psycopg.connect(pg.url) as conn:
                            with conn.cursor() as cur:
                                with cur.copy(f"COPY {index_name} (id, tids, values) FROM STDIN;") as copy:
                                    for i in range(intervals[pid], intervals[pid+1]):
                                        copy.write_row((i+1, tids[i], values[i]))
                    processes = [mp.Process(target=write_tids, args=(pid,)) for pid in range(process_count)]
                    [p.start() for p in processes]
                    [p.join() for p in processes]
                    pg.cur.execute(f"CREATE INDEX {index_name}_id ON {index_name} (id);")
                    pg.cur.execute(f"CLUSTER {index_name} USING {index_name}_id;")
                    pg.conn.commit()
                    logger.info(f"Finish creating BTree++ index {index_name}")
            pg.conn.commit()

    def node_solve(self, pg, node):
        if node.is_leaf():
            assert(isinstance(node.left, Variable))
            assert(isinstance(node.right, Constant))
            table_name = self.aqp.from_tables.get_roots()[0]
            column_name = node.left.name
            op = str(node.op)
            node_value = float(node.right.value)
            pg.cur.execute(f"SELECT data FROM {TDIGEST_TABLE} WHERE table_name='{table_name}' AND column_name='{column_name}'")
            tdigest = TDigest().update_from_dict(pickle.loads(pg.cur.fetchall()[0][0]))
            percentile_distance = TID_ARRAY_SIZE / tdigest.n
            bucket_id = int(max(math.ceil(tdigest.cdf(node_value) / percentile_distance), 1.0))

            index_name = f"{table_name}_{column_name}"
            tid_list = []
            if op in ['<', '<=']:
                pg.cur.execute(f"SELECT tids FROM {index_name} WHERE id<{bucket_id}")
            elif op in ['>', '>=']:
                pg.cur.execute(f"SELECT tids FROM {index_name} WHERE id>{bucket_id}")
            for row in pg.cur:
                tid_list.extend(row[0])
            pg.cur.execute(f"SELECT tids, values FROM {index_name} WHERE id='{bucket_id}'")
            tids, values = pg.cur.fetchall()[0]
            if op == '<':
                for tid, value in zip(tids, values):
                    if value < node_value:
                        tid_list.append(tid)
            elif op == '<=':
                for tid, value in zip(tids, values):
                    if value <= node_value:
                        tid_list.append(tid)
            elif op == '>':
                for tid, value in zip(tids, values):
                    if value > node_value:
                        tid_list.append(tid)
            elif op == '>=':
                for tid, value in zip(tids, values):
                    if value >= node_value:
                        tid_list.append(tid)
            return set(tid_list)
        else:
            left = self.node_solve(pg, node.left)
            right = self.node_solve(pg, node.right)
            op = node.op.value.upper()
            if op == "AND":
                return left.intersection(right)
            elif op == "OR":
                return left.union(right)
            else:
                raise TypeError(f"{op} is not supported")

    def solving(self):
        with PgManager() as pg:
            tid_set = self.node_solve(pg, self.aqp.where_predicate.ast)
            table_name = self.aqp.from_tables.get_roots()[0]
            pk_name = pg.get_unique_column(table_name)
            if len(tid_set) > 1:
                id_list = tuple(tid_set)
            else:
                id_list = f"({list(tid_set)[0]})"
            pg.cur.execute(f"SELECT {', '.join(self.aqp.select_columns)} FROM {table_name} WHERE {pk_name} IN {id_list}")
            results = []
            for row in pg.cur:
                results.append(row)
            return results