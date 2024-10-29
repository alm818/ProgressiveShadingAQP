import subprocess, math, psycopg, asyncio, heapq
import numpy as np

from ps.util.pg import PgManager
from ps.util.debug import deb, logger
from ps.solver.naive import Solver
from ps.core.histogram import Stat, Histogram
from ps.core.predicate import AND, OR, is_op_less, is_op_greater

PG_ARRAY_SIZE = 1000000
# NUM_PRODUCERS = 4

# async def producer(producer_id, pg_url, sql, queue):
#     async with await psycopg.AsyncConnection.connect(pg_url) as conn:
#         async with conn.cursor(name=f"ServerCursor_{producer_id}") as cur:
#             cur.arraysize = PG_ARRAY_SIZE
#             await cur.execute(sql)
#             while True:
#                 rows = await cur.fetchmany()
#                 if not rows:
#                     break
#                 await queue.put(rows)
#                 print(f"Put {len(rows)} rows")
#             await queue.put(None)

# async def histogram_consumer(queue, histograms, max_sizes, producer_count):
#     producers_done = 0
#     while True:
#         rows = await queue.get()
#         if rows is None:
#             producers_done += 1
#             if producers_done == producer_count:
#                 break
#             else:
#                 continue
#         for row in rows:
#             for j in range(len(histograms)):
#                 histograms[j][row[0] // max_sizes[j]].update(np.array(row[1:], dtype=float))
#         print(f"Processed {len(rows)} rows")

# async def stat_consumer(queue, stats, max_sizes, producer_count):
#     producers_done = 0
#     while True:
#         rows = await queue.get()
#         if rows is None:
#             producers_done += 1
#             if producers_done == producer_count:
#                 break
#             else:
#                 continue
#         for row in rows:
#             for j in range(len(stats)):
#                 stats[j][row[0] // max_sizes[j]].update(np.array(row[1:], dtype=float))
#         print(f"Processed {len(rows)} rows")

class PivotHistogram:

    def __init__(self, pg, histogram_table, layer_index, group_index):
        self.pg = pg
        self.histogram_table = histogram_table
        self.layer_index, self.group_index = layer_index, group_index
        self.pg.cur.execute(f"SELECT count, columns, mins, maxs, one_count_0 FROM {histogram_table} WHERE layer_index={layer_index} AND group_index={group_index}")
        self.n, self.columns, self.mins, self.maxs, self.pivot_count = self.pg.cur.fetchone()
        self.pivot_count = np.diff(np.array(self.pivot_count), prepend=0)
        self.pg.cur.execute(f"SELECT {','.join([f'array_length(one_count_{i}, 1)' for i in range(len(self.columns))])} FROM {histogram_table} WHERE layer_index={layer_index} AND group_index={group_index}")
        self.bin_counts = np.array(self.pg.cur.fetchone())

    def get_two_count_row(self, column_index, row_index):
        self.pg.cur.execute(f"SELECT two_count_{column_index}[{row_index+1}:{row_index+1}] FROM {self.histogram_table} WHERE layer_index={self.layer_index} AND group_index={self.group_index}")
        # print(f"SELECT two_count_{column_index}[{row_index+1}:{row_index+1}] FROM {self.histogram_table} WHERE layer_index={self.layer_index} AND group_index={self.group_index}")
        # tmp = self.pg.cur.fetchone()[0][0]
        # print(tmp)
        # return np.array(tmp)
        return np.array(self.pg.cur.fetchone()[0][0])

class Progressive(Solver):

    def __init__(self, aqp):
        super().__init__(aqp)

    def estimate_prob_recurse(self, node, pivot_hist):
        op = str(node.op)
        if not node.is_condition():
            left = self.estimate_prob_recurse(node.left, pivot_hist)
            right = self.estimate_prob_recurse(node.right, pivot_hist)
            # deb(left, right, pivot_hist.mins, pivot_hist.maxs)
            if op == AND:
                return left * right
            elif op == OR:
                return 1 - (1 - left) * (1 - right)
            else:
                raise Exception(f"Invalid op {node.op}")
        else:
            column_name = node.left.name
            column_index = pivot_hist.columns.index(column_name)
            value = float(node.right.value)
            if value < pivot_hist.mins[column_index]:
                le_prob = np.zeros(pivot_hist.bin_counts[0])
            elif value > pivot_hist.maxs[column_index]:
                le_prob = np.ones(pivot_hist.bin_counts[0])
            else:
                if pivot_hist.maxs[column_index] == pivot_hist.mins[column_index]:
                    bin_index = 0
                    frac = 1
                else:
                    temp = (value - pivot_hist.mins[column_index]) / (pivot_hist.maxs[column_index] - pivot_hist.mins[column_index]) * pivot_hist.bin_counts[column_index]
                    bin_index = int(min(temp, pivot_hist.bin_counts[column_index] - 1))
                    frac = temp - bin_index
                    # deb(pivot_hist.mins[column_index], pivot_hist.maxs[column_index])
                if column_index == 0:
                    le_prob = np.array([1 for i in range(bin_index)] + [frac] + [0 for i in range(pivot_hist.bin_counts[column_index] - 1 - bin_index)])
                else:
                    low = np.zeros(pivot_hist.bin_counts[0])
                    if bin_index > 0:
                        low = pivot_hist.get_two_count_row(column_index, bin_index - 1)
                    low = np.diff(low, prepend=0)
                    high = np.diff(pivot_hist.get_two_count_row(column_index, bin_index), prepend=0)
                    # deb(pivot_hist.columns, column_index, bin_index, pivot_hist.bin_counts[column_index], low, high, frac, (low + (high - low) * frac), pivot_hist.pivot_count)
                    count = low + (high - low) * frac
                    le_prob = np.divide(count, pivot_hist.pivot_count, out=np.zeros_like(count), where=pivot_hist.pivot_count!=0)
            if is_op_less(op):
                return le_prob
            elif is_op_greater(op):
                return 1 - le_prob
            else:
                raise Exception(f"Op {op} not implemented yet")

    def estimate(self, layer_index, group_index, option):
        table_name = self.aqp.get_table_name()
        histogram_table = f"{table_name}_histogram"
        with PgManager() as pg:
            pg.cur.execute(f"SELECT COUNT(*) FROM {histogram_table} WHERE layer_index={layer_index} AND group_index={group_index}")
            if pg.cur.fetchone()[0] == 0:
                return None
            pivot_hist = PivotHistogram(pg, histogram_table, layer_index, group_index)
            prob = self.estimate_prob_recurse(self.aqp.where_predicate.ast, pivot_hist)
            # tmp = np.sum(prob * pivot_hist.pivot_count) / pivot_hist.n
            # deb(prob, pivot_hist.pivot_count, pivot_hist.n, tmp)
            if option['func'] == 'PROBABILITY':
                return np.sum(prob * pivot_hist.pivot_count) / pivot_hist.n
            elif option['func'] == 'COUNT':
                return np.sum(prob * pivot_hist.pivot_count)
            else:
                raise Exception(f"Func {option['func']} not implemented yet")


    def solving(self, leaf_size):
        table_name = self.aqp.get_table_name()
        histogram_table = f"{table_name}_histogram"
        with PgManager() as pg:
            partition_column = pg.config["pgmanager"]["partition_column"]
            info = pg.get_partitioning_info(table_name)
            make_histogram = pg.config.getboolean("setup", "rebuild_histogram") or not pg.exist_table(histogram_table)
            if pg.config.getboolean("setup", "rebuild_dstree") or info is None or info[1] != leaf_size:
                logger.info(f"Start partitioning {table_name}...")
                process = subprocess.Popen(['bash', 'cpp_run.sh', table_name, str(leaf_size), f'"l_quantity l_extendedprice"'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                for line in process.stdout:
                    print(line.strip())
                info = pg.get_partitioning_info(table_name)
                make_histogram = True

            # columns = list(set(pg.get_numeric_columns(table_name)) - set(pg.get_all_keys(table_name) + [pg.config["pgmanager"]["partition_column"]]))
            columns, leaf_size, partition_count = info
            df = leaf_size
            layer_count = math.ceil(math.log(partition_count, df))

            if make_histogram:
                stats = []
                max_sizes = [df**j for j in range(layer_count)]
                for j in range(layer_count):
                    group_count = int(math.ceil(partition_count / max_sizes[j]))
                    stats.append([Stat(columns, max_sizes[j]) for i in range(group_count)])
                # async def stat_main():
                #     queue = asyncio.Queue(maxsize=NUM_PRODUCERS)
                #     producer_tasks = []
                #     for i in range(NUM_PRODUCERS):
                #         sql = f"SELECT {partition_column},{','.join(columns)} FROM {table_name} WHERE {table_name}_id >= {i*5*PG_ARRAY_SIZE} ORDER BY {table_name}_id LIMIT {PG_ARRAY_SIZE}"
                #         producer_tasks.append(asyncio.create_task(producer(i, pg.url, sql, queue)))
                #     consumer_task = asyncio.create_task(stat_consumer(queue, stats, max_sizes, NUM_PRODUCERS))
                #     await asyncio.gather(*producer_tasks, consumer_task)
                # asyncio.run(stat_main())
                row_count = 0
                with psycopg.connect(pg.url) as conn:
                    with psycopg.ServerCursor(conn, name="ServerCursor") as cur:
                        cur.arraysize = PG_ARRAY_SIZE
                        # cur.execute(f"SELECT {partition_column},{','.join(columns)} FROM {table_name} ORDER BY {table_name}_id LIMIT {PG_ARRAY_SIZE}")
                        cur.execute(f"SELECT {partition_column},{','.join(columns)} FROM {table_name}")
                        rows = cur.fetchmany()
                        while len(rows) > 0:
                            for row in rows:
                                for j in range(len(stats)):
                                    stats[j][row[0] // max_sizes[j]].update(np.array(row[1:], dtype=float))
                            row_count += len(rows)
                            print(f"Phase 1: {row_count}/{partition_count*leaf_size}")
                            rows = cur.fetchmany()

                histograms = [[Histogram(stat) for stat in layer_stat] for layer_stat in stats]
                # async def histogram_main():
                #     queue = asyncio.Queue(maxsize=NUM_PRODUCERS)
                #     producer_tasks = []
                #     for i in range(NUM_PRODUCERS):
                #         sql = f"SELECT {partition_column},{','.join(columns)} FROM {table_name} WHERE {table_name}_id >= {i*5*PG_ARRAY_SIZE} ORDER BY {table_name}_id LIMIT {PG_ARRAY_SIZE}"
                #         producer_tasks.append(asyncio.create_task(producer(i, pg.url, sql, queue)))
                #     consumer_task = asyncio.create_task(histogram_consumer(queue, histograms, max_sizes, NUM_PRODUCERS))
                #     await asyncio.gather(*producer_tasks, consumer_task)
                # asyncio.run(histogram_main())
                row_count = 0
                with psycopg.connect(pg.url) as conn:
                    with psycopg.ServerCursor(conn, name="ServerCursor") as cur:
                        cur.arraysize = PG_ARRAY_SIZE
                        # cur.execute(f"SELECT {partition_column},{','.join(columns)} FROM {table_name} ORDER BY {table_name}_id LIMIT {PG_ARRAY_SIZE}")
                        cur.execute(f"SELECT {partition_column},{','.join(columns)} FROM {table_name}")
                        rows = cur.fetchmany()
                        while len(rows) > 0:
                            for row in rows:
                                for j in range(len(histograms)):
                                    histograms[j][row[0] // max_sizes[j]].update(np.array(row[1:], dtype=float))
                            row_count += len(rows)
                            print(f"Phase 2: {row_count}/{partition_count*leaf_size}")
                            rows = cur.fetchmany()

                columns_string = ','.join([f"one_count_{i} INTEGER[]" for i in range(len(columns))]+[f"two_count_{i+1} INTEGER[][]" for i in range(len(columns)-1)])
                pg.drop_table(histogram_table)
                pg.cur.execute(f"""
                    CREATE TABLE {histogram_table} (
                        layer_index   INTEGER,
                        group_index   INTEGER,
                        count         INTEGER,
                        columns       TEXT[],
                        mins          DOUBLE PRECISION[],
                        maxs          DOUBLE PRECISION[],
                        {columns_string}
                );""")
                pg.conn.commit()
                columns_string = ','.join([f"one_count_{i}" for i in range(len(columns))]+[f"two_count_{i+1}" for i in range(len(columns)-1)])
                delim = '|'
                sql = f"""
                    COPY {histogram_table} (layer_index, group_index, count, columns, mins, maxs, {columns_string})
                    FROM STDIN WITH (FORMAT CSV, DELIMITER '{delim}')
                """
                async def main():
                    async with await psycopg.AsyncConnection.connect(pg.url) as conn:
                        async with conn.cursor() as cur:
                            async with cur.copy(sql) as copy:
                                write_tasks = []
                                for layer_index, layer_histogram in enumerate(histograms):
                                    for group_index, histogram in enumerate(layer_histogram):
                                        # if histogram.n <= 2:
                                        #     continue
                                        csv_row = f"{layer_index}{delim}{group_index}{delim}{histogram.n}{delim}"
                                        csv_row += f"{{{','.join(histogram.columns)}}}{delim}"
                                        csv_row += f"{{{','.join(map(str, histogram.mins))}}}{delim}"
                                        csv_row += f"{{{','.join(map(str, histogram.maxs))}}}{delim}"
                                        for one_count in histogram.one_counts:
                                            csv_row += f"{{{','.join(map(str, np.cumsum(one_count)))}}}{delim}"
                                        for two_count in histogram.two_counts:
                                            csv_row += f"{{{','.join(map(lambda r: '{' + ','.join(map(str, r)) + '}', np.cumsum(np.cumsum(two_count, axis=0), axis=1)))}}}{delim}"
                                        write_tasks.append(asyncio.create_task(copy.write(csv_row[:-1] + "\n")))
                                await asyncio.gather(*write_tasks)
                asyncio.run(main())
                pg.cur.execute(f"CREATE INDEX {histogram_table}_id ON {histogram_table} (layer_index, group_index)")
                pg.conn.commit()
                pg.cur.execute(f"CLUSTER {histogram_table} USING {histogram_table}_id;")
                pg.conn.commit()
            
            # Histograms are available, start progressive shading
            pg.cur.execute(f"SELECT MAX(group_index) FROM {histogram_table} WHERE layer_index = {layer_count-1}")
            option = {'func':'PROBABILITY'}

            last_layer_count = pg.cur.fetchone()[0] + 1
            pq = []
            for i in range(last_layer_count):
                estimate = self.estimate(layer_count - 1, i, option)
                if estimate > 0:
                    pq.append((-estimate, layer_count - 1, i))
            print(pq)

            d = 1
            li = []
            for i in range(last_layer_count*df**d):
                estimate = self.estimate(layer_count - d-1, i, option)
                if estimate is not None and estimate > 0:
                    li.append((-estimate, i))
            li = sorted(li)[::-1]
            for v, i in li:
                print(-v, i)
            # print(self.estimate(0, 397996, option))
            # print(self.estimate(1, 39799, option))
            # print(self.estimate(2, 3979, option))
            # print(self.estimate(3, 397, option))
            # print(self.estimate(4, 39, option))
            # print(self.estimate(5, 3, option))

            # heapq.heapify(pq)
            # while len(pq) > 0:
            #     _, layer_index, group_index = heapq.heappop(pq)
            #     print("POP", -_, layer_index, group_index)
            #     if layer_index == 0:
            #         deb(-_, layer_index, group_index)
            #     if layer_index > 0:
            #         for i in range(group_index*df, (group_index+1)*df):
            #             estimate = self.estimate(layer_index - 1, i, option)
            #             if estimate is not None and estimate > 0:
            #                 print("PUSH", -estimate, layer_index - 1, i)
            #                 heapq.heappush(pq, (-estimate, layer_index - 1, i))
            #     else:
            #         pass