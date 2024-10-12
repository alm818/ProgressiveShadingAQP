import subprocess, math, psycopg, asyncio
import numpy as np

from ps.util.pg import PgManager
from ps.util.debug import deb, logger
from ps.solver.naive import Solver
from ps.core.histogram import Stat, Histogram

PG_ARRAY_SIZE = 100000

class Progressive(Solver):

    def __init__(self, aqp):
        super().__init__(aqp)

    def solving(self, leaf_size):
        table_name = self.aqp.from_tables.get_roots()[0]
        histogram_table = f"{table_name}_histogram"
        with PgManager() as pg:
            info = pg.get_partitioning_info(table_name)
            make_histogram = pg.config.getboolean("setup", "rebuild_histogram") or not pg.exist_table(histogram_table)
            if pg.config.getboolean("setup", "rebuild_dstree") or info is None or info[1] != leaf_size:
                logger.info(f"Start partitioning {table_name}...")
                process = subprocess.Popen(['bash', 'cpp_run.sh', table_name, str(leaf_size)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                for line in process.stdout:
                    print(line.strip())
                info = pg.get_partitioning_info(table_name)
                make_histogram = True
            # columns = list(set(pg.get_numeric_columns(table_name)) - set(pg.get_all_keys(table_name) + [pg.config["pgmanager"]["partition_column"]]))
            if make_histogram:
                columns, leaf_size, partition_count = info
                df, stats = leaf_size, []
                layer_count = math.ceil(math.log(partition_count, df))
                max_sizes = [df**j for j in range(layer_count)]
                for j in range(layer_count):
                    group_count = int(math.ceil(partition_count / max_sizes[j]))
                    stats.append([Stat(columns, max_sizes[j]) for i in range(group_count)])
                row_count = 0
                with psycopg.connect(pg.url) as conn:
                    with psycopg.ServerCursor(conn, name="ServerCursor") as cur:
                        cur.arraysize = PG_ARRAY_SIZE
                        cur.execute(f"SELECT {pg.config["pgmanager"]["partition_column"]},{','.join(columns)} FROM {table_name} ORDER BY {table_name}_id LIMIT {3*PG_ARRAY_SIZE}")
                        # cur.execute(f"SELECT {pg.config["pgmanager"]["partition_column"]},{','.join(columns)} FROM {table_name}")
                        rows = cur.fetchmany()
                        while len(rows) > 0:
                            for row in rows:
                                for j in range(len(stats)):
                                    stats[j][row[0] // max_sizes[j]].update(np.array(row[1:], dtype=float))
                            row_count += len(rows)
                            print(f"Phase 1: {row_count}/{partition_count*leaf_size}")
                            rows = cur.fetchmany()

                histograms = [[Histogram(stat) for stat in layer_stat] for layer_stat in stats]
                row_count = 0
                with psycopg.connect(pg.url) as conn:
                    with psycopg.ServerCursor(conn, name="ServerCursor") as cur:
                        cur.arraysize = PG_ARRAY_SIZE
                        cur.execute(f"SELECT {pg.config["pgmanager"]["partition_column"]},{','.join(columns)} FROM {table_name} ORDER BY {table_name}_id LIMIT {3*PG_ARRAY_SIZE}")
                        # cur.execute(f"SELECT {pg.config["pgmanager"]["partition_column"]},{','.join(columns)} FROM {table_name}")
                        rows = cur.fetchmany()
                        while len(rows) > 0:
                            for row in rows:
                                for j in range(len(histograms)):
                                    histograms[j][row[0] // max_sizes[j]].update(np.array(row[1:], dtype=float))
                            row_count += len(rows)
                            print(f"Phase 2: {row_count}/{partition_count*leaf_size}")
                            rows = cur.fetchmany()

                one_count_string = ','.join([f"one_count_{i} INTEGER[]" for i in range(len(columns))])
                two_count_string = ','.join([f"two_count_{i+1} INTEGER[][]" for i in range(len(columns)-1)])
                if len(two_count_string) > 0:
                    one_count_string += ","
                pg.drop_table(histogram_table)
                pg.cur.execute(f"""
                    CREATE TABLE {histogram_table} (
                        layer_index   INTEGER,
                        group_index   INTEGER,
                        count         INTEGER,
                        mins          DOUBLE PRECISION[],
                        scales        DOUBLE PRECISION[],
                        {one_count_string} {two_count_string}
                );""")
                pg.conn.commit()
                for layer_index, layer_histogram in enumerate(histograms):
                    for group_index, histogram in enumerate(layer_histogram):
                        histogram.one_counts = [np.cumsum(one_count) for one_count in histogram.one_counts]
                        histogram.two_counts = [np.cumsum(np.cumsum(two_count, axis=0), axis=1) for two_count in histogram.two_counts]