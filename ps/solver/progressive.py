import subprocess

from ps.util.pg import PgManager
from ps.util.debug import deb, logger
from ps.util.declare import DOWNSCALE_FACTOR
from ps.solver.naive import Solver

class Progressive(Solver):
    def __init__(self, aqp):
        super().__init__(aqp)

    def solving(self, df=DOWNSCALE_FACTOR):
        table_name = self.aqp.from_tables.get_roots()[0]
        with PgManager() as pg:
            if pg.config.getboolean("setup", "rebuild_dstree") or not pg.has_partitioned(table_name):
                logger.info(f"Start partitioning {table_name}...")
                process = subprocess.Popen(['bash', 'cpp_run.sh', table_name, str(df)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                for line in process.stdout:
                    print(line.strip())
            # columns = list(set(pg.get_numeric_columns(table_name)) - set(pg.get_all_keys(table_name) + [pg.config["pgmanager"]["partition_column"]]))