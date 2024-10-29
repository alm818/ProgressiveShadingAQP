import psycopg
from ps.util.debug import deb
from ps.util.misc import get_config

class PgManager:
    
    def __init__(self):
        self.config = get_config()
        self.url = f"dbname={self.config["postgres"]["database"]} user={self.config["postgres"]["username"]} password={self.config["postgres"]["password"]} host={self.config["postgres"]["hostname"]} port={self.config["postgres"]["port"]}"
        self.conn = psycopg.connect(self.url)
        self.cur = self.conn.cursor()

        self.cur.execute(f"SET search_path TO {self.config["postgres"]["schema"]}")
        self.conn.commit()
        self.init()

    def init(self):
        self.cur.execute(f"""
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = '{self.config["postgres"]["schema"]}'
            ORDER BY table_name, column_name;
        """)
        rows = self.cur.fetchall()
        self.unique_indices = {}
        for table_name, column_name in rows:
            if table_name not in self.unique_indices:
                self.unique_indices[table_name] = {}
            self.unique_indices[table_name][column_name] = False
        
        self.cur.execute(f"""
            SELECT
                t.relname AS table_name,
                a.attname AS column_name
            FROM
                pg_index i
            JOIN
                pg_class t ON t.oid = i.indrelid
            JOIN
                pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(i.indkey)
            JOIN
                pg_namespace n ON n.oid = t.relnamespace
            WHERE
                i.indisunique = true
                AND t.relkind = 'r'  -- only consider ordinary tables
                AND n.nspname = '{self.config["postgres"]["schema"]}';       
        """)
        rows = self.cur.fetchall()
        for table_name, column_name in rows:
            self.unique_indices[table_name][column_name] = True

    def get_unique_column(self, table_name):
        for column_name, is_unique in self.unique_indices[table_name].items():
            if is_unique:
                return column_name
        return None

    def get_serial_column(self, table_name):
        for column_name, is_unique in self.unique_indices[table_name].items():
            if is_unique:
                self.cur.execute(f"SELECT pg_get_serial_sequence('{table_name}', '{column_name}') AS sequence_name")
                seq = self.cur.fetchone()
                if seq is not None:
                    result = seq[0].split('.')
                    self.cur.execute(f"""
                        SELECT start_value           
                        FROM   pg_sequences                         
                        WHERE  schemaname = '{result[0]}' AND sequencename = '{result[1]}'""")
                    result = self.cur.fetchone()
                    if result is not None and int(result[0]) == 1:
                        return column_name
        return None

    def exist_table(self, table_name):
        self.cur.execute(f"""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE  table_schema = '{self.config["postgres"]["schema"]}'
            AND    table_name   = '{table_name}'
        """)
        return self.cur.fetchone()[0] > 0

    def drop_table(self, table_name):
        self.cur.execute(f"DROP TABLE IF EXISTS {table_name};")
        self.conn.commit()

    def get_max_connections(self):
        self.cur.execute("SHOW max_connections;")
        return int(self.cur.fetchone()[0])

    def get_block_count(self, table_name):
        self.cur.execute(f"SELECT pg_relation_size('{table_name}') / current_setting('block_size')::int;")
        return self.cur.fetchone()[0]

    def get_partitioning_info(self, table_name):
        dstree_table = self.config["pgmanager"]["partition_table"]
        if not self.exist_table(dstree_table):
            return None
        self.cur.execute(f"SELECT columns, leaf_size, partition_count FROM {dstree_table} WHERE table_name = '{table_name}'")
        return self.cur.fetchone()

    def get_numeric_columns(self, table_name):
        numeric_types = self.config["pgmanager"]["numeric_type"].split(',')
        self.cur.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            AND data_type IN ({','.join([f"'{type}'" for type in numeric_types])});
            """)
        columns = []
        for row in self.cur:
            columns.append(row[0])
        return columns

    def get_all_keys(self, table_name):
        self.cur.execute(f"""                              
            SELECT DISTINCT(kcu.column_name)                    
            FROM                                                
                information_schema.table_constraints AS tc      
                JOIN information_schema.key_column_usage AS kcu 
                ON tc.constraint_name = kcu.constraint_name     
                AND tc.table_schema = kcu.table_schema          
            WHERE                                               
                tc.table_name = '{table_name}'                            
                AND tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE');
            """)
        keys = []
        for row in self.cur:
            keys.append(row[0])
        return keys

    def close(self):
        self.cur.close()
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()