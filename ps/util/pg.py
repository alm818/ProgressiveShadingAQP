import psycopg
from ps.util.debug import deb

class PgManager:
    
    def __init__(self):
        self.config = {}
        config_file = open("config.txt", 'r')
        for l in config_file:
            l = l.strip()
            if len(l) > 0 and l[0] != "#":
                key, value = [x.strip() for x in l.split("=")]
                self.config[key] = value

        self.conn = psycopg.connect(dbname=self.config["database"], user=self.config["username"], password=self.config["password"], host=self.config["hostname"], port=self.config["port"])
        self.cur = self.conn.cursor()
        self.cur.execute("SET search_path TO {}".format(self.config['schema']))
        self.conn.commit()
        self._init()

    def _init(self):
        self.cur.execute("""
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = '{}'
            ORDER BY table_name, column_name
        """.format(self.config['schema']))
        rows = self.cur.fetchall()
        self.indices = {}
        for table_name, column_name in rows:
            if table_name not in self.indices:
                self.indices[table_name] = {}
            self.indices[table_name][column_name] = [None]
        
        self.cur.execute("""
            SELECT
                i.relname as index_name,
                t.relname as table_name,
                a.attname as column_name,
                count(*) OVER (PARTITION BY t.relname, i.relname) as column_count
            FROM
                pg_class t
                JOIN pg_index ix ON t.oid = ix.indrelid
                JOIN pg_class i ON i.oid = ix.indexrelid
                JOIN pg_namespace n ON n.oid = i.relnamespace
                JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
            WHERE
                n.nspname = '{}' AND t.relkind = 'r';         
        """.format(self.config["schema"]))
        rows = self.cur.fetchall()
        for index_name, table_name, column_name, column_count in rows:
            if column_count == 1:
                self.indices[table_name][column_name][0] = index_name
            else:
                self.indices[table_name][column_name].append(index_name)

    def has_index(self, table_name, column_name):
        return self.indices[table_name][column_name][0] is not None

    def get_indices(self, table_name, column_name):
        if self.has_index(table_name, column_name):
            return self.indices[table_name][column_name]
        return self.indices[table_name][column_name][1:]

    def create_index(self, table_name, column_name):
        if not self.has_index(table_name, column_name):
            index_name = "{}_{}".format(table_name, column_name)
            self.cur.execute("CREATE INDEX {} ON {} USING btree ({})".format(index_name, table_name, column_name))
            self.indices[table_name][column_name][0] = index_name
            self.conn.commit()

    def close(self):
        self.cur.close()
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()