import psycopg, os

current_path = os.getcwd()

config = {}
config_file = open("../config.txt", 'r')
for l in config_file:
    l = l.strip()
    if len(l) > 0 and l[0] != "#":
        key, value = [x.strip() for x in l.split("=")]
        config[key] = value
config["rebuild_tpch"] = config["rebuild_tpch"].lower() == 'true'

table_names = ["nation", "region", "supplier", "customer", "part", "partsupp", "orders", "lineitem"]

with psycopg.connect("dbname={} host={} port={} user={} password={}".format(config["database"], config["hostname"], config["port"], config["username"], config["password"])) as conn:
    with conn.cursor() as cur:
        if config["rebuild_tpch"]:
            for table_name in table_names:
                cur.execute("""DROP TABLE IF EXISTS {} CASCADE""".format(table_name))

        copy_tbl = False
        for table_name in table_names:
            cur.execute("""
                SELECT EXISTS (
                    SELECT * FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = '{}'
                );
            """.format(table_name))
            exists = cur.fetchone()[0]
            if not exists:
                copy_tbl = True
        if copy_tbl:
            print("Creating tpch tables...")
            cur.execute("""
                -- nation
                CREATE TABLE IF NOT EXISTS "nation" (
                "n_nationkey"  INT,
                "n_name"       CHAR(25),
                "n_regionkey"  INT,
                "n_comment"    VARCHAR(152));

                -- region
                CREATE TABLE IF NOT EXISTS "region" (
                "r_regionkey"  INT,
                "r_name"       CHAR(25),
                "r_comment"    VARCHAR(152));

                -- supplier
                CREATE TABLE IF NOT EXISTS "supplier" (
                "s_suppkey"     INT,
                "s_name"        CHAR(25),
                "s_address"     VARCHAR(40),
                "s_nationkey"   INT,
                "s_phone"       CHAR(15),
                "s_acctbal"     DECIMAL(15,2),
                "s_comment"     VARCHAR(101));

                -- customer
                CREATE TABLE IF NOT EXISTS "customer" (
                "c_custkey"     INT,
                "c_name"        VARCHAR(25),
                "c_address"     VARCHAR(40),
                "c_nationkey"   INT,
                "c_phone"       CHAR(15),
                "c_acctbal"     DECIMAL(15,2),
                "c_mktsegment"  CHAR(10),
                "c_comment"     VARCHAR(117));

                -- part
                CREATE TABLE IF NOT EXISTS "part" (
                "p_partkey"     INT,
                "p_name"        VARCHAR(55),
                "p_mfgr"        CHAR(25),
                "p_brand"       CHAR(10),
                "p_type"        VARCHAR(25),
                "p_size"        INT,
                "p_container"   CHAR(10),
                "p_retailprice" DECIMAL(15,2) ,
                "p_comment"     VARCHAR(23));

                -- partsupp
                CREATE TABLE IF NOT EXISTS "partsupp" (
                "ps_partkey"     INT,
                "ps_suppkey"     INT,
                "ps_availqty"    INT,
                "ps_supplycost"  DECIMAL(15,2),
                "ps_comment"     VARCHAR(199));

                -- orders
                CREATE TABLE IF NOT EXISTS "orders" (
                "o_orderkey"       INT,
                "o_custkey"        INT,
                "o_orderstatus"    CHAR(1),
                "o_totalprice"     DECIMAL(15,2),
                "o_orderdate"      DATE,
                "o_orderpriority"  CHAR(15),
                "o_clerk"          CHAR(15),
                "o_shippriority"   INT,
                "o_comment"        VARCHAR(79));

                -- lineitem
                CREATE TABLE IF NOT EXISTS "lineitem"(
                "l_orderkey"          INT,
                "l_partkey"           INT,
                "l_suppkey"           INT,
                "l_linenumber"        INT,
                "l_quantity"          DECIMAL(15,2),
                "l_extendedprice"     DECIMAL(15,2),
                "l_discount"          DECIMAL(15,2),
                "l_tax"               DECIMAL(15,2),
                "l_returnflag"        CHAR(1),
                "l_linestatus"        CHAR(1),
                "l_shipdate"          DATE,
                "l_commitdate"        DATE,
                "l_receiptdate"       DATE,
                "l_shipinstruct"      CHAR(25),
                "l_shipmode"          CHAR(10),
                "l_comment"           VARCHAR(44));
            """)
            
            print("Copying tpch tables...")
            cur.execute("""
                COPY "region"     FROM '{}/tpch/region.tbl'        DELIMITER '|' CSV;
                COPY "nation"     FROM '{}/tpch/nation.tbl'        DELIMITER '|' CSV;
                COPY "customer"   FROM '{}/tpch/customer.tbl'      DELIMITER '|' CSV;
                COPY "supplier"   FROM '{}/tpch/supplier.tbl'      DELIMITER '|' CSV;
                COPY "part"       FROM '{}/tpch/part.tbl'          DELIMITER '|' CSV;
                COPY "partsupp"   FROM '{}/tpch/partsupp.tbl'      DELIMITER '|' CSV;
                COPY "orders"     FROM '{}/tpch/orders.tbl'        DELIMITER '|' CSV;
                COPY "lineitem"   FROM '{}/tpch/lineitem.tbl'      DELIMITER '|' CSV;
            """.format(current_path, current_path, current_path, current_path, current_path, current_path, current_path, current_path))

            print("Adding primary keys and foreign keys...")
            cur.execute("""
                ALTER TABLE nation ADD PRIMARY KEY (n_nationkey);
                ALTER TABLE region ADD PRIMARY KEY (r_regionkey);
                ALTER TABLE supplier ADD PRIMARY KEY (s_suppkey);
                ALTER TABLE customer ADD PRIMARY KEY (c_custkey);
                ALTER TABLE part ADD PRIMARY KEY (p_partkey);
                ALTER TABLE partsupp ADD PRIMARY KEY (ps_partkey,ps_suppkey);
                ALTER TABLE orders ADD PRIMARY KEY (o_orderkey);
                ALTER TABLE nation ADD CONSTRAINT n_regionkey FOREIGN KEY(n_regionkey) REFERENCES region(r_regionkey);
                ALTER TABLE supplier ADD CONSTRAINT s_nationkey FOREIGN KEY(s_nationkey) REFERENCES nation(n_nationkey);
                ALTER TABLE customer ADD CONSTRAINT c_nationkey FOREIGN KEY(c_nationkey) REFERENCES nation(n_nationkey);
                ALTER TABLE partsupp ADD CONSTRAINT ps_partkey FOREIGN KEY(ps_partkey) REFERENCES part(p_partkey);
                ALTER TABLE partsupp ADD CONSTRAINT ps_suppkey FOREIGN KEY(ps_suppkey) REFERENCES supplier(s_suppkey);
                ALTER TABLE orders ADD CONSTRAINT o_custkey FOREIGN KEY(o_custkey) REFERENCES customer(c_custkey);
                ALTER TABLE lineitem ADD CONSTRAINT l_orderkey FOREIGN KEY(l_orderkey) REFERENCES orders(o_orderkey);
                ALTER TABLE lineitem ADD CONSTRAINT l_partsuppkey FOREIGN KEY(l_partkey, l_suppkey) REFERENCES partsupp(ps_partkey, ps_suppkey);
            """)
        conn.commit()