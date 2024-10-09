import psycopg, configparser

config = configparser.ConfigParser()
config_file = open("../config.txt", 'r')
config.read_file(config_file)
config_file.close()

with psycopg.connect(dbname=config["postgres"]["database"], user=config["postgres"]["username"], password=config["postgres"]["password"], host=config["postgres"]["hostname"], port=config["postgres"]["port"]) as conn:
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pageinspect;")
        cur.execute("ALTER SYSTEM SET work_mem = '1GB';")
        cur.execute("SELECT pg_reload_conf();")
        if config.getboolean("setup", "rebuild_data"):
            cur.execute("""
            DO $$ 
            DECLARE
                r RECORD;
            BEGIN
                FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
                    EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
                END LOOP;
            END $$;
            """)
        conn.commit()