import os
from db_helper import get_connection

def demo_probe():
    with get_connection() as con:
        with con.cursor() as cur:
            cur.execute("select sys_context('userenv','service_name') from dual")
            svc = cur.fetchone()[0]
            print("DB_OK ->", svc)

if __name__ == "__main__":
    demo_probe()
