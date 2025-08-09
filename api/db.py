import pathlib, sys
from contextlib import contextmanager

# Dynamically locate Connect_apexRI4X6.py in any parent folder of the project
current = pathlib.Path(__file__).resolve()
for parent in [current] + list(current.parents):
    cand = parent / 'Connect_apexRI4X6.py'
    if cand.exists():
        sys.path.insert(0, str(parent))
        break

from Connect_apexRI4X6 import get_connection  # private credentials

@contextmanager
def oracle_cursor():
    conn = get_connection()
    cur = conn.cursor()
    try:
        yield cur
        conn.commit()
    except:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

def query_df(sql, params=None):
    import pandas as pd
    with oracle_cursor() as cur:
        cur.execute(sql, params or {})
        rows = cur.fetchall()
        cols = [c[0] for c in cur.description]
        return pd.DataFrame(rows, columns=cols)

def execute_many(sql, data):
    with oracle_cursor() as cur:
        cur.executemany(sql, data)