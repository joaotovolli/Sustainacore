import os, os.path
try:
    import oracledb
    cfg = os.environ.get("TNS_ADMIN")
    if cfg and os.path.isdir(cfg):
        # Tell python-oracledb (Thin) where tnsnames.ora + wallet live
        try:
            oracledb.defaults.config_dir = cfg
        except Exception:
            pass
except Exception:
    pass
