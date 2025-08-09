import cx_Oracle
import os

def get_connection():
    # Set the OS environment variable to point to tnsnames.ora file
    os.environ['TNS_ADMIN'] = "C:/Users/joaot/OneDrive/Documentos/My Project/Wallet_dbRI4X6"

    # Create the connection. Parameters are: username, password, and name of database services
    con = cx_Oracle.connect('WKSP_ESGAPEX', '9o0p(O)P9o0p', 'dbri4x6_high')
    return con
