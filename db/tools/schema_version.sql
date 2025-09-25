PROMPT Ensuring SCHEMA_VERSION table exists...
SET SERVEROUTPUT ON
DECLARE
    l_count INTEGER;
BEGIN
    SELECT COUNT(*)
      INTO l_count
      FROM user_tables
     WHERE table_name = 'SCHEMA_VERSION';

    IF l_count = 0 THEN
        EXECUTE IMMEDIATE q'[
            CREATE TABLE schema_version (
                version      VARCHAR2(100 CHAR) PRIMARY KEY,
                applied_at   TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL
            )
        ]';
    END IF;
END;
/

EXIT;
