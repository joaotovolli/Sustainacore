DECLARE
    v_count NUMBER := 0;
BEGIN
    SELECT COUNT(*)
      INTO v_count
      FROM user_tab_columns
     WHERE table_name = 'SC_USER_PROFILE'
       AND column_name = 'NAME';

    IF v_count = 0 THEN
        EXECUTE IMMEDIATE 'ALTER TABLE SC_USER_PROFILE ADD (NAME VARCHAR2(200 BYTE))';
    END IF;
END;
/
