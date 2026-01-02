DECLARE
    col_count NUMBER := 0;
BEGIN
    SELECT COUNT(*) INTO col_count
      FROM user_tab_cols
     WHERE table_name = 'PROC_RESEARCH_REQUESTS'
       AND column_name = 'RETRY_COUNT';
    IF col_count = 0 THEN
        EXECUTE IMMEDIATE 'ALTER TABLE proc_research_requests ADD retry_count NUMBER DEFAULT 0';
    END IF;

    SELECT COUNT(*) INTO col_count
      FROM user_tab_cols
     WHERE table_name = 'PROC_RESEARCH_REQUESTS'
       AND column_name = 'NEXT_RETRY_AT';
    IF col_count = 0 THEN
        EXECUTE IMMEDIATE 'ALTER TABLE proc_research_requests ADD next_retry_at TIMESTAMP WITH TIME ZONE';
    END IF;
END;
/
