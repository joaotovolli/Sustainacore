-- Canonicalize European Union into a single EU bucket.
-- Idempotent: safe to run multiple times.

DECLARE
  v_canon_sk NUMBER;
  v_fact_rows NUMBER := 0;
  v_dim_rows NUMBER := 0;
  v_stg_rows NUMBER := 0;
BEGIN
  BEGIN
    SELECT jurisdiction_sk
      INTO v_canon_sk
      FROM (
        SELECT jurisdiction_sk
          FROM dim_jurisdiction
         WHERE UPPER(iso_code) IN ('EU', 'EUR')
            OR UPPER(name) LIKE '%EUROPEAN UNION%'
            OR UPPER(name) = 'EU'
         ORDER BY
           CASE WHEN UPPER(iso_code) = 'EU' THEN 0 ELSE 1 END,
           CASE WHEN UPPER(name) = 'EUROPEAN UNION' THEN 0 ELSE 1 END,
           jurisdiction_sk
      )
     WHERE ROWNUM = 1;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      v_canon_sk := NULL;
  END;

  IF v_canon_sk IS NOT NULL THEN
    UPDATE dim_jurisdiction
       SET iso_code = 'EU',
           name = 'European Union'
     WHERE jurisdiction_sk = v_canon_sk;

    UPDATE fact_instrument_snapshot
       SET jurisdiction_sk = v_canon_sk
     WHERE jurisdiction_sk IN (
           SELECT jurisdiction_sk
             FROM dim_jurisdiction
            WHERE jurisdiction_sk <> v_canon_sk
              AND (
                UPPER(iso_code) IN ('EU', 'EUR')
                OR UPPER(name) LIKE '%EUROPEAN UNION%'
                OR UPPER(name) = 'EU'
              )
     );
    v_fact_rows := SQL%ROWCOUNT;

    UPDATE stg_ai_reg_record_raw
       SET jurisdiction_iso_code = 'EU',
           jurisdiction_name = 'European Union'
     WHERE UPPER(jurisdiction_iso_code) IN ('EU', 'EUR')
        OR UPPER(jurisdiction_name) LIKE '%EUROPEAN UNION%'
        OR UPPER(jurisdiction_name) = 'EU';
    v_stg_rows := SQL%ROWCOUNT;

    UPDATE dim_jurisdiction
       SET iso_code = 'EU_OLD',
           name = 'European Union (legacy)'
     WHERE jurisdiction_sk <> v_canon_sk
       AND (
         UPPER(iso_code) IN ('EU', 'EUR')
         OR UPPER(name) LIKE '%EUROPEAN UNION%'
         OR UPPER(name) = 'EU'
       );
    v_dim_rows := SQL%ROWCOUNT;

    COMMIT;
  END IF;

  DBMS_OUTPUT.PUT_LINE('canonical_eu_sk=' || NVL(TO_CHAR(v_canon_sk), 'NULL'));
  DBMS_OUTPUT.PUT_LINE('eu_fact_updated=' || v_fact_rows);
  DBMS_OUTPUT.PUT_LINE('eu_stg_updated=' || v_stg_rows);
  DBMS_OUTPUT.PUT_LINE('eu_dim_marked_legacy=' || v_dim_rows);
END;
/
