
-- =====================================================================
-- ORDS REST modules for Sustainacore
-- =====================================================================
BEGIN
  ORDS.ENABLE_SCHEMA(
    p_enabled             => TRUE,
    p_schema              => USER,
    p_url_mapping_type    => 'BASE_PATH',
    p_url_mapping_pattern => 'esg',
    p_auto_rest_auth      => FALSE
  );
END;
/
-- News list (GET /ords/<db>/esg/news?ticker=AAPL)
BEGIN
  ORDS.DEFINE_MODULE(p_module_name => 'news', p_base_path => '/news/');
  ORDS.DEFINE_TEMPLATE(p_module_name => 'news', p_pattern => '');
  ORDS.DEFINE_HANDLER(
    p_module_name => 'news',
    p_pattern     => '',
    p_method      => 'GET',
    p_source_type => ORDS.SOURCE_TYPE_QUERY,
    p_source      => q'[
      SELECT published_at, source, title, url
      FROM ESG_NEWS
      WHERE (:ticker IS NULL OR UPPER(ticker)=UPPER(:ticker))
      ORDER BY published_at DESC
      FETCH FIRST 100 ROWS ONLY
    ]'
  );
  COMMIT;
END;
/
-- Ask endpoint (POST /ords/<db>/esg/ask) – simple pass-through proxy to your VM API
-- This keeps CORS simple for APEX/static sites.
BEGIN
  ORDS.DEFINE_MODULE(p_module_name => 'ask', p_base_path => '/ask/');
  ORDS.DEFINE_TEMPLATE(p_module_name => 'ask', p_pattern => '');
  ORDS.DEFINE_HANDLER(
    p_module_name => 'ask',
    p_pattern     => '',
    p_method      => 'POST',
    p_source_type => ORDS.SOURCE_TYPE_PLSQL,
    p_source      => q'[
DECLARE
  v_body    CLOB;
  v_answer  CLOB;
BEGIN
  v_body := :body_text; -- JSON from caller: {"question":"...", "top_k":5}
  -- Forward to your VM API (change HOST:PORT)
  -- Example using UTL_HTTP; ensure ACLs allow outbound!
  -- For Always Free, you can call your public VM Flask endpoint, e.g. https://api.sustainacore.org/ask
  v_answer := v_body; -- stub: echo back; replace with real UTL_HTTP call if you want proxying
  :status_code := 200;
  :body := v_answer;
END;
    ]'
  );
  ORDS.DEFINE_PARAMETER(
    p_module_name        => 'ask',
    p_endpoint_name      => 'ask',
    p_parameter_name     => 'body_text',
    p_bind_variable_name => 'body_text',
    p_source_type        => 'REQUEST',
    p_access_method      => 'BODY',
    p_param_type         => 'CLOB'
  );
  COMMIT;
END;
/
-- Index vs Benchmarks (GET /ords/<db>/esg/perf?index_code=SC_AI_TECH_11)
BEGIN
  ORDS.DEFINE_MODULE(p_module_name => 'perf', p_base_path => '/perf/');
  ORDS.DEFINE_TEMPLATE(p_module_name => 'perf', p_pattern => '');
  ORDS.DEFINE_HANDLER(
    p_module_name => 'perf',
    p_pattern     => '',
    p_method      => 'GET',
    p_source_type => ORDS.SOURCE_TYPE_QUERY,
    p_source      => q'[
      SELECT i.index_code, i.level_date AS d, i.index_level,
             NULL AS bench_code, NULL AS bench_close
      FROM ESG_INDEX_LEVELS i
      WHERE (:index_code IS NULL OR i.index_code = :index_code)
      UNION ALL
      SELECT NULL, p.trade_date, p.close_adj, b.code, NULL
      FROM ESG_BENCHMARKS b JOIN ESG_BENCHMARK_PRICES p ON p.bench_id=b.bench_id
      WHERE (:bench_code IS NULL OR b.code = :bench_code)
    ]'
  );
  COMMIT;
END;
/

-- Risk endpoints (GET /ords/<db>/esg/risk)
BEGIN
  ORDS.DEFINE_MODULE(p_module_name => 'risk', p_base_path => '/risk/');
  ORDS.DEFINE_TEMPLATE(p_module_name => 'risk', p_pattern => '');
  ORDS.DEFINE_HANDLER(
    p_module_name => 'risk',
    p_pattern     => '',
    p_method      => 'GET',
    p_source_type => ORDS.SOURCE_TYPE_QUERY,
    p_source      => q'[
      SELECT 'INDEX' AS kind, index_code AS code, as_of, ann_vol, ann_return
      FROM ESG_INDEX_RISK_30D
      UNION ALL
      SELECT 'BENCH' AS kind, code, as_of, ann_vol, ann_return
      FROM ESG_BENCH_RISK_30D
    ]'
  );
  COMMIT;
END;
/
-- Attribution endpoint (GET /ords/<db>/esg/attrib?index_code=SC_AI_TECH_11&bench_code=QQQ&days=365)
BEGIN
  ORDS.DEFINE_MODULE(p_module_name => 'attrib', p_base_path => '/attrib/');
  ORDS.DEFINE_TEMPLATE(p_module_name => 'attrib', p_pattern => '');
  ORDS.DEFINE_HANDLER(
    p_module_name => 'attrib',
    p_pattern     => '',
    p_method      => 'GET',
    p_source_type => ORDS.SOURCE_TYPE_QUERY,
    p_source      => q'[
      SELECT *
      FROM ESG_ATTRIBUTION_DAILY
      WHERE (:index_code IS NULL OR index_code = :index_code)
        AND (:bench_code IS NULL OR bench_code = :bench_code)
        AND calc_date >= TRUNC(SYSDATE) - NVL(:days, 365)
      ORDER BY calc_date, sector
    ]'
  );
  COMMIT;
END;
/
