-- SC_IDX index calc v1 tables (idempotent create)
BEGIN
  EXECUTE IMMEDIATE '
    CREATE TABLE SC_IDX_CONSTITUENT_DAILY (
      trade_date DATE NOT NULL,
      ticker VARCHAR2(32) NOT NULL,
      rebalance_date DATE NOT NULL,
      shares NUMBER,
      price_used NUMBER,
      market_value NUMBER,
      weight NUMBER,
      price_quality VARCHAR2(16),
      computed_at TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
      CONSTRAINT SC_IDX_CONSTITUENT_DAILY_PK PRIMARY KEY (trade_date, ticker)
    )';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -955 THEN
      RAISE;
    END IF;
END;
/

BEGIN
  EXECUTE IMMEDIATE '
    CREATE TABLE SC_IDX_CONTRIBUTION_DAILY (
      trade_date DATE NOT NULL,
      ticker VARCHAR2(32) NOT NULL,
      weight_prev NUMBER,
      ret_1d NUMBER,
      contribution NUMBER,
      computed_at TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
      CONSTRAINT SC_IDX_CONTRIBUTION_DAILY_PK PRIMARY KEY (trade_date, ticker)
    )';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -955 THEN
      RAISE;
    END IF;
END;
/

BEGIN
  EXECUTE IMMEDIATE '
    CREATE TABLE SC_IDX_STATS_DAILY (
      trade_date DATE PRIMARY KEY,
      level_tr NUMBER,
      ret_1d NUMBER,
      ret_5d NUMBER,
      ret_20d NUMBER,
      vol_20d NUMBER,
      max_drawdown_252d NUMBER,
      n_constituents NUMBER,
      n_imputed NUMBER,
      top5_weight NUMBER,
      herfindahl NUMBER,
      computed_at TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL
    )';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -955 THEN
      RAISE;
    END IF;
END;
/
