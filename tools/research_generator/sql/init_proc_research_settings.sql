CREATE TABLE proc_research_settings (
    settings_id NUMBER PRIMARY KEY,
    schedule_enabled CHAR(1) DEFAULT 'Y' NOT NULL,
    schedule_tz VARCHAR2(16) DEFAULT 'UTC' NOT NULL,
    schedule_hour NUMBER DEFAULT 3 NOT NULL,
    schedule_minute NUMBER DEFAULT 0 NOT NULL,
    schedule_freq VARCHAR2(16) DEFAULT 'DAILY' NOT NULL,
    schedule_dow_mask VARCHAR2(64),
    max_context_pct NUMBER DEFAULT 10 NOT NULL,
    saver_mode VARCHAR2(16) DEFAULT 'MEDIUM' NOT NULL,
    dev_noop CHAR(1) DEFAULT 'N' NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE,
    updated_by VARCHAR2(80)
);
