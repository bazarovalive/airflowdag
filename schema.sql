create schema IF NOT EXISTS DS ;
CREATE SCHEMA IF NOT EXISTS LOGS;

CREATE TABLE IF NOT EXISTS DS.FT_BALANCE_F (
    on_date DATE not null,
    account_rk NUMERIC not null,
    currency_rk NUMERIC,
    balance_out FLOAT,

    PRIMARY KEY (on_date, account_rk)
);

CREATE TABLE IF NOT EXISTS DS.FT_POSTING_F (
    oper_date DATE not null,
    credit_account_rk NUMERIC not null,
    debet_account_rk NUMERIC not null,
    credit_amount FLOAT,
    debet_amount FLOAT,

    PRIMARY KEY (oper_date, credit_account_rk, debet_account_rk)
);

CREATE TABLE IF NOT EXISTS DS.MD_ACCOUNT_D (
    data_actual_date DATE not null,
    data_actual_end_date DATE not null,
    account_rk NUMERIC not null,
    account_number VARCHAR(20) not null,
    char_type VARCHAR(1) not null,
    currency_rk NUMERIC not null,
    currency_code VARCHAR(3) not null,

    PRIMARY KEY (data_actual_date, account_rk)
);

CREATE TABLE IF NOT EXISTS DS.MD_CURRENCY_D (
    currency_rk NUMERIC not null,
    data_actual_date DATE not null ,
    data_actual_end_date DATE,
    currency_code VARCHAR(3),
    code_iso_char VARCHAR(3),
    PRIMARY KEY (currency_rk, data_actual_date)
);

CREATE TABLE IF NOT EXISTS DS.MD_EXCHANGE_RATE_D (
    data_actual_date DATE not null,
    data_actual_end_date DATE,
    currency_rk NUMERIC not null,
    reduced_cource FLOAT,
    code_iso_num VARCHAR(3),
    PRIMARY KEY (data_actual_date, currency_rk)
);

CREATE TABLE IF NOT EXISTS DS.MD_LEDGER_ACCOUNT_S (
    chapter CHAR(1),
    chapter_name VARCHAR(16),
    section_number INTEGER,
    section_name VARCHAR(22),
    subsection_name VARCHAR(21),
    ledger1_account INTEGER,
    ledger1_account_name VARCHAR(47),
    ledger_account INTEGER NOT NULL,
    ledger_account_name VARCHAR(153),
    characteristic CHAR(1),
    is_resident INTEGER,
    is_reserve INTEGER,
    is_reserved INTEGER,
    is_loan INTEGER,
    is_reserved_assets INTEGER,
    is_overdue INTEGER,
    is_interest INTEGER,
    pair_account VARCHAR(5),
    start_date DATE NOT NULL,
    end_date DATE,
    is_rub_only INTEGER,
    min_term VARCHAR(1),
    min_term_measure VARCHAR(1),
    max_term VARCHAR(1),
    max_term_measure VARCHAR(1),
    ledger_acc_full_name_translit VARCHAR(1),
    is_revaluation VARCHAR(1),
    is_correct VARCHAR(1),
    PRIMARY KEY (ledger_account, start_date)
);
commit;



CREATE TABLE IF NOT EXISTS LOGS.LogTable (
    process_name VARCHAR(255) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    PRIMARY KEY (process_name)
);
commit;

