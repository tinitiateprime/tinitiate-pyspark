CREATE SCHEMA IF NOT EXISTS training;

CREATE TABLE IF NOT EXISTS training.location (
    location_id BIGINT PRIMARY KEY,
    location_name VARCHAR(100) NOT NULL,
    region VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS training.product (
    product_id BIGINT PRIMARY KEY,
    product_name VARCHAR(150) NOT NULL,
    category VARCHAR(80) NOT NULL,
    unit_price NUMERIC(14, 2) NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS training.customer (
    customer_id BIGINT PRIMARY KEY,
    customer_name VARCHAR(150) NOT NULL,
    email VARCHAR(255),
    location_id BIGINT,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS training.sales (
    sale_id BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    location_id BIGINT NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(14, 2) NOT NULL,
    sale_ts TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS training.dept (
    deptno BIGINT PRIMARY KEY,
    dname VARCHAR(100) NOT NULL,
    loc VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS training.emp (
    empno BIGINT PRIMARY KEY,
    ename VARCHAR(100) NOT NULL,
    job VARCHAR(100) NOT NULL,
    mgr BIGINT,
    hiredate DATE NOT NULL,
    sal NUMERIC(14, 2) NOT NULL,
    commission NUMERIC(14, 2),
    deptno BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS training.projects (
    project_id BIGINT PRIMARY KEY,
    project_name VARCHAR(150) NOT NULL,
    budget NUMERIC(16, 2) NOT NULL,
    location_id BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS training.emp_projects (
    emp_project_id BIGINT PRIMARY KEY,
    empno BIGINT NOT NULL,
    project_id BIGINT NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE
);

CREATE TABLE IF NOT EXISTS training.sales_transaction (
    transaction_id BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    location_id BIGINT NOT NULL,
    amount NUMERIC(16, 2) NOT NULL,
    transaction_ts TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS training.sales_transaction_changes_staging (
    operation CHAR(1) NOT NULL,
    transaction_id BIGINT NOT NULL,
    amount NUMERIC(16, 2),
    transaction_ts TIMESTAMP,
    change_ts TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS training.load_audit (
    load_id BIGSERIAL PRIMARY KEY,
    scenario VARCHAR(100) NOT NULL,
    target_table VARCHAR(100) NOT NULL,
    source_format VARCHAR(20) NOT NULL,
    source_path TEXT NOT NULL,
    source_files BIGINT,
    accepted_rows BIGINT NOT NULL,
    rejected_rows BIGINT NOT NULL,
    read_seconds NUMERIC(14, 3) NOT NULL,
    write_seconds NUMERIC(14, 3) NOT NULL,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sales_customer ON training.sales (customer_id);
CREATE INDEX IF NOT EXISTS idx_sales_product ON training.sales (product_id);
CREATE INDEX IF NOT EXISTS idx_sales_location ON training.sales (location_id);
CREATE INDEX IF NOT EXISTS idx_emp_dept ON training.emp (deptno);
CREATE INDEX IF NOT EXISTS idx_emp_projects_emp ON training.emp_projects (empno);
CREATE INDEX IF NOT EXISTS idx_emp_projects_project ON training.emp_projects (project_id);
CREATE INDEX IF NOT EXISTS idx_sales_transaction_customer ON training.sales_transaction (customer_id);
CREATE INDEX IF NOT EXISTS idx_sales_transaction_ts ON training.sales_transaction (transaction_ts);
CREATE INDEX IF NOT EXISTS idx_sales_transaction_changes_key
    ON training.sales_transaction_changes_staging (transaction_id);

CREATE OR REPLACE FUNCTION training.apply_sales_transaction_changes()
RETURNS TABLE (updated_rows BIGINT, deleted_rows BIGINT)
LANGUAGE plpgsql
AS $$
DECLARE
    v_updated_rows BIGINT;
    v_deleted_rows BIGINT;
BEGIN
    UPDATE training.sales_transaction AS target
       SET amount = change.amount,
           transaction_ts = change.transaction_ts
      FROM training.sales_transaction_changes_staging AS change
     WHERE change.operation = 'U'
       AND target.transaction_id = change.transaction_id;
    GET DIAGNOSTICS v_updated_rows = ROW_COUNT;

    DELETE FROM training.sales_transaction AS target
     USING training.sales_transaction_changes_staging AS change
     WHERE change.operation = 'D'
       AND target.transaction_id = change.transaction_id;
    GET DIAGNOSTICS v_deleted_rows = ROW_COUNT;

    TRUNCATE TABLE training.sales_transaction_changes_staging;
    RETURN QUERY SELECT v_updated_rows, v_deleted_rows;
END;
$$;
