CREATE DATABASE IF NOT EXISTS capstone_lumi_dw;
USE capstone_lumi_dw;

CREATE TABLE IF NOT EXISTS employees (
    ingestion_timestamp VARCHAR(40) NOT NULL,
    execution_id VARCHAR(36) NOT NULL,
    source_creation_time VARCHAR(40) NOT NULL,
    employee_id VARCHAR(100) NOT NULL,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    phone_number VARCHAR(100) NOT NULL,
    hire_date VARCHAR(50) NOT NULL,
    department VARCHAR(255) NOT NULL,
    job_title VARCHAR(255) NOT NULL,
    salary VARCHAR(32) NOT NULL DEFAULT 'MA==',
    currency VARCHAR(20) NOT NULL,
    employment_status VARCHAR(100) NOT NULL,
    manager_id VARCHAR(100) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    skills JSON NOT NULL,
    address JSON NOT NULL,
    emergency_contact JSON NOT NULL,
    PRIMARY KEY (employee_id, execution_id),
    UNIQUE KEY uq_employees_employee_id (employee_id)
);
