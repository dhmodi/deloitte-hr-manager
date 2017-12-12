--  Sample employee database 
--  See changelog table for details
--  Copyright (C) 2007,2008, MySQL AB
--  
--  Original data created by Fusheng Wang and Carlo Zaniolo
--  http://www.cs.aau.dk/TimeCenter/software.htm
--  http://www.cs.aau.dk/TimeCenter/Data/employeeTemporalDataSet.zip
-- 
--  Current schema by Giuseppe Maxia 
--  Data conversion from XML to relational by Patrick Crews
-- 
-- This work is licensed under the 
-- Creative Commons Attribution-Share Alike 3.0 Unported License. 
-- To view a copy of this license, visit 
-- http://creativecommons.org/licenses/by-sa/3.0/ or send a letter to 
-- Creative Commons, 171 Second Street, Suite 300, San Francisco, 
-- California, 94105, USA.
-- 
--  DISCLAIMER
--  To the best of our knowledge, this data is fabricated, and
--  it does not correspond to real people. 
--  Any similarity to existing people is purely coincidental.
-- 

DROP DATABASE IF EXISTS `emp``;
CREATE DATABASE IF NOT EXISTS `emp`;
USE employees;

SELECT 'CREATING DATABASE STRUCTURE' as 'INFO';

DROP TABLE IF EXISTS dept_emp,
                     dept_manager,
                     titles,
                     salaries, 
                     employees, 
                     departments;

/*!50503 set default_storage_engine = InnoDB */;
/*!50503 select CONCAT('storage engine: ', @@default_storage_engine) as INFO */;

CREATE TABLE `inventory` (
    `sku`      INT             NOT NULL,
    `calendar_Day`  DATE            NOT NULL,
    `category`  VARCHAR(14)     NOT NULL,
    `location`   VARCHAR(16)     NOT NULL,
    `dc`      VARCHAR(16)     NOT NULL,
    `prod_plant` DOUBLE            NOT NULL,
    `usd_total_plant_stock`   DOUBLE            NOT NULL,
    `total_plant_stock`   DOUBLE            NOT NULL,
    `usd_quality_hold`   DOUBLE            NOT NULL,
    `quality_hold`   DOUBLE            NOT NULL,
    `usd_safety_stock`   DOUBLE            NOT NULL,
    `safety_stock`   DOUBLE            NOT NULL,
    `usd_cycle_stock`   DOUBLE            NOT NULL,
    `cycle_stock`   DOUBLE            NOT NULL,
    `usd_transit_stock`   DOUBLE            NOT NULL,
    `transit_stock`   DOUBLE            NOT NULL,
    `usd_excess`   DOUBLE            NOT NULL,
    `excess`   DOUBLE            NOT NULL,
    PRIMARY KEY (`sku`)
);

CREATE TABLE `forecast_vs_actual` (
    `calendar_year_month`     DATE         NOT NULL,
    `sku`    INT             NOT NULL,
    `dc`      VARCHAR(16)     NOT NULL,
    `category`  VARCHAR(14)     NOT NULL,
    `actual_total_inventory`    DOUBLE            NOT NULL,
    `actual_production`    DOUBLE            NOT NULL,
    `actual_demand`    DOUBLE            NOT NULL,
    `forecast_demand`    DOUBLE            NOT NULL,
    PRIMARY KEY (`sku`)
);

CREATE TABLE `dept_manager` (
   `emp_no`       INT             NOT NULL,
   `dept_no`      CHAR(4)         NOT NULL,
   `from_date`    DATE            NOT NULL,
   `to_date`      DATE            NOT NULL,
   FOREIGN KEY (`emp_no`)  REFERENCES employees (emp_no)    ON DELETE CASCADE,
   FOREIGN KEY (`dept_no`) REFERENCES departments (dept_no) ON DELETE CASCADE,
   PRIMARY KEY (`emp_no`,`dept_no`)
); 

CREATE TABLE `dept_emp` (
    `emp_no`      INT             NOT NULL,
    `dept_no`     CHAR(4)         NOT NULL,
    `from_date`   DATE            NOT NULL,
    `to_date`     DATE            NOT NULL,
    FOREIGN KEY (emp_no)  REFERENCES employees   (emp_no)  ON DELETE CASCADE,
    FOREIGN KEY (dept_no) REFERENCES departments (dept_no) ON DELETE CASCADE,
    PRIMARY KEY (`emp_no`,`dept_no`)
);

CREATE TABLE `titles` (
    `emp_no`      INT             NOT NULL,
    `title`       VARCHAR(50)     NOT NULL,
    `from_date`   DATE            NOT NULL,
    `to_date`     DATE,
    FOREIGN KEY (emp_no) REFERENCES employees (emp_no) ON DELETE CASCADE,
    PRIMARY KEY (`emp_no`,`title`,`from_date`)
) 
; 

CREATE TABLE `salaries` (
    `emp_no`      INT             NOT NULL,
    `salary`      INT             NOT NULL,
    `from_date`   DATE            NOT NULL,
    `to_date`     DATE            NOT NULL,
    FOREIGN KEY (emp_no) REFERENCES employees (emp_no) ON DELETE CASCADE,
    PRIMARY KEY (`emp_no`,`from_date`)
) 
; 

CREATE OR REPLACE VIEW dept_emp_latest_date AS
    SELECT emp_no, MAX(from_date) AS from_date, MAX(to_date) AS to_date
    FROM dept_emp
    GROUP BY emp_no;

# shows only the current department for each employee
CREATE OR REPLACE VIEW current_dept_emp AS
    SELECT l.emp_no, dept_no, l.from_date, l.to_date
    FROM dept_emp d
        INNER JOIN dept_emp_latest_date l
        ON d.emp_no=l.emp_no AND d.from_date=l.from_date AND l.to_date = d.to_date;

flush /*!50503 binary */ logs;

SELECT 'LOADING departments' as 'INFO';
source load_departments.dump ;
SELECT 'LOADING employees' as 'INFO';
source load_employees.dump ;
SELECT 'LOADING dept_emp' as 'INFO';
source load_dept_emp.dump ;
SELECT 'LOADING dept_manager' as 'INFO';
source load_dept_manager.dump ;
SELECT 'LOADING titles' as 'INFO';
source load_titles.dump ;
SELECT 'LOADING salaries' as 'INFO';
source load_salaries1.dump ;
source load_salaries2.dump ;
source load_salaries3.dump ;

source show_elapsed.sql ;
