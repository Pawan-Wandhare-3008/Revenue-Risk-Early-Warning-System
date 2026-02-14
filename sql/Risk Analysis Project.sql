CREATE DATABASE risk_analysis;
USE risk_analysis;

CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    signup_date DATE,
    city VARCHAR(50)
);

CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    order_amount DECIMAL(10,2),
    order_status VARCHAR(20),
    city VARCHAR(50),
    cancellation_reason VARCHAR(50)
);

CREATE TABLE payments (
    order_id INT,
    order_date DATE,
    payment_id INT PRIMARY KEY,
    payment_date DATETIME,
    payment_method VARCHAR(20),
    payment_status VARCHAR(20),
    failure_reason VARCHAR(50)
);

CREATE TABLE refunds (
    refund_id INT,
    order_id INT,
    refund_amount DECIMAL(10,2),
    refund_date DATETIME,
    refund_reason VARCHAR(50)
);


CREATE TABLE operations (
    order_id INT,
    promised_time_min INT,
    actual_time_min INT,
    sla_breached BOOLEAN,
    ops_issue VARCHAR(50)
);


-- SHOW VARIABLES LIKE 'secure_file_priv';

ALTER TABLE Customers
ADD costumer_type VARCHAR(20);

ALTER TABLE customers
DROP COLUMN costumer_type;

ALTER TABLE Customers
ADD customer_type VARCHAR(20);

LOAD DATA INFILE
'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/customers.csv'
INTO TABLE customers
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(customer_id, signup_date, city, customer_type);

SELECT * FROM customers LIMIT 10;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/orders.csv'
INTO TABLE orders
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(order_id,
 customer_id,
 @order_date,
 order_amount,
 order_status,
 city,
 cancellation_reason)
SET order_date = STR_TO_DATE(@order_date, '%Y-%m-%d');

LOAD DATA INFILE
'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/payments.csv'
INTO TABLE payments
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(order_id,
 order_date,
 payment_id,
 payment_date,
 payment_method,
 payment_status,
 failure_reason);
 
 SELECT COUNT(*) FROM payments;
SELECT * FROM payments LIMIT 5;

LOAD DATA INFILE
'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/operations.csv'
INTO TABLE operations
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(order_id,
 promised_time_min,
 actual_time_min,
 @sla_breached,
 ops_issue)
SET sla_breached =
    CASE
        WHEN @sla_breached = 'TRUE' THEN 1
        WHEN @sla_breached = 'FALSE' THEN 0
        ELSE NULL
    END;
    
    
SELECT COUNT(*) FROM operations;
SELECT * FROM operations LIMIT 100;

LOAD DATA INFILE
'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/refunds.csv'
INTO TABLE refunds
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(refund_id,
 order_id,
 refund_amount,
 refund_date,
 refund_reason);
 
 SELECT COUNT(*) FROM refunds;
 SELECT refund_reason, COUNT(*)
FROM refunds
GROUP BY refund_reason;
SELECT *
FROM refunds
ORDER BY refund_date DESC
LIMIT 5;


-- queries

USE risk_analysis;

-- Are cancellations increasing compared to normal behavior?

SELECT
    DATE_FORMAT(order_date, '%Y-%m') AS order_month,
    COUNT(*) AS total_orders,
    SUM(CASE WHEN order_status = 'Cancelled' THEN 1 ELSE 0 END) AS cancelled_orders,
    ROUND(
        SUM(CASE WHEN order_status = 'Cancelled' THEN 1 ELSE 0 END) * 100.0
        / COUNT(*),
        2
    ) AS cancellation_rate_pct
FROM orders
GROUP BY order_month
ORDER BY order_month;

-- How often are customers failing to pay after placing orders?

SELECT
    DATE_FORMAT(payment_date, '%Y-%m') AS payment_month,
    COUNT(*) AS total_payments,
    SUM(CASE WHEN payment_status = 'Failed' THEN 1 ELSE 0 END) AS failed_payments,
    ROUND(
        SUM(CASE WHEN payment_status = 'Failed' THEN 1 ELSE 0 END) * 100.0
        / COUNT(*),
        2
    ) AS payment_failure_rate_pct
FROM payments
GROUP BY payment_month
ORDER BY payment_month;

-- Are delivery delays becoming more frequent?

SELECT
    DATE_FORMAT(o.order_date, '%Y-%m') AS order_month,
    COUNT(*) AS total_orders,
    SUM(op.sla_breached) AS breached_orders,
    ROUND(
        SUM(op.sla_breached) * 100.0 / COUNT(*),
        2
    ) AS sla_breach_rate_pct
FROM orders o
JOIN operations op ON o.order_id = op.order_id
GROUP BY order_month
ORDER BY order_month;

-- How much revenue are we giving back over time?

SELECT
    DATE_FORMAT(refund_date, '%Y-%m') AS refund_month,
    COUNT(*) AS total_refunds,
    ROUND(SUM(refund_amount), 2) AS total_refund_amount,
    ROUND(AVG(refund_amount), 2) AS avg_refund_amount
FROM refunds
GROUP BY refund_month
ORDER BY refund_month;


USE risk_analysis;

-- Did cancellations suddenly jump compared to last month?

WITH monthly_cancellations AS (
    SELECT
        DATE_FORMAT(order_date, '%Y-%m') AS order_month,
        COUNT(*) AS total_orders,
        SUM(CASE WHEN order_status = 'Cancelled' THEN 1 ELSE 0 END) AS cancelled_orders,
        ROUND(
            SUM(CASE WHEN order_status = 'Cancelled' THEN 1 ELSE 0 END) * 100.0
            / COUNT(*),
            2
        ) AS cancellation_rate_pct
    FROM orders
    GROUP BY order_month
)
SELECT
    order_month,
    cancellation_rate_pct,
    LAG(cancellation_rate_pct) OVER (ORDER BY order_month) AS prev_month_rate,
    ROUND(
        cancellation_rate_pct -
        LAG(cancellation_rate_pct) OVER (ORDER BY order_month),
        2
    ) AS mom_change_pct
FROM monthly_cancellations
ORDER BY order_month;



-- Are payment failures accelerating unexpectedly?

WITH monthly_payment_failures AS (
    SELECT
        DATE_FORMAT(payment_date, '%Y-%m') AS payment_month,
        ROUND(
            SUM(CASE WHEN payment_status = 'Failed' THEN 1 ELSE 0 END) * 100.0
            / COUNT(*),
            2
        ) AS failure_rate_pct
    FROM payments
    GROUP BY payment_month
)
SELECT
    payment_month,
    failure_rate_pct,
    LAG(failure_rate_pct) OVER (ORDER BY payment_month) AS prev_month_rate,
    ROUND(
        failure_rate_pct -
        LAG(failure_rate_pct) OVER (ORDER BY payment_month),
        2
    ) AS mom_change_pct
FROM monthly_payment_failures
ORDER BY payment_month;



-- Did operational delays suddenly worsen?

WITH monthly_sla AS (
    SELECT
        DATE_FORMAT(o.order_date, '%Y-%m') AS order_month,
        ROUND(
            SUM(op.sla_breached) * 100.0 / COUNT(*),
            2
        ) AS sla_breach_rate_pct
    FROM orders o
    JOIN operations op ON o.order_id = op.order_id
    GROUP BY order_month
)
SELECT
    order_month,
    sla_breach_rate_pct,
    LAG(sla_breach_rate_pct) OVER (ORDER BY order_month) AS prev_month_rate,
    ROUND(
        sla_breach_rate_pct -
        LAG(sla_breach_rate_pct) OVER (ORDER BY order_month),
        2
    ) AS mom_change_pct
FROM monthly_sla
ORDER BY order_month;


-- Flag risk if MoM change > +2%
/* CASE
    WHEN mom_change_pct > 2 THEN 'High Risk'
    WHEN mom_change_pct BETWEEN 1 AND 2 THEN 'Medium Risk'
    ELSE 'Normal'
END AS risk_flag */

USE risk_analysis;


-- How much potential revenue are we losing because of cancellations?

SELECT
    DATE_FORMAT(order_date, '%Y-%m') AS order_month,
    COUNT(*) AS cancelled_orders,
    ROUND(SUM(order_amount), 2) AS cancelled_revenue
FROM orders
WHERE order_status = 'Cancelled'
GROUP BY order_month
ORDER BY cancelled_revenue DESC;



-- How much revenue is blocked due to failed payments?
SELECT
    DATE_FORMAT(p.payment_date, '%Y-%m') AS payment_month,
    COUNT(*) AS failed_payments,
    ROUND(SUM(o.order_amount), 2) AS blocked_revenue
FROM payments p
JOIN orders o ON p.order_id = o.order_id
WHERE p.payment_status = 'Failed'
GROUP BY payment_month
ORDER BY blocked_revenue DESC;



-- How much revenue have we already earned but given back?
SELECT
    DATE_FORMAT(refund_date, '%Y-%m') AS refund_month,
    COUNT(*) AS refund_count,
    ROUND(SUM(refund_amount), 2) AS refunded_revenue
FROM refunds
GROUP BY refund_month
ORDER BY refunded_revenue DESC;


-- are SLA breaches actually causing refunds?

/*SELECT
    CASE
        WHEN op.sla_breached = 1 THEN 'SLA Breached'
        ELSE 'SLA Met'
    END AS sla_status,
    COUNT(DISTINCT o.order_id) AS orders,
    COUNT(DISTINCT r.refund_id) AS refunds,
    ROUND(
        COUNT(DISTINCT r.refund_id) * 100.0 /
        COUNT(DISTINCT o.order_id),
        2
    ) AS refund_rate_pct
FROM orders o
JOIN operations op ON o.order_id = op.order_id*/



-- Which risk should we fix first?

	SELECT
		'Cancellations' AS risk_type,
		ROUND(SUM(order_amount), 2) AS revenue_impact
	FROM orders
	WHERE order_status = 'Cancelled'

	UNION ALL

	SELECT
		'Payment Failures',
		ROUND(SUM(o.order_amount), 2)
	FROM payments p
	JOIN orders o ON p.order_id = o.order_id
	WHERE p.payment_status = 'Failed'

	UNION ALL

	SELECT
		'Refunds',
		ROUND(SUM(refund_amount), 2)
	FROM refunds
	ORDER BY revenue_impact DESC;













    
    

























