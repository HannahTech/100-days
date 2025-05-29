-- 1) Creating Data in SQL

-- Select default Database
use master;

-- Delete a Database
drop database if exists winners;

-- CREATE DATABASE
create DATABASE winners;

-- Select a Specific Database
use winners;

-- Delete table
drop table if exists products;

-- Create a New Table
create table products(
    product_id INT PRIMARY KEY,
    product_name VARCHAR(50),
    product_category VARCHAR(50),
    price DECIMAL(4, 2)
);

-- Clear Table or Remove all rows
truncate table products;

-- Insert datas into Table
insert into products VALUES
    (1, 'florar dress', 'woman_cloth', 14.99),
    (2, 'yellow tshirt', 'kids_cloth', 2.99),
    (3, 'sport tshirt', 'woman_cloth', 5.99),
    (4, 'brown_shoes', 'man_cloth', 24.99),
    (5, 'wine glass', 'household', 6.99);

-- Modify An Existing Table
ALTER TABLE products
ADD product_desc VARCHAR(50);

-- Update an Existing Table
update products
SET product_desc = CASE product_id
    WHEN 1 THEN 'Summer floral dress'
    WHEN 2 THEN 'Bright yellow kids shirt'
    WHEN 3 THEN 'Comfortable sports tee'
    WHEN 4 THEN 'Elegant brown leather shoes'
    WHEN 5 THEN 'Crystal wine glass'
    ELSE NULL
END;

-- Show all of table
SELECT * FROM products;

-- Only shows all value and all duplicates
SELECT product_name, price FROM products;

-- 2) Reading/Querying Data in SQL

-- Only shows unique value and don't show duplicates
SELECT DISTINCT product_name, price FROM products;

-- Only shows value with specific price
SELECT product_name, price FROM products WHERE price > 5;

-- Only shows 3 first values with specific price
SELECT TOP(3) product_name, price FROM products WHERE price > 5;

-- Only shows 3 more expensive
SELECT TOP(3) product_name, price
FROM products
WHERE price > 5
ORDER BY price DESC;

-- Order products from low to high
SELECT product_name, price 
FROM products
ORDER BY price;

-- Order products from low to high
SELECT product_name, price 
FROM products
ORDER BY price ASC;

-- Shows datas with order high to low price
SELECT product_name, price
FROM products
ORDER BY price DESC;

-- Shows 2 datas with order low to high price except first 2
SELECT product_name, price 
FROM products
ORDER BY price
OFFSET 2 ROWS FETCH NEXT 2 ROWS ONLY;

-- create price_category based on prices, Using Case condition
SELECT
    CASE
        WHEN price < 5 THEN 'Cheap'
        WHEN price BETWEEN 5 AND 10 THEN 'Normal'
        WHEN price > 10 THEN 'Expensive'
    END AS price_categpry,
    product_name,
    product_category,
    product_desc,
    price    
FROM products;

-- 3) Updating/Manipulating/Deleting Data in SQL

-- change product_category
UPDATE products
SET product_category='house_staff'
WHERE product_category= 'household';

-- 4) Filtering Data in SQL

-- delete one specific row
DELETE FROM products
WHERE product_name='sport tshirt';

-- shows specific rows include woman at first
SELECT * FROM products
WHERE product_category LIKE 'woman%';

-- shows rows includes one list
SELECT * FROM products
WHERE product_category IN ('man_cloth', 'woman_cloth');

-- 5) SQL Operator

-- shows rows includes one list
SELECT * FROM products
WHERE product_category LIKE 'man%'
   OR product_category LIKE 'woman%'
   OR product_category LIKE 'kid%';

-- shows rows excludes cloth at the end
SELECT * FROM products
WHERE product_category NOT LIKE '%cloth';

-- insert NULL value
INSERT INTO products VALUES
(6, 'pot', 'house_staff', 19.99, NULL);

-- show rows includes NULL
SELECT * FROM products 
WHERE product_desc IS NULL;

-- 6) Aggregation Data in SQL

-- Count all products number
SELECT COUNT(*) AS product_number 
FROM products;

-- Count category values products
SELECT product_category, COUNT(*) AS product_count
FROM products
GROUP BY product_category;

-- Count category values products more than 1 (Having: Filter Groups Based on Specified Conditions)
SELECT product_category, COUNT(*) AS product_count
FROM products
GROUP BY product_category
HAVING COUNT(*) > 1;

-- sum all product prices
SELECT SUM(price) AS Value FROM products;

-- Average all product prices
SELECT AVG(price) AS Average FROM products;

-- Average all product prices up to 2 digits but shows 6 digits
SELECT ROUND(AVG(price), 2) AS Average FROM products;

-- Average all product prices up to 2 digits and shows 2 digits
SELECT CAST(ROUND(AVG(price), 2) AS DECIMAL(10,2)) AS Average FROM products;

-- Show only minimum price
SELECT MIN(price) AS minimum_price FROM products;

-- Show row with minimum price
SELECT * FROM products WHERE price = (SELECT MIN(price) FROM products);


-- Show only maximum price
SELECT MAX(price) AS maximum_price FROM products;

-- Show row with maximum price
SELECT * FROM products WHERE price = (SELECT MAX(price) FROM products);

-- 7) Constraints in SQL (Primary/Foreign Key/Not Null/UNIQUE/CHECK/)
drop table if exists products;
drop table if exists products_category;

create table products_category(
    category_id INT PRIMARY KEY,
    category_name VARCHAR(50) UNIQUE
);

create table products(
    product_id INT PRIMARY KEY,
    product_name VARCHAR(50) UNIQUE,
    product_category VARCHAR(50),
    price DECIMAL(4, 2) NOT NULL CHECK (price > 0),
    FOREIGN KEY (product_category) REFERENCES products_category(category_name)
);

insert into products_category VALUES
    (11, 'kid_cloth'),
    (12, 'woman_cloth'),
    (13, 'man_cloth'),
    (14, 'household'),
    (15, 'toys');

insert into products (product_id, product_name, product_category, price) VALUES
    (1, 'florar dress', 'woman_cloth', 14.99),
    (2, 'yellow tshirt', 'kid_cloth', 2.99),
    (3, 'sport tshirt', 'woman_cloth', 5.99),
    (4, 'brown_shoes', 'man_cloth', 24.99),
    (5, 'wine glass', 'household', 6.99),
    (6, 'patio_chair', 'household', 23.99),
    (7, NULL, NULL, 1.99),
    (8, 'man_cloth', 'woman_cloth', 7.99)

SELECT * FROM products;

SELECT * FROM products_category;

-- 8) Joins in SQL

-- INNER JOIN (Have Matching Values in Both Tables)
SELECT * FROM products
INNER JOIN products_category 
ON products.product_category = products_category.category_name;

-- LEFT JOIN (All Records from the Left Table and the Matched Records from the Right Table)
SELECT * FROM products
LEFT JOIN products_category 
ON products.product_category = products_category.category_name;

-- RIGHT JOIN (All Records from the Right Table and the Matched Records from the Left Table)
SELECT * FROM products
RIGHT JOIN products_category 
ON products.product_category = products_category.category_name;

-- FULL OUTER JOIN (All Records When There Is a Match in Either the Left or Right Table)
SELECT * FROM products
FULL OUTER JOIN products_category 
ON products.product_category = products_category.category_name;

-- CROSS JOIN (Cartesian Product of the Two Tables)
SELECT * FROM products
CROSS JOIN products_category;

-- SELF JOIN (Joins a Table to Itself)
SELECT p1.product_name, p2.product_name
FROM products p1, products p2
WHERE p1.product_name = p2.product_category;

-- 9) SQL Functions

-- Scalar Functions (Return a Single Value)
SELECT product_category, UPPER(LEFT(product_category, 1)) AS category_initial FROM products;

ALTER TABLE products
ADD category_initial VARCHAR(2);

update products
SET category_initial = UPPER(LEFT(product_category, 1));

SELECT * FROM products;

-- Aggregate Functions (Operate on a Set of Values and Return a Single Value)
SELECT AVG(price) AS average_price FROM products;

-- String Functions (Manipulate String Values)
SELECT CONCAT(category_initial, ' -> ', product_category) AS full_category FROM products;

SELECT SUBSTRING(product_category,1, 2) AS category_initial FROM products;

SELECT STUFF(full_category, 2, 0, ' Initial ') AS modified_category 
FROM (
    SELECT CONCAT(category_initial, ': ', product_category) AS full_category 
    FROM products
) AS pro_cat;

-- Date and Time Functions
SELECT GETDATE() AS current_datetime;

-- Mathematical Functions
SELECT SQRT(25) AS square_root;

-- 10) Subqueries in SQL
-- Single-row Subquery (Row of Result)
SELECT * FROM products WHERE price = (SELECT MAX(price) FROM products);

-- Multiple-row Subquery
SELECT * FROM products WHERE product_category IN (SELECT category_name FROM products_category);

-- Correlated Subquery (References a Column from the Outer Query)
-- prices higher than overall average
SELECT product_name, price FROM products p
WHERE price > (SELECT AVG(price) FROM products);

-- checks if the product's price is greater than the average price of its own category
SELECT product_name, price FROM products p
WHERE price > (SELECT AVG(price) FROM products WHERE product_category = p.product_category);

-- Nested Subquery: A Subquery Inside Another Subquery
SELECT product_name, category_initial FROM products
WHERE product_category IN (SELECT category_name FROM products_category WHERE category_id = 11);

-- 11) Views in SQL
-- CREATE VIEW (Create a Virtual Table)
DROP VIEW IF EXISTS expensive_product;

CREATE VIEW expensive_product AS
SELECT product_name, price FROM products
WHERE price > 15;

SELECT * FROM expensive_product;

-- 12) Indexes in SQL
-- Create an Index on a Table
DROP INDEX IF EXISTS products.idx_products;

CREATE INDEX idx_products ON products(product_category);

-- 13) Transactions in SQL
-- For example we changed all prices to 0.00 and we can't undo that because all table now changed and we need to run all commands again
-- >>> UPDATE products SET price = 0.5;
-- For that reason we create one transactions which can undo our steps
-- Start a New Transaction
BEGIN TRANSACTION;

UPDATE products SET price = 0.5;

SELECT * FROM products; -- All prices changed to 0.5

-- Undo Changes
ROLLBACK;

SELECT * FROM products; -- All prices back to original one

-- Save Changes Made
BEGIN TRANSACTION;

UPDATE products SET price = 0.5 WHERE product_id = 1;

SELECT * FROM products; -- Only price of id = 1 changed to 0.5

COMMIT;


-- 14) Procedures/Triggers
-- Procedures (Like Functions in Coding)
CREATE PROCEDURE get_product_count
AS
BEGIN
  SELECT COUNT(*) AS product_count FROM products;
END;

EXEC get_product_count;

-- Procedure with input
CREATE PROCEDURE get_products_by_category
    @category VARCHAR(50)
AS
BEGIN
    SELECT * FROM products
    WHERE product_category = @category;
END;

EXEC get_products_by_category @category = 'woman_cloth';

-- Triggers (Automatically Execute a Set of SQL Statements When a Specified Event Occurs)
ALTER TABLE products
ADD creation_date DATETIME;


CREATE TRIGGER trg_set_creation_date
ON products
AFTER INSERT
AS
BEGIN
  UPDATE p
  SET creation_date = GETDATE()
  FROM products p
  INNER JOIN inserted i ON p.product_id = i.product_id;
END;

INSERT INTO products (product_id, product_name, price, product_category)
VALUES (9, 'iphone_case', 9.99, 'household');

SELECT * FROM products;
