
------CREAR LOS bases de datos, en este caso usamos 4 contenedores, el 1 usa la region A, el 2 la region B, el 3 la region C y D 
------ Y el 4 es el que usamos para manejar todo, este script esta hecho para correr el la base 4, las demas bases son para distribuir los fragmentos
-------basicamente todo lo que se haga de crud pasara en la base 4 y hara replicacion dependiendo de la region a la que pertenezcan los datos, principalmente
-------basandose en la tabla customers, asi que esa es la primera que se crea y a la que se le inserta, la tabla de productos es independiente de la region por lo que esta en todos
------- si se hacen cambios a una base de datos ya sea la 1 2 o 3 lo podremos ver en la base 4 mediante la vista materializada refrescando en demanda.
--------Los procedmientos almacenados hacen el crud checando siempre que no existan cosas ya en la base de datos.

---Crear los database links
-------NOTA: cambiar el host por la IP de la instancia, es importante prender los contenedores en orden, porque el primero que enciende es el primero que tiene la primera ip
create database link link_b4_a_b1 connect to system identified by "123" using '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=172.17.0.X)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=XE)))';
create database link link_b4_a_b2 connect to system identified by "123" using '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=172.17.0.X)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=XE)))';
create database link link_b4_a_b3 connect to system identified by "123" using '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=172.17.0.X)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=XE)))';

--- Creacion de sinonimos
create SYNONYM customersb1 for customers@link_b4_a_b1;
create SYNONYM customersb2 for customers@link_b4_a_b2;
create SYNONYM customersb3 for customers@link_b4_a_b3;

create SYNONYM ordersb1 for orders@link_b4_a_b1;
create SYNONYM ordersb2 for orders@link_b4_a_b2;
create SYNONYM ordersb3 for orders@link_b4_a_b3;

create SYNONYM productsb1 for product_information@link_b4_a_b1;
create SYNONYM productsb2 for product_information@link_b4_a_b2;
create SYNONYM productsb3 for product_information@link_b4_a_b3;

create SYNONYM itemsb1 for order_items@link_b4_a_b1;
create SYNONYM itemsb2 for order_items@link_b4_a_b2;
create SYNONYM itemsb3 for order_items@link_b4_a_b3;

--- Creacion de triggers

---Para customers
create or replace NONEDITIONABLE TRIGGER trg_replicacion_insertar
AFTER INSERT ON CUSTOMERS
FOR EACH ROW
BEGIN
    IF :NEW.REGION = 'A' THEN
        INSERT INTO CUSTOMERS@link_b4_a_b1 (CUSTOMER_ID, CUST_FIRST_NAME, CUST_LAST_NAME, CREDIT_LIMIT, CUST_EMAIL, INCOME_LEVEL, REGION)
        VALUES (:NEW.CUSTOMER_ID, :NEW.CUST_FIRST_NAME, :NEW.CUST_LAST_NAME, :NEW.CREDIT_LIMIT, :NEW.CUST_EMAIL, :NEW.INCOME_LEVEL, :NEW.REGION);
    ELSIF :NEW.REGION = 'B' THEN
        INSERT INTO CUSTOMERS@link_b4_a_b2 (CUSTOMER_ID, CUST_FIRST_NAME, CUST_LAST_NAME, CREDIT_LIMIT, CUST_EMAIL, INCOME_LEVEL, REGION)
        VALUES (:NEW.CUSTOMER_ID, :NEW.CUST_FIRST_NAME, :NEW.CUST_LAST_NAME, :NEW.CREDIT_LIMIT, :NEW.CUST_EMAIL, :NEW.INCOME_LEVEL, :NEW.REGION);
    ELSIF :NEW.REGION IN ('C', 'D') THEN
        INSERT INTO CUSTOMERS@link_b4_a_b3 (CUSTOMER_ID, CUST_FIRST_NAME, CUST_LAST_NAME, CREDIT_LIMIT, CUST_EMAIL, INCOME_LEVEL, REGION)
        VALUES (:NEW.CUSTOMER_ID, :NEW.CUST_FIRST_NAME, :NEW.CUST_LAST_NAME, :NEW.CREDIT_LIMIT, :NEW.CUST_EMAIL, :NEW.INCOME_LEVEL, :NEW.REGION);
    ELSE
        RAISE_APPLICATION_ERROR(-20001, 'Invalid region');
    END IF;
END;

CREATE OR REPLACE TRIGGER trg_replicacion_actualizar
AFTER UPDATE ON CUSTOMERS
FOR EACH ROW
BEGIN
    IF :NEW.REGION = 'A' THEN
        UPDATE customersb1
        SET CUST_FIRST_NAME = :NEW.CUST_FIRST_NAME,
            CUST_LAST_NAME = :NEW.CUST_LAST_NAME,
            CREDIT_LIMIT = :NEW.CREDIT_LIMIT,
            CUST_EMAIL = :NEW.CUST_EMAIL,
            INCOME_LEVEL = :NEW.INCOME_LEVEL,
            REGION = :NEW.REGION
        WHERE CUSTOMER_ID = :OLD.CUSTOMER_ID;
    ELSIF :NEW.REGION = 'B' THEN
        UPDATE customersb2
        SET CUST_FIRST_NAME = :NEW.CUST_FIRST_NAME,
            CUST_LAST_NAME = :NEW.CUST_LAST_NAME,
            CREDIT_LIMIT = :NEW.CREDIT_LIMIT,
            CUST_EMAIL = :NEW.CUST_EMAIL,
            INCOME_LEVEL = :NEW.INCOME_LEVEL,
            REGION = :NEW.REGION
        WHERE CUSTOMER_ID = :OLD.CUSTOMER_ID;
    ELSIF :NEW.REGION IN ('C', 'D') THEN
        UPDATE customersb3
        SET CUST_FIRST_NAME = :NEW.CUST_FIRST_NAME,
            CUST_LAST_NAME = :NEW.CUST_LAST_NAME,
            CREDIT_LIMIT = :NEW.CREDIT_LIMIT,
            CUST_EMAIL = :NEW.CUST_EMAIL,
            INCOME_LEVEL = :NEW.INCOME_LEVEL,
            REGION = :NEW.REGION
        WHERE CUSTOMER_ID = :OLD.CUSTOMER_ID;
    ELSE
        RAISE_APPLICATION_ERROR(-20001, 'Invalid region');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER trg_replicacion_borrar
AFTER DELETE ON CUSTOMERS
FOR EACH ROW
BEGIN
    IF :OLD.REGION = 'A' THEN
        DELETE FROM customersb1
        WHERE CUSTOMER_ID = :OLD.CUSTOMER_ID;
    ELSIF :OLD.REGION = 'B' THEN
        DELETE FROM customersb2
        WHERE CUSTOMER_ID = :OLD.CUSTOMER_ID;
    ELSIF :OLD.REGION IN ('C', 'D') THEN
        DELETE FROM customersb3
        WHERE CUSTOMER_ID = :OLD.CUSTOMER_ID;
    ELSE
        RAISE_APPLICATION_ERROR(-20001, 'Invalid region');
    END IF;
END;
/


--Para ORDERS

CREATE OR REPLACE NONEDITIONABLE TRIGGER trg_replicacion_insertar_ordenes
AFTER INSERT ON ORDERS
FOR EACH ROW
BEGIN
    DECLARE
        v_region CHAR(1);
    BEGIN
        -- checar la region del cliente que pidio la orden
        SELECT REGION INTO v_region
        FROM CUSTOMERS
        WHERE CUSTOMER_ID = :NEW.CUSTOMER_ID;
        
        IF v_region = 'A' THEN
            INSERT INTO ORDERS@link_b4_a_b1 (ORDER_ID, ORDER_DATE, ORDER_MODE, CUSTOMER_ID, ORDER_STATUS, ORDER_TOTAL, SALES_REP_ID, PROMOTION_ID)
            VALUES (:NEW.ORDER_ID, :NEW.ORDER_DATE, :NEW.ORDER_MODE, :NEW.CUSTOMER_ID, :NEW.ORDER_STATUS, :NEW.ORDER_TOTAL, :NEW.SALES_REP_ID, :NEW.PROMOTION_ID);
        ELSIF v_region = 'B' THEN
            INSERT INTO ORDERS@link_b4_a_b2 (ORDER_ID, ORDER_DATE, ORDER_MODE, CUSTOMER_ID, ORDER_STATUS, ORDER_TOTAL, SALES_REP_ID, PROMOTION_ID)
            VALUES (:NEW.ORDER_ID, :NEW.ORDER_DATE, :NEW.ORDER_MODE, :NEW.CUSTOMER_ID, :NEW.ORDER_STATUS, :NEW.ORDER_TOTAL, :NEW.SALES_REP_ID, :NEW.PROMOTION_ID);
        ELSIF v_region IN ('C', 'D') THEN
            INSERT INTO ORDERS@link_b4_a_b3 (ORDER_ID, ORDER_DATE, ORDER_MODE, CUSTOMER_ID, ORDER_STATUS, ORDER_TOTAL, SALES_REP_ID, PROMOTION_ID)
            VALUES (:NEW.ORDER_ID, :NEW.ORDER_DATE, :NEW.ORDER_MODE, :NEW.CUSTOMER_ID, :NEW.ORDER_STATUS, :NEW.ORDER_TOTAL, :NEW.SALES_REP_ID, :NEW.PROMOTION_ID);
        ELSE
            RAISE_APPLICATION_ERROR(-20001, 'Invalid region');
        END IF;
    END;
END;
/

CREATE OR REPLACE NONEDITIONABLE TRIGGER trg_replicacion_actualizar_ordenes
AFTER UPDATE ON ORDERS
FOR EACH ROW
BEGIN
    DECLARE
        v_region CHAR(1);
    BEGIN
        SELECT REGION INTO v_region
        FROM CUSTOMERS
        WHERE CUSTOMER_ID = :NEW.CUSTOMER_ID;
        
        IF v_region = 'A' THEN
            UPDATE ORDERS@link_b4_a_b1
            SET ORDER_DATE = :NEW.ORDER_DATE,
                ORDER_MODE = :NEW.ORDER_MODE,
                CUSTOMER_ID = :NEW.CUSTOMER_ID,
                ORDER_STATUS = :NEW.ORDER_STATUS,
                ORDER_TOTAL = :NEW.ORDER_TOTAL,
                SALES_REP_ID = :NEW.SALES_REP_ID,
                PROMOTION_ID = :NEW.PROMOTION_ID
            WHERE ORDER_ID = :OLD.ORDER_ID;
        ELSIF v_region = 'B' THEN
            UPDATE ORDERS@link_b4_a_b2
            SET ORDER_DATE = :NEW.ORDER_DATE,
                ORDER_MODE = :NEW.ORDER_MODE,
                CUSTOMER_ID = :NEW.CUSTOMER_ID,
                ORDER_STATUS = :NEW.ORDER_STATUS,
                ORDER_TOTAL = :NEW.ORDER_TOTAL,
                SALES_REP_ID = :NEW.SALES_REP_ID,
                PROMOTION_ID = :NEW.PROMOTION_ID
            WHERE ORDER_ID = :OLD.ORDER_ID;
        ELSIF v_region IN ('C', 'D') THEN
            UPDATE ORDERS@link_b4_a_b3
            SET ORDER_DATE = :NEW.ORDER_DATE,
                ORDER_MODE = :NEW.ORDER_MODE,
                CUSTOMER_ID = :NEW.CUSTOMER_ID,
                ORDER_STATUS = :NEW.ORDER_STATUS,
                ORDER_TOTAL = :NEW.ORDER_TOTAL,
                SALES_REP_ID = :NEW.SALES_REP_ID,
                PROMOTION_ID = :NEW.PROMOTION_ID
            WHERE ORDER_ID = :OLD.ORDER_ID;
        ELSE
            RAISE_APPLICATION_ERROR(-20001, 'Invalid region');
        END IF;
    END;
END;
/

CREATE OR REPLACE  TRIGGER trg_replicacion_borrar_ordenes
AFTER DELETE ON ORDERS
FOR EACH ROW
BEGIN
    DECLARE
        v_region CHAR(1);
    BEGIN
        SELECT REGION INTO v_region
        FROM CUSTOMERS
        WHERE CUSTOMER_ID = :OLD.CUSTOMER_ID;
        
        IF v_region = 'A' THEN
            DELETE FROM ORDERS@link_b4_a_b1
            WHERE ORDER_ID = :OLD.ORDER_ID;
        ELSIF v_region = 'B' THEN
            DELETE FROM ORDERS@link_b4_a_b2
            WHERE ORDER_ID = :OLD.ORDER_ID;
        ELSIF v_region IN ('C', 'D') THEN
            DELETE FROM ORDERS@link_b4_a_b3
            WHERE ORDER_ID = :OLD.ORDER_ID;
        ELSE
            RAISE_APPLICATION_ERROR(-20001, 'Invalid region');
        END IF;
    END;
END;
/

-- PARA Order_items

CREATE OR REPLACE NONEDITIONABLE TRIGGER trg_replicacion_insertar_order_items
AFTER INSERT ON ORDER_ITEMS
FOR EACH ROW
BEGIN
    DECLARE
        v_region CHAR(1);
    BEGIN
        -- Obtener la región del pedido
        SELECT REGION INTO v_region
        FROM CUSTOMERS c
        JOIN ORDERS o ON c.CUSTOMER_ID = o.CUSTOMER_ID
        WHERE o.ORDER_ID = :NEW.ORDER_ID;

        IF v_region = 'A' THEN
            INSERT INTO itemsb1 (ORDER_ID, LINE_ITEM_ID, PRODUCT_ID, UNIT_PRICE, QUANTITY)
            VALUES (:NEW.ORDER_ID, :NEW.LINE_ITEM_ID, :NEW.PRODUCT_ID, :NEW.UNIT_PRICE, :NEW.QUANTITY);
        ELSIF v_region = 'B' THEN
            INSERT INTO itemsb2 (ORDER_ID, LINE_ITEM_ID, PRODUCT_ID, UNIT_PRICE, QUANTITY)
            VALUES (:NEW.ORDER_ID, :NEW.LINE_ITEM_ID, :NEW.PRODUCT_ID, :NEW.UNIT_PRICE, :NEW.QUANTITY);
        ELSIF v_region IN ('C', 'D') THEN
            INSERT INTO itemsb3 (ORDER_ID, LINE_ITEM_ID, PRODUCT_ID, UNIT_PRICE, QUANTITY)
            VALUES (:NEW.ORDER_ID, :NEW.LINE_ITEM_ID, :NEW.PRODUCT_ID, :NEW.UNIT_PRICE, :NEW.QUANTITY);
        ELSE
            RAISE_APPLICATION_ERROR(-20001, 'Invalid region');
        END IF;
    END;
END;
/
CREATE OR REPLACE NONEDITIONABLE TRIGGER trg_replicacion_actualizar_order_items
AFTER UPDATE ON ORDER_ITEMS
FOR EACH ROW
BEGIN
    DECLARE
        v_region CHAR(1);
    BEGIN
        -- Obtener la región del pedido 
        SELECT REGION INTO v_region
        FROM CUSTOMERS c
        JOIN ORDERS o ON c.CUSTOMER_ID = o.CUSTOMER_ID
        WHERE o.ORDER_ID = :NEW.ORDER_ID;

        IF v_region = 'A' THEN
            UPDATE itemsb1
            SET LINE_ITEM_ID = :NEW.LINE_ITEM_ID,
                PRODUCT_ID = :NEW.PRODUCT_ID,
                UNIT_PRICE = :NEW.UNIT_PRICE,
                QUANTITY = :NEW.QUANTITY
            WHERE ORDER_ID = :OLD.ORDER_ID AND LINE_ITEM_ID = :OLD.LINE_ITEM_ID;
        ELSIF v_region = 'B' THEN
            UPDATE itemsb2
            SET LINE_ITEM_ID = :NEW.LINE_ITEM_ID,
                PRODUCT_ID = :NEW.PRODUCT_ID,
                UNIT_PRICE = :NEW.UNIT_PRICE,
                QUANTITY = :NEW.QUANTITY
            WHERE ORDER_ID = :OLD.ORDER_ID AND LINE_ITEM_ID = :OLD.LINE_ITEM_ID;
        ELSIF v_region IN ('C', 'D') THEN
            UPDATE itemsb3
            SET LINE_ITEM_ID = :NEW.LINE_ITEM_ID,
                PRODUCT_ID = :NEW.PRODUCT_ID,
                UNIT_PRICE = :NEW.UNIT_PRICE,
                QUANTITY = :NEW.QUANTITY
            WHERE ORDER_ID = :OLD.ORDER_ID AND LINE_ITEM_ID = :OLD.LINE_ITEM_ID;
        ELSE
            RAISE_APPLICATION_ERROR(-20001, 'Invalid region');
        END IF;
    END;
END;
/
CREATE OR REPLACE  TRIGGER trg_replicacion_borrar_order_items
AFTER DELETE ON ORDER_ITEMS
FOR EACH ROW
BEGIN
    DECLARE
        v_region CHAR(1);
    BEGIN
        -- Obtener la región del pedido
        SELECT REGION INTO v_region
        FROM CUSTOMERS c
        JOIN ORDERS o ON c.CUSTOMER_ID = o.CUSTOMER_ID
        WHERE o.ORDER_ID = :OLD.ORDER_ID;

        IF v_region = 'A' THEN
            DELETE FROM itemsb1
            WHERE ORDER_ID = :OLD.ORDER_ID AND LINE_ITEM_ID = :OLD.LINE_ITEM_ID;
        ELSIF v_region = 'B' THEN
            DELETE FROM itemsb2
            WHERE ORDER_ID = :OLD.ORDER_ID AND LINE_ITEM_ID = :OLD.LINE_ITEM_ID;
        ELSIF v_region IN ('C', 'D') THEN
            DELETE FROM itemsb3
            WHERE ORDER_ID = :OLD.ORDER_ID AND LINE_ITEM_ID = :OLD.LINE_ITEM_ID;
        ELSE
            RAISE_APPLICATION_ERROR(-20001, 'Invalid region');
        END IF;
    END;
END;
/

--PARA PRODUCTOS
CREATE OR REPLACE  TRIGGER trg_replicacion_insertar_productos
AFTER INSERT ON PRODUCT_INFORMATION
FOR EACH ROW
BEGIN
    INSERT INTO productsb1 (PRODUCT_ID, PRODUCT_NAME, PRODUCT_DESCRIPTION, CATEGORY_ID, WEIGHT_CLASS, WARRANTY_PERIOD, SUPPLIER_ID, PRODUCT_STATUS, LIST_PRICE, MIN_PRICE, CATALOG_URL)
    VALUES (:NEW.PRODUCT_ID, :NEW.PRODUCT_NAME, :NEW.PRODUCT_DESCRIPTION, :NEW.CATEGORY_ID, :NEW.WEIGHT_CLASS, :NEW.WARRANTY_PERIOD, :NEW.SUPPLIER_ID, :NEW.PRODUCT_STATUS, :NEW.LIST_PRICE, :NEW.MIN_PRICE, :NEW.CATALOG_URL);
    
    INSERT INTO productsb2 (PRODUCT_ID, PRODUCT_NAME, PRODUCT_DESCRIPTION, CATEGORY_ID, WEIGHT_CLASS, WARRANTY_PERIOD, SUPPLIER_ID, PRODUCT_STATUS, LIST_PRICE, MIN_PRICE, CATALOG_URL)
    VALUES (:NEW.PRODUCT_ID, :NEW.PRODUCT_NAME, :NEW.PRODUCT_DESCRIPTION, :NEW.CATEGORY_ID, :NEW.WEIGHT_CLASS, :NEW.WARRANTY_PERIOD, :NEW.SUPPLIER_ID, :NEW.PRODUCT_STATUS, :NEW.LIST_PRICE, :NEW.MIN_PRICE, :NEW.CATALOG_URL);
    
    INSERT INTO productsb3 (PRODUCT_ID, PRODUCT_NAME, PRODUCT_DESCRIPTION, CATEGORY_ID, WEIGHT_CLASS, WARRANTY_PERIOD, SUPPLIER_ID, PRODUCT_STATUS, LIST_PRICE, MIN_PRICE, CATALOG_URL)
    VALUES (:NEW.PRODUCT_ID, :NEW.PRODUCT_NAME, :NEW.PRODUCT_DESCRIPTION, :NEW.CATEGORY_ID, :NEW.WEIGHT_CLASS, :NEW.WARRANTY_PERIOD, :NEW.SUPPLIER_ID, :NEW.PRODUCT_STATUS, :NEW.LIST_PRICE, :NEW.MIN_PRICE, :NEW.CATALOG_URL);
END;
/

CREATE OR REPLACE NONEDITIONABLE TRIGGER trg_replicacion_product_update
AFTER UPDATE ON PRODUCT_INFORMATION
FOR EACH ROW
BEGIN
    UPDATE productsb1
    SET PRODUCT_NAME = :NEW.PRODUCT_NAME,
        PRODUCT_DESCRIPTION = :NEW.PRODUCT_DESCRIPTION,
        CATEGORY_ID = :NEW.CATEGORY_ID,
        WEIGHT_CLASS = :NEW.WEIGHT_CLASS,
        WARRANTY_PERIOD = :NEW.WARRANTY_PERIOD,
        SUPPLIER_ID = :NEW.SUPPLIER_ID,
        PRODUCT_STATUS = :NEW.PRODUCT_STATUS,
        LIST_PRICE = :NEW.LIST_PRICE,
        MIN_PRICE = :NEW.MIN_PRICE,
        CATALOG_URL = :NEW.CATALOG_URL
    WHERE PRODUCT_ID = :OLD.PRODUCT_ID;

    UPDATE productsb2
    SET PRODUCT_NAME = :NEW.PRODUCT_NAME,
        PRODUCT_DESCRIPTION = :NEW.PRODUCT_DESCRIPTION,
        CATEGORY_ID = :NEW.CATEGORY_ID,
        WEIGHT_CLASS = :NEW.WEIGHT_CLASS,
        WARRANTY_PERIOD = :NEW.WARRANTY_PERIOD,
        SUPPLIER_ID = :NEW.SUPPLIER_ID,
        PRODUCT_STATUS = :NEW.PRODUCT_STATUS,
        LIST_PRICE = :NEW.LIST_PRICE,
        MIN_PRICE = :NEW.MIN_PRICE,
        CATALOG_URL = :NEW.CATALOG_URL
    WHERE PRODUCT_ID = :OLD.PRODUCT_ID;

    UPDATE productsb3
    SET PRODUCT_NAME = :NEW.PRODUCT_NAME,
        PRODUCT_DESCRIPTION = :NEW.PRODUCT_DESCRIPTION,
        CATEGORY_ID = :NEW.CATEGORY_ID,
        WEIGHT_CLASS = :NEW.WEIGHT_CLASS,
        WARRANTY_PERIOD = :NEW.WARRANTY_PERIOD,
        SUPPLIER_ID = :NEW.SUPPLIER_ID,
        PRODUCT_STATUS = :NEW.PRODUCT_STATUS,
        LIST_PRICE = :NEW.LIST_PRICE,
        MIN_PRICE = :NEW.MIN_PRICE,
        CATALOG_URL = :NEW.CATALOG_URL
    WHERE PRODUCT_ID = :OLD.PRODUCT_ID;
END;
/
CREATE OR REPLACE  TRIGGER trg_replicacion_product_delete
AFTER DELETE ON PRODUCT_INFORMATION
FOR EACH ROW
BEGIN
    DELETE FROM productsb1 WHERE PRODUCT_ID = :OLD.PRODUCT_ID;
    DELETE FROM productsb2 WHERE PRODUCT_ID = :OLD.PRODUCT_ID;
    DELETE FROM productsb3 WHERE PRODUCT_ID = :OLD.PRODUCT_ID;
END;
/



--- Vista materializadas

--- customers global
CREATE MATERIALIZED VIEW mv_customers_global
REFRESH ON DEMAND
AS
SELECT * FROM customersb1
UNION ALL
SELECT * FROM customersb2
UNION ALL
SELECT * FROM customersb3

--- products global
CREATE MATERIALIZED VIEW mv_products_global
REFRESH ON DEMAND
AS
SELECT * FROM productsb1
UNION
SELECT * FROM productsb2
UNION
SELECT * FROM productsb3

--- orders global
CREATE MATERIALIZED VIEW mv_orders_global
REFRESH ON DEMAND
AS
SELECT * FROM ordersb1
UNION
SELECT * FROM ordersb2
UNION
SELECT * FROM ordersb3

--- order_items global
CREATE MATERIALIZED VIEW mv_order_items_global
REFRESH ON DEMAND
AS
SELECT * FROM itemsb1
UNION
SELECT * FROM itemsb2
UNION
SELECT * FROM itemsb3


---CRUDS

------inserts

CREATE OR REPLACE PROCEDURE insert_customer(
    p_customer_id IN NUMBER,
    p_first_name IN VARCHAR2,
    p_last_name IN VARCHAR2,
    p_credit_limit IN NUMBER,
    p_email IN VARCHAR2,
    p_income_level IN VARCHAR2,
    p_region IN VARCHAR2
) AS
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM CUSTOMERS WHERE CUSTOMER_ID = p_customer_id;
    IF v_count = 0 THEN
        INSERT INTO CUSTOMERS (CUSTOMER_ID, CUST_FIRST_NAME, CUST_LAST_NAME, CREDIT_LIMIT, CUST_EMAIL, INCOME_LEVEL, REGION)
        VALUES (p_customer_id, p_first_name, p_last_name, p_credit_limit, p_email, p_income_level, p_region);
    ELSE
        DBMS_OUTPUT.PUT_LINE('El cliente con ID ' || p_customer_id || ' ya existe.');
    END IF;
END;
/

CREATE OR REPLACE PROCEDURE insert_order(
    p_order_id IN NUMBER,
    p_order_date IN TIMESTAMP,
    p_order_mode IN VARCHAR2,
    p_customer_id IN NUMBER,
    p_order_status IN NUMBER,
    p_order_total IN NUMBER,
    p_sales_rep_id IN NUMBER,
    p_promotion_id IN NUMBER
) AS
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM ORDERS WHERE ORDER_ID = p_order_id;
    IF v_count = 0 THEN
        INSERT INTO ORDERS (ORDER_ID, ORDER_DATE, ORDER_MODE, CUSTOMER_ID, ORDER_STATUS, ORDER_TOTAL, SALES_REP_ID, PROMOTION_ID)
        VALUES (p_order_id, p_order_date, p_order_mode, p_customer_id, p_order_status, p_order_total, p_sales_rep_id, p_promotion_id);
    ELSE
        DBMS_OUTPUT.PUT_LINE('La orden con ORDER_ID ' || p_order_id || ' ya existe.');
    END IF;
END;
/


CREATE OR REPLACE PROCEDURE insert_order_item(
    p_order_id IN NUMBER,
    p_line_item_id IN NUMBER,
    p_product_id IN NUMBER,
    p_unit_price IN NUMBER,
    p_quantity IN NUMBER
) AS
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM ORDER_ITEMS WHERE ORDER_ID = p_order_id AND LINE_ITEM_ID = p_line_item_id;
    IF v_count = 0 THEN
        INSERT INTO ORDER_ITEMS (ORDER_ID, LINE_ITEM_ID, PRODUCT_ID, UNIT_PRICE, QUANTITY)
        VALUES (p_order_id, p_line_item_id, p_product_id, p_unit_price, p_quantity);
    ELSE
        DBMS_OUTPUT.PUT_LINE('El elemento de orden con ORDER_ID ' || p_order_id || ' y LINE_ITEM_ID ' || p_line_item_id || ' ya existe.');
    END IF;
END;
/

CREATE OR REPLACE PROCEDURE insert_product(
    p_product_id IN NUMBER,
    p_product_name IN VARCHAR2,
    p_product_description IN VARCHAR2,
    p_category_id IN NUMBER,
    p_weight_class IN NUMBER,
    p_warranty_period IN INTERVAL YEAR TO MONTH,
    p_supplier_id IN NUMBER,
    p_product_status IN VARCHAR2,
    p_list_price IN NUMBER,
    p_min_price IN NUMBER,
    p_catalog_url IN VARCHAR2
) AS
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM PRODUCT_INFORMATION WHERE PRODUCT_ID = p_product_id;
    IF v_count = 0 THEN
        INSERT INTO PRODUCT_INFORMATION (PRODUCT_ID, PRODUCT_NAME, PRODUCT_DESCRIPTION, CATEGORY_ID, WEIGHT_CLASS, WARRANTY_PERIOD, SUPPLIER_ID, PRODUCT_STATUS, LIST_PRICE, MIN_PRICE, CATALOG_URL)
        VALUES (p_product_id, p_product_name, p_product_description, p_category_id, p_weight_class, p_warranty_period, p_supplier_id, p_product_status, p_list_price, p_min_price, p_catalog_url);
    ELSE
        DBMS_OUTPUT.PUT_LINE('El producto con PRODUCT_ID ' || p_product_id || ' ya existe.');
    END IF;
END;
/


--- updates

CREATE OR REPLACE PROCEDURE update_customer(
    p_customer_id IN NUMBER,
    p_first_name IN VARCHAR2,
    p_last_name IN VARCHAR2,
    p_credit_limit IN NUMBER,
    p_email IN VARCHAR2,
    p_income_level IN VARCHAR2,
    p_region IN VARCHAR2
) AS
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM CUSTOMERS WHERE CUSTOMER_ID = p_customer_id;
    IF v_count > 0 THEN
        UPDATE CUSTOMERS
        SET CUST_FIRST_NAME = p_first_name,
            CUST_LAST_NAME = p_last_name,
            CREDIT_LIMIT = p_credit_limit,
            CUST_EMAIL = p_email,
            INCOME_LEVEL = p_income_level,
            REGION = p_region
        WHERE CUSTOMER_ID = p_customer_id;
    ELSE
        DBMS_OUTPUT.PUT_LINE('El cliente con ID ' || p_customer_id || ' no existe.');
    END IF;
END;
/

CREATE OR REPLACE PROCEDURE update_order(
    p_order_id IN NUMBER,
    p_order_date IN TIMESTAMP,
    p_order_mode IN VARCHAR2,
    p_customer_id IN NUMBER,
    p_order_status IN NUMBER,
    p_order_total IN NUMBER,
    p_sales_rep_id IN NUMBER,
    p_promotion_id IN NUMBER
) AS
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM ORDERS WHERE ORDER_ID = p_order_id;
    IF v_count > 0 THEN
        UPDATE ORDERS
        SET ORDER_DATE = p_order_date,
            ORDER_MODE = p_order_mode,
            CUSTOMER_ID = p_customer_id,
            ORDER_STATUS = p_order_status,
            ORDER_TOTAL = p_order_total,
            SALES_REP_ID = p_sales_rep_id,
            PROMOTION_ID = p_promotion_id
        WHERE ORDER_ID = p_order_id;
    ELSE
        DBMS_OUTPUT.PUT_LINE('La orden con ORDER_ID ' || p_order_id || ' no existe.');
    END IF;
END;
/

CREATE OR REPLACE PROCEDURE update_order_item(
    p_order_id IN NUMBER,
    p_line_item_id IN NUMBER,
    p_product_id IN NUMBER,
    p_unit_price IN NUMBER,
    p_quantity IN NUMBER
) AS
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM ORDER_ITEMS WHERE ORDER_ID = p_order_id AND LINE_ITEM_ID = p_line_item_id;
    IF v_count > 0 THEN
        UPDATE ORDER_ITEMS
        SET PRODUCT_ID = p_product_id,
            UNIT_PRICE = p_unit_price,
            QUANTITY = p_quantity
        WHERE ORDER_ID = p_order_id AND LINE_ITEM_ID = p_line_item_id;
    ELSE
        DBMS_OUTPUT.PUT_LINE('El elemento de orden con ORDER_ID ' || p_order_id || ' y LINE_ITEM_ID ' || p_line_item_id || ' no existe.');
    END IF;
END;
/

CREATE OR REPLACE PROCEDURE update_product(
    p_product_id IN NUMBER,
    p_product_name IN VARCHAR2,
    p_product_description IN VARCHAR2,
    p_category_id IN NUMBER,
    p_weight_class IN NUMBER,
    p_warranty_period IN INTERVAL YEAR TO MONTH,
    p_supplier_id IN NUMBER,
    p_product_status IN VARCHAR2,
    p_list_price IN NUMBER,
    p_min_price IN NUMBER,
    p_catalog_url IN VARCHAR2
) AS
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM PRODUCT_INFORMATION WHERE PRODUCT_ID = p_product_id;
    IF v_count > 0 THEN
        UPDATE PRODUCT_INFORMATION
        SET PRODUCT_NAME = p_product_name,
            PRODUCT_DESCRIPTION = p_product_description,
            CATEGORY_ID = p_category_id,
            WEIGHT_CLASS = p_weight_class,
            WARRANTY_PERIOD = p_warranty_period,
            SUPPLIER_ID = p_supplier_id,
            PRODUCT_STATUS = p_product_status,
            LIST_PRICE = p_list_price,
            MIN_PRICE = p_min_price,
            CATALOG_URL = p_catalog_url
        WHERE PRODUCT_ID = p_product_id;
    ELSE
        DBMS_OUTPUT.PUT_LINE('El producto con PRODUCT_ID ' || p_product_id || ' no existe.');
    END IF;
END;
/

--- deletes

CREATE OR REPLACE PROCEDURE delete_customer(
    p_customer_id IN NUMBER
) AS
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM CUSTOMERS WHERE CUSTOMER_ID = p_customer_id;
    IF v_count > 0 THEN
        DELETE FROM CUSTOMERS WHERE CUSTOMER_ID = p_customer_id;
    ELSE
        DBMS_OUTPUT.PUT_LINE('El cliente con ID ' || p_customer_id || ' no existe.');
    END IF;
END;
/

CREATE OR REPLACE PROCEDURE delete_order(
    p_order_id IN NUMBER
) AS
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM ORDERS WHERE ORDER_ID = p_order_id;
    IF v_count > 0 THEN
        DELETE FROM ORDERS WHERE ORDER_ID = p_order_id;
    ELSE
        DBMS_OUTPUT.PUT_LINE('La orden con ORDER_ID ' || p_order_id || ' no existe.');
    END IF;
END;
/

CREATE OR REPLACE PROCEDURE delete_order_item(
    p_order_id IN NUMBER,
    p_line_item_id IN NUMBER
) AS
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM ORDER_ITEMS WHERE ORDER_ID = p_order_id AND LINE_ITEM_ID = p_line_item_id;
    IF v_count > 0 THEN
        DELETE FROM ORDER_ITEMS WHERE ORDER_ID = p_order_id AND LINE_ITEM_ID = p_line_item_id;
    ELSE
        DBMS_OUTPUT.PUT_LINE('El elemento de orden con ORDER_ID ' || p_order_id || ' y LINE_ITEM_ID ' || p_line_item_id || ' no existe.');
    END IF;
END;
/

CREATE OR REPLACE PROCEDURE delete_product(
    p_product_id IN NUMBER
) AS
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM PRODUCT_INFORMATION WHERE PRODUCT_ID = p_product_id;
    IF v_count > 0 THEN
        DELETE FROM PRODUCT_INFORMATION WHERE PRODUCT_ID = p_product_id;
    ELSE
        DBMS_OUTPUT.PUT_LINE('El producto con PRODUCT_ID ' || p_product_id || ' no existe.');
    END IF;
END;
/

---Nota en python al llamar la funcion habrá que ponerle que haga commit; porque si no se hace no se reflejan los cambios en las otras bases de datos...