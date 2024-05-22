
---- Cambiado para funcionar con dos bases de datos, los cambios principales son en los triggers, ahora los triggers son antes de insertar en lugar de despues
---- para customers funciona igual pero para orders y order_items usa las vistas materializadas para tener el esquema completo de customers y checar la region correspondiente.


---Crear los database links
-------NOTA: cambiar el host por la IP de la instancia, es importante prender los contenedores en orden, porque el primero que enciende es el primero que tiene la primera ip
create database link link_b1_a_b2 connect to system identified by "123" using '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=172.17.0.3)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=XE)))';
----create database link link_b2_a_b1 connect to system identified by "123" using '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=172.17.0.2)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=XE)))';

--- Creacion de sinonimos

create SYNONYM customersb2 for customers@link_b1_a_b2;



create SYNONYM ordersb2 for orders@link_b1_a_b2;



create SYNONYM productsb2 for product_information@link_b1_a_b2;



create SYNONYM itemsb2 for order_items@link_b1_a_b2;


--- Vista materializadas


--- customers global

CREATE MATERIALIZED VIEW mv_customers_global
REFRESH ON DEMAND
AS
SELECT * FROM customers
UNION ALL
SELECT * FROM customersb2;

--- products global
CREATE MATERIALIZED VIEW mv_products_global
REFRESH ON DEMAND
AS
SELECT * FROM product_information
UNION
SELECT * FROM productsb2;

--- orders global
CREATE MATERIALIZED VIEW mv_orders_global
REFRESH ON DEMAND
AS
SELECT * FROM orders
UNION
SELECT * FROM ordersb2;

CREATE MATERIALIZED VIEW mv_order_items_global
REFRESH ON DEMAND
AS
SELECT * FROM order_items
UNION
SELECT * FROM itemsb2;

---CRUDS

CREATE OR REPLACE PROCEDURE insert_customer(
    p_customer_id IN NUMBER,
    p_first_name IN VARCHAR2,
    p_last_name IN VARCHAR2,
    p_credit_limit IN NUMBER,
    p_email IN VARCHAR2,
    p_income_level IN VARCHAR2,
    p_region IN VARCHAR2
) AS
    v_customer_count NUMBER;
BEGIN
    DBMS_MVIEW.REFRESH('MV_CUSTOMERS_GLOBAL', 'C');

    SELECT COUNT(*)
    INTO v_customer_count
    FROM mv_customers_global
    WHERE CUSTOMER_ID = p_customer_id;

    IF v_customer_count = 0 THEN
        IF p_region IN ('A', 'B') THEN
            INSERT INTO CUSTOMERS (CUSTOMER_ID, CUST_FIRST_NAME, CUST_LAST_NAME, CREDIT_LIMIT, CUST_EMAIL, INCOME_LEVEL, REGION)
            VALUES (p_customer_id, p_first_name, p_last_name, p_credit_limit, p_email, p_income_level, p_region);
        ELSE
            INSERT INTO customersb2 (CUSTOMER_ID, CUST_FIRST_NAME, CUST_LAST_NAME, CREDIT_LIMIT, CUST_EMAIL, INCOME_LEVEL, REGION)
            VALUES (p_customer_id, p_first_name, p_last_name, p_credit_limit, p_email, p_income_level, p_region);
        END IF;
        COMMIT;
    ELSE
        RAISE_APPLICATION_ERROR(-20001, 'El cliente ya existe');
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END;
/

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
    DBMS_MVIEW.REFRESH('MV_CUSTOMERS_GLOBAL', 'C');

    SELECT COUNT(*) INTO v_count FROM mv_customers_global WHERE CUSTOMER_ID = p_customer_id;
    IF v_count > 0 THEN
        IF p_region IN ('A', 'B') THEN
            UPDATE CUSTOMERS
            SET CUST_FIRST_NAME = p_first_name,
                CUST_LAST_NAME = p_last_name,
                CREDIT_LIMIT = p_credit_limit,
                CUST_EMAIL = p_email,
                INCOME_LEVEL = p_income_level,
                REGION = p_region
            WHERE CUSTOMER_ID = p_customer_id;
        ELSE
            UPDATE customersb2
            SET CUST_FIRST_NAME = p_first_name,
                CUST_LAST_NAME = p_last_name,
                CREDIT_LIMIT = p_credit_limit,
                CUST_EMAIL = p_email,
                INCOME_LEVEL = p_income_level,
                REGION = p_region
            WHERE CUSTOMER_ID = p_customer_id;
        END IF;
        COMMIT;
    ELSE
        DBMS_OUTPUT.PUT_LINE('El cliente con ID ' || p_customer_id || ' no existe.');
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END;
/

CREATE OR REPLACE PROCEDURE delete_customer(
    p_customer_id IN NUMBER
) AS
    v_customer_count NUMBER;
    v_region CHAR(1);
BEGIN
    -- Refrescar la vista materializada global
    DBMS_MVIEW.REFRESH('MV_CUSTOMERS_GLOBAL', 'C');

    -- Verificar la existencia del cliente
    BEGIN
        SELECT COUNT(*)
        INTO v_customer_count
        FROM MV_CUSTOMERS_GLOBAL
        WHERE CUSTOMER_ID = p_customer_id;
        
        IF v_customer_count > 0 THEN
            -- Obtener la región del cliente
            SELECT REGION
            INTO v_region
            FROM MV_CUSTOMERS_GLOBAL
            WHERE CUSTOMER_ID = p_customer_id;

            -- Eliminar el cliente de la tabla correspondiente según la región
            IF v_region IN ('A', 'B') THEN
                DELETE FROM CUSTOMERS WHERE CUSTOMER_ID = p_customer_id;
            ELSE
                DELETE FROM customersb2 WHERE CUSTOMER_ID = p_customer_id;
            END IF;
            COMMIT;
        ELSE
            DBMS_OUTPUT.PUT_LINE('El cliente con ID ' || p_customer_id || ' no existe.');
        END IF;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            DBMS_OUTPUT.PUT_LINE('El cliente con ID ' || p_customer_id || ' no existe.');
        WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
    END;
END;
/




---ORDERS

CREATE OR REPLACE PROCEDURE insert_order(
    p_order_id IN NUMBER,
    p_order_date IN TIMESTAMP,
    p_order_mode IN VARCHAR2,
    p_customer_id IN NUMBER,
    p_order_status IN NUMBER,
    p_order_total IN NUMBER,
    p_sales_rep_id IN NUMBER,
    p_promotion_id IN NUMBER
) IS
    v_region CHAR(1);
    v_order_count NUMBER;
BEGIN
    DBMS_MVIEW.REFRESH('MV_CUSTOMERS_GLOBAL', 'C');
    DBMS_MVIEW.REFRESH('MV_ORDERS_GLOBAL', 'C');

    SELECT REGION INTO v_region
    FROM MV_CUSTOMERS_GLOBAL
    WHERE CUSTOMER_ID = p_customer_id;

    SELECT COUNT(*)
    INTO v_order_count
    FROM mv_orders_global
    WHERE ORDER_ID = p_order_id;

    IF v_order_count = 0 THEN
        IF v_region IN ('A', 'B') THEN
            INSERT INTO ORDERS (ORDER_ID, ORDER_DATE, ORDER_MODE, CUSTOMER_ID, ORDER_STATUS, ORDER_TOTAL, SALES_REP_ID, PROMOTION_ID)
            VALUES (p_order_id, p_order_date, p_order_mode, p_customer_id, p_order_status, p_order_total, p_sales_rep_id, p_promotion_id);
        ELSE
            INSERT INTO ordersb2 (ORDER_ID, ORDER_DATE, ORDER_MODE, CUSTOMER_ID, ORDER_STATUS, ORDER_TOTAL, SALES_REP_ID, PROMOTION_ID)
            VALUES (p_order_id, p_order_date, p_order_mode, p_customer_id, p_order_status, p_order_total, p_sales_rep_id, p_promotion_id);
        END IF;
        COMMIT;
    ELSE
        DBMS_OUTPUT.PUT_LINE('La orden con ORDER_ID ' || p_order_id || ' ya existe.');
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
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
    v_region CHAR(1);
    v_order_count NUMBER;
BEGIN
    DBMS_MVIEW.REFRESH('MV_ORDERS_GLOBAL', 'C');
    DBMS_MVIEW.REFRESH('MV_CUSTOMERS_GLOBAL', 'C');

    SELECT COUNT(*)
    INTO v_order_count
    FROM mv_orders_global
    WHERE ORDER_ID = p_order_id;

    IF v_order_count > 0 THEN
        SELECT REGION INTO v_region
        FROM MV_CUSTOMERS_GLOBAL
        WHERE CUSTOMER_ID = p_customer_id;

        IF v_region IN ('A', 'B') THEN
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
            UPDATE ordersb2
            SET ORDER_DATE = p_order_date,
                ORDER_MODE = p_order_mode,
                CUSTOMER_ID = p_customer_id,
                ORDER_STATUS = p_order_status,
                ORDER_TOTAL = p_order_total,
                SALES_REP_ID = p_sales_rep_id,
                PROMOTION_ID = p_promotion_id
            WHERE ORDER_ID = p_order_id;
        END IF;
        COMMIT;
    ELSE
        DBMS_OUTPUT.PUT_LINE('La orden con ORDER_ID ' || p_order_id || ' no existe.');
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
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
    v_product_count NUMBER;
BEGIN
    DBMS_MVIEW.REFRESH('MV_PRODUCTS_GLOBAL', 'C');

    SELECT COUNT(*)
    INTO v_product_count
    FROM mv_products_global
    WHERE PRODUCT_ID = p_product_id;

    IF v_product_count = 0 THEN
        INSERT INTO PRODUCT_INFORMATION (PRODUCT_ID, PRODUCT_NAME, PRODUCT_DESCRIPTION, CATEGORY_ID, WEIGHT_CLASS, WARRANTY_PERIOD, SUPPLIER_ID, PRODUCT_STATUS, LIST_PRICE, MIN_PRICE, CATALOG_URL)
        VALUES (p_product_id, p_product_name, p_product_description, p_category_id, p_weight_class, p_warranty_period, p_supplier_id, p_product_status, p_list_price, p_min_price, p_catalog_url);

        INSERT INTO productsb2 (PRODUCT_ID, PRODUCT_NAME, PRODUCT_DESCRIPTION, CATEGORY_ID, WEIGHT_CLASS, WARRANTY_PERIOD, SUPPLIER_ID, PRODUCT_STATUS, LIST_PRICE, MIN_PRICE, CATALOG_URL)
        VALUES (p_product_id, p_product_name, p_product_description, p_category_id, p_weight_class, p_warranty_period, p_supplier_id, p_product_status, p_list_price, p_min_price, p_catalog_url);

        COMMIT;
    ELSE
        DBMS_OUTPUT.PUT_LINE('El producto con PRODUCT_ID ' || p_product_id || ' ya existe.');
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
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
    v_product_count NUMBER;
BEGIN
    DBMS_MVIEW.REFRESH('MV_PRODUCTS_GLOBAL', 'C');

    SELECT COUNT(*)
    INTO v_product_count
    FROM mv_products_global
    WHERE PRODUCT_ID = p_product_id;

    IF v_product_count > 0 THEN
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

        UPDATE productsb2
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

        COMMIT;
    ELSE
        DBMS_OUTPUT.PUT_LINE('El producto con PRODUCT_ID ' || p_product_id || ' no existe.');
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END;
/

CREATE OR REPLACE PROCEDURE delete_order(
    p_order_id IN NUMBER
) AS
    v_order_count NUMBER;
    v_region CHAR(1);
BEGIN
    DBMS_MVIEW.REFRESH('MV_ORDERS_GLOBAL', 'C');
    DBMS_MVIEW.REFRESH('MV_CUSTOMERS_GLOBAL', 'C');

    -- Verificar la existencia de la orden
    BEGIN
        SELECT COUNT(*)
        INTO v_order_count
        FROM MV_ORDERS_GLOBAL
        WHERE ORDER_ID = p_order_id;

        IF v_order_count > 0 THEN
            -- Obtener la región del cliente asociado a la orden
            SELECT c.REGION
            INTO v_region
            FROM MV_ORDERS_GLOBAL o
            JOIN MV_CUSTOMERS_GLOBAL c ON o.CUSTOMER_ID = c.CUSTOMER_ID
            WHERE o.ORDER_ID = p_order_id;

            -- Eliminar la orden de la tabla correspondiente según la región
            IF v_region IN ('A', 'B') THEN
                DELETE FROM ORDERS WHERE ORDER_ID = p_order_id;
            ELSE
                DELETE FROM ordersb2 WHERE ORDER_ID = p_order_id;
            END IF;
            COMMIT;
        ELSE
            DBMS_OUTPUT.PUT_LINE('La orden con ORDER_ID ' || p_order_id || ' no existe.');
        END IF;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            DBMS_OUTPUT.PUT_LINE('La orden con ORDER_ID ' || p_order_id || ' no existe.');
        WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
    END;
END;
/


---ORDER_ITEMS

CREATE OR REPLACE PROCEDURE insert_order_item (
    p_order_id IN NUMBER,
    p_line_item_id IN NUMBER,
    p_product_id IN NUMBER,
    p_unit_price IN NUMBER,
    p_quantity IN NUMBER
) IS
    v_region CHAR(1);
    v_order_item_count NUMBER;
BEGIN
    DBMS_MVIEW.REFRESH('MV_ORDERS_GLOBAL', 'C');
    DBMS_MVIEW.REFRESH('mv_order_items_global', 'C');

    SELECT REGION INTO v_region
    FROM MV_CUSTOMERS_GLOBAL c
    JOIN MV_ORDERS_GLOBAL o ON c.CUSTOMER_ID = o.CUSTOMER_ID
    WHERE o.ORDER_ID = p_order_id;

    SELECT COUNT(*)
    INTO v_order_item_count
    FROM mv_order_items_global
    WHERE ORDER_ID = p_order_id AND LINE_ITEM_ID = p_line_item_id;

    IF v_order_item_count = 0 THEN
        IF v_region IN ('A', 'B') THEN
            INSERT INTO ORDER_ITEMS (ORDER_ID, LINE_ITEM_ID, PRODUCT_ID, UNIT_PRICE, QUANTITY)
            VALUES (p_order_id, p_line_item_id, p_product_id, p_unit_price, p_quantity);
        ELSE
            INSERT INTO itemsb2 (ORDER_ID, LINE_ITEM_ID, PRODUCT_ID, UNIT_PRICE, QUANTITY)
            VALUES (p_order_id, p_line_item_id, p_product_id, p_unit_price, p_quantity);
        END IF;
        COMMIT;
    ELSE
        DBMS_OUTPUT.PUT_LINE('El elemento de orden con ORDER_ID ' || p_order_id || ' y LINE_ITEM_ID ' || p_line_item_id || ' ya existe.');
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END;
/


CREATE OR REPLACE PROCEDURE update_order_item(
    p_order_id IN NUMBER,
    p_line_item_id IN NUMBER,
    p_product_id IN NUMBER,
    p_unit_price IN NUMBER,
    p_quantity IN NUMBER
) AS
    v_region CHAR(1);
    v_order_item_count NUMBER;
BEGIN
    DBMS_MVIEW.REFRESH('MV_ORDERS_GLOBAL', 'C');
    DBMS_MVIEW.REFRESH('mv_order_items_global', 'C');

    SELECT REGION INTO v_region
    FROM MV_CUSTOMERS_GLOBAL c
    JOIN MV_ORDERS_GLOBAL o ON c.CUSTOMER_ID = o.CUSTOMER_ID
    WHERE o.ORDER_ID = p_order_id;

    SELECT COUNT(*)
    INTO v_order_item_count
    FROM mv_order_items_global
    WHERE ORDER_ID = p_order_id AND LINE_ITEM_ID = p_line_item_id;

    IF v_order_item_count > 0 THEN
        IF v_region IN ('A', 'B') THEN
            UPDATE ORDER_ITEMS
            SET PRODUCT_ID = p_product_id,
                UNIT_PRICE = p_unit_price,
                QUANTITY = p_quantity
            WHERE ORDER_ID = p_order_id AND LINE_ITEM_ID = p_line_item_id;
        ELSE
            UPDATE itemsb2
            SET PRODUCT_ID = p_product_id,
                UNIT_PRICE = p_unit_price,
                QUANTITY = p_quantity
            WHERE ORDER_ID = p_order_id AND LINE_ITEM_ID = p_line_item_id;
        END IF;
        COMMIT;
    ELSE
        DBMS_OUTPUT.PUT_LINE('El elemento de orden con ORDER_ID ' || p_order_id || ' y LINE_ITEM_ID ' || p_line_item_id || ' no existe.');
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END;
/

CREATE OR REPLACE PROCEDURE delete_order_item(
    p_order_id IN NUMBER,
    p_line_item_id IN NUMBER
) AS
    v_region CHAR(1);
    v_order_item_count NUMBER;
BEGIN
    DBMS_MVIEW.REFRESH('MV_ORDERS_GLOBAL', 'C');
    DBMS_MVIEW.REFRESH('mv_order_items_global', 'C');


    SELECT REGION INTO v_region
    FROM MV_CUSTOMERS_GLOBAL c
    JOIN MV_ORDERS_GLOBAL o ON c.CUSTOMER_ID = o.CUSTOMER_ID
    WHERE o.ORDER_ID = p_order_id;

    SELECT COUNT(*)
    INTO v_order_item_count
    FROM mv_order_items_global
    WHERE ORDER_ID = p_order_id AND LINE_ITEM_ID = p_line_item_id;

    IF v_order_item_count > 0 THEN
        IF v_region IN ('A', 'B') THEN
            DELETE FROM ORDER_ITEMS WHERE ORDER_ID = p_order_id AND LINE_ITEM_ID = p_line_item_id;
        ELSE
            DELETE FROM itemsb2 WHERE ORDER_ID = p_order_id AND LINE_ITEM_ID = p_line_item_id;
        END IF;
        COMMIT;
    ELSE
        DBMS_OUTPUT.PUT_LINE('El elemento de orden con ORDER_ID ' || p_order_id || ' y LINE_ITEM_ID ' || p_line_item_id || ' no existe.');
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END;
/

CREATE OR REPLACE PROCEDURE delete_product(
    p_product_id IN NUMBER
) AS
    v_product_count NUMBER;
BEGIN
    DBMS_MVIEW.REFRESH('MV_PRODUCTS_GLOBAL', 'C');

    SELECT COUNT(*)
    INTO v_product_count
    FROM mv_products_global
    WHERE PRODUCT_ID = p_product_id;

    IF v_product_count > 0 THEN
        DELETE FROM PRODUCT_INFORMATION WHERE PRODUCT_ID = p_product_id;
        DELETE FROM productsb2 WHERE PRODUCT_ID = p_product_id;
        COMMIT;
    ELSE
        DBMS_OUTPUT.PUT_LINE('El producto con PRODUCT_ID ' || p_product_id || ' no existe.');
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END;
/