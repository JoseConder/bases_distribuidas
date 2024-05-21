import customtkinter as ctk
import oracledb
import tkinter.messagebox as msg

conexion = None

def conectar_bd():
    global conexion
    try:
        if not conexion:
            conexion = oracledb.connect(
                user="system",
                password="123",
                dsn='localhost:1522/xe'
            )
        return conexion
    except oracledb.DatabaseError as e:
        error, = e.args
        msg.showerror("Error de Conexión", f"Error: {error.code}\nMensaje: {error.message}")
        return None

def insert_product(product_id, product_name, product_description, category_id, weight_class, warranty_period, supplier_id, product_status, list_price, min_price, catalog_url):
    conexion = conectar_bd()
    if conexion:
        try:
            cursor = conexion.cursor()
            cursor.callproc("insert_product", [product_id, product_name, product_description, category_id, weight_class, warranty_period, supplier_id, product_status, list_price, min_price, catalog_url])
            conexion.commit()
            msg.showinfo("Éxito", "Producto insertado correctamente.")
            clear_fields()
        except oracledb.DatabaseError as e:
            error, = e.args
            msg.showerror("Error de Base de Datos", f"Error: {error.code}\nMensaje: {error.message}")
            conexion.rollback()

def get_product(product_id):
    global conexion
    conexion = conectar_bd()
    if conexion:
        try:
            cursor = conexion.cursor()
            cursor.callproc("DBMS_MVIEW.REFRESH", ['MV_PRODUCTS_GLOBAL', 'C'])
            cursor.execute("SELECT PRODUCT_ID, PRODUCT_NAME, PRODUCT_DESCRIPTION, CATEGORY_ID, WEIGHT_CLASS, TO_CHAR(WARRANTY_PERIOD, 'YYYY-MM') AS WARRANTY_PERIOD, SUPPLIER_ID, PRODUCT_STATUS, LIST_PRICE, MIN_PRICE, CATALOG_URL FROM mv_products_global WHERE PRODUCT_ID = :1", [product_id])
            result = cursor.fetchone()
            return result
        except oracledb.DatabaseError as e:
            error, = e.args
            msg.showerror("Error de Base de Datos", f"Error: {error.code}\nMensaje: {error.message}")
        finally:
            cursor.close()

def update_product(product_id, product_name, product_description, category_id, weight_class, warranty_period, supplier_id, product_status, list_price, min_price, catalog_url):
    conexion = conectar_bd()
    if conexion:
        try:
            cursor = conexion.cursor()
            cursor.callproc("update_product", [product_id, product_name, product_description, category_id, weight_class, warranty_period, supplier_id, product_status, list_price, min_price, catalog_url])
            conexion.commit()
            msg.showinfo("Éxito", "Producto actualizado correctamente.")
            clear_fields()
        except oracledb.DatabaseError as e:
            error, = e.args
            msg.showerror("Error de Base de Datos", f"Error: {error.code}\nMensaje: {error.message}")
            conexion.rollback()

def delete_product(product_id):
    conexion = conectar_bd()
    if conexion:
        try:
            cursor = conexion.cursor()
            cursor.callproc("delete_product", [product_id])
            conexion.commit()
            msg.showinfo("Éxito", "Producto eliminado correctamente.")
            clear_fields()
        except oracledb.IntegrityError as e:
            error, = e.args
            if error.code == 2292:
                msg.showerror("Error de Integridad", "No se puede eliminar el producto porque está referenciado por otros registros.")
            else:
                msg.showerror("Error de Base de Datos", f"Error: {error.code}\nMensaje: {error.message}")
            conexion.rollback()
        except oracledb.DatabaseError as e:
            error, = e.args
            msg.showerror("Error de Base de Datos", f"Error: {error.code}\nMensaje: {error.message}")
            conexion.rollback()

def clear_fields():
    entry_product_id.delete(0, ctk.END)
    entry_product_name.delete(0, ctk.END)
    entry_product_description.delete(0, ctk.END)
    entry_category_id.delete(0, ctk.END)
    entry_weight_class.delete(0, ctk.END)
    entry_warranty_period.delete(0, ctk.END)
    entry_supplier_id.delete(0, ctk.END)
    entry_product_status.delete(0, ctk.END)
    entry_list_price.delete(0, ctk.END)
    entry_min_price.delete(0, ctk.END)
    entry_catalog_url.delete(0, ctk.END)

def insertar_producto():
    try:
        product_id = int(entry_product_id.get())
        product_name = entry_product_name.get()
        product_description = entry_product_description.get()
        category_id = int(entry_category_id.get())
        weight_class = int(entry_weight_class.get())
        warranty_period_str = entry_warranty_period.get()
        supplier_id = int(entry_supplier_id.get())
        product_status = entry_product_status.get()
        list_price = float(entry_list_price.get())
        min_price = float(entry_min_price.get())
        catalog_url = entry_catalog_url.get()

        try:
            warranty_period = f'{warranty_period_str}'  # Convertir a intervalo apropiado si es necesario
        except ValueError:
            msg.showerror("Error de Validación", "El periodo de garantía debe estar en el formato adecuado.")
            return

        insert_product(product_id, product_name, product_description, category_id, weight_class, warranty_period, supplier_id, product_status, list_price, min_price, catalog_url)
    except ValueError as ve:
        msg.showerror("Error de Validación", f"Error en el tipo de datos: {ve}")

def actualizar_producto():
    try:
        product_id = int(entry_product_id.get())
        product_name = entry_product_name.get()
        product_description = entry_product_description.get()
        category_id = int(entry_category_id.get())
        weight_class = int(entry_weight_class.get())
        warranty_period_str = entry_warranty_period.get()
        supplier_id = int(entry_supplier_id.get())
        product_status = entry_product_status.get()
        list_price = float(entry_list_price.get())
        min_price = float(entry_min_price.get())
        catalog_url = entry_catalog_url.get()

        try:
            warranty_period = f'{warranty_period_str}'  # Convertir a intervalo apropiado si es necesario
        except ValueError:
            msg.showerror("Error de Validación", "El periodo de garantía debe estar en el formato adecuado.")
            return

        update_product(product_id, product_name, product_description, category_id, weight_class, warranty_period, supplier_id, product_status, list_price, min_price, catalog_url)
    except ValueError as ve:
        msg.showerror("Error de Validación", f"Error en el tipo de datos: {ve}")

def eliminar_producto():
    if msg.askyesno("Confirmar Eliminación", "¿Estás seguro de que deseas eliminar este producto?"):
        delete_product(entry_product_id.get())

def buscar_producto():
    product_id = entry_product_id.get()
    product = get_product(product_id)
    if product:
        entry_product_name.delete(0, ctk.END)
        entry_product_name.insert(0, product[1] if product[1] else "")
        entry_product_description.delete(0, ctk.END)
        entry_product_description.insert(0, product[2] if product[2] else "")
        entry_category_id.delete(0, ctk.END)
        entry_category_id.insert(0, product[3] if product[3] else "")
        entry_weight_class.delete(0, ctk.END)
        entry_weight_class.insert(0, product[4] if product[4] else "")
        entry_warranty_period.delete(0, ctk.END)
        entry_warranty_period.insert(0, product[5] if product[5] else "")
        entry_supplier_id.delete(0, ctk.END)
        entry_supplier_id.insert(0, product[6] if product[6] else "")
        entry_product_status.delete(0, ctk.END)
        entry_product_status.insert(0, product[7] if product[7] else "")
        entry_list_price.delete(0, ctk.END)
        entry_list_price.insert(0, product[8] if product[8] else "")
        entry_min_price.delete(0, ctk.END)
        entry_min_price.insert(0, product[9] if product[9] else "")
        entry_catalog_url.delete(0, ctk.END)
        entry_catalog_url.insert(0, product[10] if product[10] else "")
    else:
        msg.showinfo("Resultado", "No se encontró el producto.")

def create_app(parent):

    global entry_product_id, entry_product_name, entry_product_description, entry_category_id, entry_weight_class, entry_warranty_period, entry_supplier_id, entry_product_status, entry_list_price, entry_min_price, entry_catalog_url


    frame_form = ctk.CTkFrame(parent)
    frame_form.pack(padx=10, pady=10, fill="both", expand=True)

    label_product_id = ctk.CTkLabel(frame_form, text="Product ID:")
    label_product_id.grid(row=0, column=0, padx=10, pady=5, sticky="e")
    entry_product_id = ctk.CTkEntry(frame_form)
    entry_product_id.grid(row=0, column=1, padx=10, pady=5)

    label_product_name = ctk.CTkLabel(frame_form, text="Product Name:")
    label_product_name.grid(row=1, column=0, padx=10, pady=5, sticky="e")
    entry_product_name = ctk.CTkEntry(frame_form)
    entry_product_name.grid(row=1, column=1, padx=10, pady=5)

    label_product_description = ctk.CTkLabel(frame_form, text="Product Description:")
    label_product_description.grid(row=2, column=0, padx=10, pady=5, sticky="e")
    entry_product_description = ctk.CTkEntry(frame_form)
    entry_product_description.grid(row=2, column=1, padx=10, pady=5)

    label_category_id = ctk.CTkLabel(frame_form, text="Category ID:")
    label_category_id.grid(row=3, column=0, padx=10, pady=5, sticky="e")
    entry_category_id = ctk.CTkEntry(frame_form)
    entry_category_id.grid(row=3, column=1, padx=10, pady=5)

    label_weight_class = ctk.CTkLabel(frame_form, text="Weight Class:")
    label_weight_class.grid(row=4, column=0, padx=10, pady=5, sticky="e")
    entry_weight_class = ctk.CTkEntry(frame_form)
    entry_weight_class.grid(row=4, column=1, padx=10, pady=5)

    label_warranty_period = ctk.CTkLabel(frame_form, text="Warranty Period (Years-Months):")
    label_warranty_period.grid(row=5, column=0, padx=10, pady=5, sticky="e")
    entry_warranty_period = ctk.CTkEntry(frame_form)
    entry_warranty_period.grid(row=5, column=1, padx=10, pady=5)

    label_supplier_id = ctk.CTkLabel(frame_form, text="Supplier ID:")
    label_supplier_id.grid(row=6, column=0, padx=10, pady=5, sticky="e")
    entry_supplier_id = ctk.CTkEntry(frame_form)
    entry_supplier_id.grid(row=6, column=1, padx=10, pady=5)

    label_product_status = ctk.CTkLabel(frame_form, text="Product Status:")
    label_product_status.grid(row=7, column=0, padx=10, pady=5, sticky="e")
    entry_product_status = ctk.CTkEntry(frame_form)
    entry_product_status.grid(row=7, column=1, padx=10, pady=5)

    label_list_price = ctk.CTkLabel(frame_form, text="List Price:")
    label_list_price.grid(row=8, column=0, padx=10, pady=5, sticky="e")
    entry_list_price = ctk.CTkEntry(frame_form)
    entry_list_price.grid(row=8, column=1, padx=10, pady=5)

    label_min_price = ctk.CTkLabel(frame_form, text="Min Price:")
    label_min_price.grid(row=9, column=0, padx=10, pady=5, sticky="e")
    entry_min_price = ctk.CTkEntry(frame_form)
    entry_min_price.grid(row=9, column=1, padx=10, pady=5)

    label_catalog_url = ctk.CTkLabel(frame_form, text="Catalog URL:")
    label_catalog_url.grid(row=10, column=0, padx=10, pady=5, sticky="e")
    entry_catalog_url = ctk.CTkEntry(frame_form)
    entry_catalog_url.grid(row=10, column=1, padx=10, pady=5)

    button_insertar = ctk.CTkButton(frame_form, text="Insertar Producto", command=insertar_producto)
    button_insertar.grid(row=11, column=0, padx=10, pady=10, sticky="ew")

    button_actualizar = ctk.CTkButton(frame_form, text="Actualizar Producto", command=actualizar_producto)
    button_actualizar.grid(row=11, column=1, padx=10, pady=10, sticky="ew")

    button_eliminar = ctk.CTkButton(frame_form, text="Eliminar Producto", command=eliminar_producto)
    button_eliminar.grid(row=12, column=0, padx=10, pady=10, sticky="ew")

    button_buscar = ctk.CTkButton(frame_form, text="Buscar Producto", command=buscar_producto)
    button_buscar.grid(row=12, column=1, padx=10, pady=10, sticky="ew")

    return frame_form
