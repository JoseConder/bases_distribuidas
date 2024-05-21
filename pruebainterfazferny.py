import customtkinter as ctk
import oracledb
import tkinter.messagebox as msg

conexion = None
orders_window = None  
products_window = None 

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

# Funciones relacionadas con la gestión de clientes

def insert_customer(customer_id, first_name, last_name, credit_limit, email, income_level, region):
    conexion = conectar_bd()
    if conexion:
        try:
            cursor = conexion.cursor()
            cursor.callproc("insert_customer", [customer_id, first_name, last_name, credit_limit, email, income_level, region])
            conexion.commit()
            msg.showinfo("Éxito", "Cliente insertado correctamente.")
            clear_fields()
        except oracledb.DatabaseError as e:
            error, = e.args
            msg.showerror("Error de Base de Datos", f"Error: {error.code}\nMensaje: {error.message}")
            conexion.rollback()
        finally:
            cursor.close()

def get_customer(customer_id):
    global conexion
    conexion = conectar_bd()
    if conexion:
        try:
            cursor = conexion.cursor()
            cursor.callproc("DBMS_MVIEW.REFRESH", ['MV_CUSTOMERS_GLOBAL', 'C'])
            cursor.execute("SELECT * FROM MV_CUSTOMERS_GLOBAL WHERE CUSTOMER_ID = :1", [customer_id])
            result = cursor.fetchone()
            return result
        except oracledb.DatabaseError as e:
            error, = e.args
            msg.showerror("Error de Base de Datos", f"Error: {error.code}\nMensaje: {error.message}")
        finally:
            cursor.close()

def update_customer(customer_id, first_name, last_name, credit_limit, email, income_level, region):
    conexion = conectar_bd()
    if conexion:
        try:
            cursor = conexion.cursor()
            cursor.callproc("update_customer", [customer_id, first_name, last_name, credit_limit, email, income_level, region])
            conexion.commit()
            msg.showinfo("Éxito", "Cliente actualizado correctamente.")
            clear_fields()  # Limpiar los campos después de la actualización
        except oracledb.DatabaseError as e:
            error, = e.args
            msg.showerror("Error de Base de Datos", f"Error: {error.code}\nMensaje: {error.message}")
        finally:
            cursor.close()

def delete_customer(customer_id):
    conexion = conectar_bd()
    if conexion:
        try:
            cursor = conexion.cursor()
            cursor.callproc("delete_customer", [customer_id])
            conexion.commit()
            msg.showinfo("Éxito", "Cliente eliminado correctamente.")
            clear_fields()  # Limpiar los campos después de la eliminación
        except oracledb.DatabaseError as e:
            error, = e.args
            msg.showerror("Error de Base de Datos", f"Error: {error.code}\nMensaje: {error.message}")
        finally:
            cursor.close()

# Función para limpiar los campos
def clear_fields():
    entry_customer_id.delete(0, ctk.END)
    entry_first_name.delete(0, ctk.END)
    entry_last_name.delete(0, ctk.END)
    entry_credit_limit.delete(0, ctk.END)
    entry_email.delete(0, ctk.END)
    entry_income_level.delete(0, ctk.END)
    entry_region.delete(0, ctk.END)

# Función para insertar un cliente
def insertar_cliente():
    region_valida = entry_region.get().upper() in ['A', 'B', 'C', 'D']
    if not region_valida:
        msg.showerror("Error", "La región debe ser A, B, C o D.")
        return
    insert_customer(
        entry_customer_id.get(),
        entry_first_name.get(),
        entry_last_name.get(),
        entry_credit_limit.get(),
        entry_email.get(),
        entry_income_level.get(),
        entry_region.get().upper()
    )

# Función para actualizar un cliente
def actualizar_cliente():
    region_valida = entry_region.get().upper() in ['A', 'B', 'C', 'D']
    if not region_valida:
        msg.showerror("Error", "La región debe ser A, B, C o D.")
        return
    update_customer(
        entry_customer_id.get(),
        entry_first_name.get(),
        entry_last_name.get(),
        entry_credit_limit.get(),
        entry_email.get(),
        entry_income_level.get(),
        entry_region.get().upper()
    )

# Función para eliminar un cliente
def eliminar_cliente():
    delete_customer(entry_customer_id.get())

# Función para buscar un cliente
def buscar_cliente():
    customer_id = entry_customer_id.get()
    customer = get_customer(customer_id)
    if customer:
        entry_first_name.delete(0, ctk.END)  # Borra el contenido actual del campo
        entry_first_name.insert(0, customer[1])
        entry_last_name.delete(0, ctk.END)
        entry_last_name.insert(0, customer[2])
        entry_credit_limit.delete(0, ctk.END)
        entry_credit_limit.insert(0, customer[3])
        entry_email.delete(0, ctk.END)
        entry_email.insert(0, customer[4])
        entry_income_level.delete(0, ctk.END)
        entry_income_level.insert(0, customer[5])
        entry_region.delete(0, ctk.END)
        entry_region.insert(0, customer[6])
    else:
        msg.showinfo("Resultado", "No se encontró el cliente.")

def show_customers():

    global entry_customer_id, entry_first_name, entry_last_name, entry_credit_limit, entry_email, entry_income_level, entry_region

    customers_window = ctk.CTk()
    customers_window.title("Clientes")
    
    app = ctk.CTk()
    app.title("CRUD Clientes")

    label_customer_id = ctk.CTkLabel(app, text="Customer ID:")
    label_customer_id.pack(padx=10, pady=5)
    entry_customer_id = ctk.CTkEntry(app)
    entry_customer_id.pack(padx=10, pady=5)

    label_first_name = ctk.CTkLabel(app, text="First Name:")
    label_first_name.pack(padx=10, pady=5)
    entry_first_name = ctk.CTkEntry(app)
    entry_first_name.pack(padx=10, pady=5)

    label_last_name = ctk.CTkLabel(app, text="Last Name:")
    label_last_name.pack(padx=10, pady=5)
    entry_last_name = ctk.CTkEntry(app)
    entry_last_name.pack(padx=10, pady=5)

    label_credit_limit = ctk.CTkLabel(app, text="Credit Limit:")
    label_credit_limit.pack(padx=10, pady=5)
    entry_credit_limit = ctk.CTkEntry(app)
    entry_credit_limit.pack(padx=10, pady=5)

    label_email = ctk.CTkLabel(app, text="Email:")
    label_email.pack(padx=10, pady=5)
    entry_email = ctk.CTkEntry(app)
    entry_email.pack(padx=10, pady=5)

    label_income_level = ctk.CTkLabel(app, text="Income Level:")
    label_income_level.pack(padx=10, pady=5)
    entry_income_level = ctk.CTkEntry(app)
    entry_income_level.pack(padx=10, pady=5)

    label_region = ctk.CTkLabel(app, text="Region:")
    label_region.pack(padx=10, pady=5)
    entry_region = ctk.CTkEntry(app)
    entry_region.pack(padx=10, pady=5)

    button_insertar = ctk.CTkButton(app, text="Insertar Cliente", command=insertar_cliente)
    button_insertar.pack(padx=10, pady=10)

    button_actualizar = ctk.CTkButton(app, text="Actualizar Cliente", command=actualizar_cliente)
    button_actualizar.pack(padx=10, pady=10)

    button_eliminar = ctk.CTkButton(app, text="Eliminar Cliente", command=eliminar_cliente)
    button_eliminar.pack(padx=10, pady=10)

    button_buscar = ctk.CTkButton(app, text="Buscar Cliente", command=buscar_cliente)
    button_buscar.pack(padx=10, pady=10)

    app.mainloop()

def show_orders():
    global orders_window
    if orders_window is None or not orders_window.winfo_exists():
        orders_window = ctk.CTk()
        orders_window.title("Órdenes")
        orders_window.geometry("800x600")
        import orders
        orders.create_app(orders_window)
        orders_window.protocol("WM_DELETE_WINDOW", lambda: close_window('orders'))
        orders_window.mainloop()

def show_product_info():
    global products_window
    if products_window is None or not products_window.winfo_exists():
        products_window = ctk.CTk()
        products_window.title("Información de Productos")
        products_window.geometry("800x600")
        import products
        products.create_app(products_window)
        products_window.protocol("WM_DELETE_WINDOW", lambda: close_window('products'))
        products_window.mainloop()

def close_window(window_name):
    global orders_window, products_window
    if window_name == 'orders':
        orders_window.destroy()
        orders_window = None
    elif window_name == 'products':
        products_window.destroy()
        products_window = None

def show_customer_table():
    try:
        conexion = conectar_bd()
        if conexion:
            cursor = conexion.cursor()
            cursor.execute("SELECT * FROM MV_CUSTOMERS_GLOBAL")
            rows = cursor.fetchall()
            for row in rows:
                print(row) 
    except oracledb.DatabaseError as e:
        error, = e.args
        msg.showerror("Error de Base de Datos", f"Error: {error.code}\nMensaje: {error.message}")
    finally:
        if conexion:
            cursor.close()

def show_order_table():
    try:
        conexion = conectar_bd()
        if conexion:
            cursor = conexion.cursor()
            cursor.execute("SELECT * FROM MV_ORDERS_GLOBAL")
            rows = cursor.fetchall()
            for row in rows:
                print(row)  # Puedes adaptar este bucle para mostrar los datos en la interfaz gráfica
    except oracledb.DatabaseError as e:
        error, = e.args
        msg.showerror("Error de Base de Datos", f"Error: {error.code}\nMensaje: {error.message}")
    finally:
        if conexion:
            cursor.close()


def show_product_table():
    try:
        conexion = conectar_bd()
        if conexion:
            cursor = conexion.cursor()
            cursor.execute("SELECT * FROM MV_PRODUCTS_GLOBAL")
            rows = cursor.fetchall()
            for row in rows:
                print(row)  # Puedes adaptar este bucle para mostrar los datos en la interfaz gráfica
    except oracledb.DatabaseError as e:
        error, = e.args
        msg.showerror("Error de Base de Datos", f"Error: {error.code}\nMensaje: {error.message}")
    finally:
        if conexion:
            cursor.close()

# Función para manejar el evento de cierre de la ventana principal
def on_closing():
    window.destroy()  # Destruir la ventana principal
    import sys
    sys.exit()  # Salir del programa

# Crear la ventana principal
window = ctk.CTk()
window.title("Interfaz Principal") 
window.geometry("400x150")  # Definir el tamaño de la ventana
window.protocol("WM_DELETE_WINDOW", on_closing)

# Crear un frame para contener los botones
button_frame = ctk.CTkFrame(window)
button_frame.pack(expand=True, fill='both')  # Hacer que el frame ocupe todo el espacio

# Crear botones
button_customers = ctk.CTkButton(button_frame, text="Clientes", command=show_customers)
button_orders = ctk.CTkButton(button_frame, text="Órdenes", command=show_orders)
button_product_info = ctk.CTkButton(button_frame, text="Información de Productos", command=show_product_info)

# Nuevos botones para mostrar tablas
button_customer_table = ctk.CTkButton(button_frame, text="Mostrar Tabla de Clientes", command=show_customer_table)
button_order_table = ctk.CTkButton(button_frame, text="Mostrar Tabla de Órdenes", command=show_order_table)
button_product_table = ctk.CTkButton(button_frame, text="Mostrar Tabla de Productos", command=show_product_table)

# Centrar los botones en el frame
button_customers.pack(pady=10, padx=10, fill='x')
button_orders.pack(pady=10, padx=10, fill='x')
button_product_info.pack(pady=10, padx=10, fill='x')

# Nuevos botones
button_customer_table.pack(pady=10, padx=10, fill='x')
button_order_table.pack(pady=10, padx=10, fill='x')
button_product_table.pack(pady=10, padx=10, fill='x')

# Mostrar la ventana principal
window.mainloop()
