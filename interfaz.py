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

def get_customer(customer_id):
    conexion = conectar_bd()
    if conexion:
        try:
            cursor = conexion.cursor()
            cursor.execute("SELECT * FROM CUSTOMERS WHERE CUSTOMER_ID = :1", [customer_id])
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
            conexion.close()

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
            conexion.close()

def clear_fields():
    entry_customer_id.delete(0, ctk.END)
    entry_first_name.delete(0, ctk.END)
    entry_last_name.delete(0, ctk.END)
    entry_credit_limit.delete(0, ctk.END)
    entry_email.delete(0, ctk.END)
    entry_income_level.delete(0, ctk.END)
    entry_region.delete(0, ctk.END)

def insertar_cliente():
    insert_customer(
        entry_customer_id.get(),
        entry_first_name.get(),
        entry_last_name.get(),
        entry_credit_limit.get(),
        entry_email.get(),
        entry_income_level.get(),
        entry_region.get()
    )

def actualizar_cliente():
    update_customer(
        entry_customer_id.get(),
        entry_first_name.get(),
        entry_last_name.get(),
        entry_credit_limit.get(),
        entry_email.get(),
        entry_income_level.get(),
        entry_region.get()
    )

def eliminar_cliente():
    delete_customer(entry_customer_id.get())

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