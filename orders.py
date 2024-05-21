import customtkinter as ctk
import oracledb
import tkinter.messagebox as msg
from datetime import datetime

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

def insert_order(order_id, order_date, order_mode, customer_id, order_status, order_total, sales_rep_id, promotion_id):
    conexion = conectar_bd()
    if conexion:
        try:
            cursor = conexion.cursor()
            cursor.callproc("insert_order", [order_id, order_date, order_mode, customer_id, order_status, order_total, sales_rep_id, promotion_id])
            conexion.commit()
            msg.showinfo("Éxito", "Orden insertada correctamente.")
            clear_fields()
        except oracledb.DatabaseError as e:
            error, = e.args
            msg.showerror("Error de Base de Datos", f"Error: {error.code}\nMensaje: {error.message}")
            conexion.rollback()

def get_order(order_id):
    global conexion
    conexion = conectar_bd()
    if conexion:
        try:
            cursor = conexion.cursor()
            cursor.callproc("DBMS_MVIEW.REFRESH", ['MV_ORDERS_GLOBAL', 'C'])
            cursor.execute("SELECT * FROM MV_ORDERS_GLOBAL WHERE ORDER_ID = :1", [order_id])
            result = cursor.fetchone()
            return result
        except oracledb.DatabaseError as e:
            error, = e.args
            msg.showerror("Error de Base de Datos", f"Error: {error.code}\nMensaje: {error.message}")
        finally:
            cursor.close()

def update_order(order_id, order_date, order_mode, customer_id, order_status, order_total, sales_rep_id, promotion_id):
    conexion = conectar_bd()
    if conexion:
        try:
            cursor = conexion.cursor()
            cursor.callproc("update_order", [order_id, order_date, order_mode, customer_id, order_status, order_total, sales_rep_id, promotion_id])
            conexion.commit()
            msg.showinfo("Éxito", "Orden actualizada correctamente.")
            clear_fields()
        except oracledb.DatabaseError as e:
            error, = e.args
            msg.showerror("Error de Base de Datos", f"Error: {error.code}\nMensaje: {error.message}")
            conexion.rollback()

def delete_order(order_id):
    conexion = conectar_bd()
    if conexion:
        try:
            cursor = conexion.cursor()
            cursor.callproc("delete_order", [order_id])
            conexion.commit()
            msg.showinfo("Éxito", "Orden eliminada correctamente.")
            clear_fields()
        except oracledb.IntegrityError as e:
            error, = e.args
            if error.code == 2292:
                msg.showerror("Error de Integridad", "No se puede eliminar la orden porque está referenciada por otros registros.")
            else:
                msg.showerror("Error de Base de Datos", f"Error: {error.code}\nMensaje: {error.message}")
            conexion.rollback()
        except oracledb.DatabaseError as e:
            error, = e.args
            msg.showerror("Error de Base de Datos", f"Error: {error.code}\nMensaje: {error.message}")
            conexion.rollback()

def clear_fields():
    entry_order_id.delete(0, ctk.END)
    entry_order_date.delete(0, ctk.END)
    entry_order_mode.delete(0, ctk.END)
    entry_customer_id.delete(0, ctk.END)
    entry_order_status.delete(0, ctk.END)
    entry_order_total.delete(0, ctk.END)
    entry_sales_rep_id.delete(0, ctk.END)
    entry_promotion_id.delete(0, ctk.END)

def insertar_orden():
        try:
            order_id = int(entry_order_id.get())
            order_date_str = entry_order_date.get()
            order_mode = entry_order_mode.get()
            customer_id = int(entry_customer_id.get())
            order_status = int(entry_order_status.get())
            order_total = float(entry_order_total.get())
            sales_rep_id = int(entry_sales_rep_id.get())
            promotion_id = entry_promotion_id.get()
            try:
                order_date = datetime.strptime(order_date_str, '%Y-%m-%d %H:%M:%S')            
            except ValueError:
                msg.showerror("Error de Validación", "La fecha debe estar en el formato YYYY-MM-DD HH:MM:SS")
                return
            insert_order(order_id, order_date, order_mode, customer_id, order_status, order_total, sales_rep_id, promotion_id)
        except ValueError as ve:
            msg.showerror("Error de Validación", f"Error en el tipo de datos: {ve}")


def actualizar_orden():
    try:
        order_id = int(entry_order_id.get())
        order_date_str = entry_order_date.get()
        order_mode = entry_order_mode.get()
        customer_id = int(entry_customer_id.get())
        order_status = int(entry_order_status.get())
        order_total = float(entry_order_total.get())
        sales_rep_id = int(entry_sales_rep_id.get())
        promotion_id = entry_promotion_id.get()
        try:
            order_date = datetime.strptime(order_date_str, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            msg.showerror("Error de Validación", "La fecha debe estar en el formato YYYY-MM-DD HH:MM:SS")
            return
        update_order(order_id, order_date, order_mode, customer_id, order_status, order_total, sales_rep_id, promotion_id)
    except ValueError as ve:
        msg.showerror("Error de Validación", f"Error en el tipo de datos: {ve}")
    

def eliminar_orden():
    if msg.askyesno("Confirmar Eliminación", "¿Estás seguro de que deseas eliminar esta orden?"):
        delete_order(entry_order_id.get())

def buscar_orden():
    order_id = entry_order_id.get()
    print(order_id)
    order = get_order(order_id)
    if order:
        entry_order_date.delete(0, ctk.END)
        entry_order_date.insert(0, order[1].strftime('%Y-%m-%d %H:%M:%S') if order[1] else "")
        entry_order_mode.delete(0, ctk.END)
        entry_order_mode.insert(0, order[2] if order[2] else "")
        entry_customer_id.delete(0, ctk.END)
        entry_customer_id.insert(0, order[3] if order[3] else "")
        entry_order_status.delete(0, ctk.END)
        entry_order_status.insert(0, order[4] if order[4] else "")
        entry_order_total.delete(0, ctk.END)
        entry_order_total.insert(0, order[5] if order[5] else "")
        entry_sales_rep_id.delete(0, ctk.END)
        entry_sales_rep_id.insert(0, order[6] if order[6] else "")
        entry_promotion_id.delete(0, ctk.END)
        entry_promotion_id.insert(0, order[7] if order[7] else "")
    else:
        msg.showinfo("Resultado", "No se encontró la orden.")

def create_app(parent):
    global entry_order_id, entry_order_date, entry_order_mode, entry_customer_id, entry_order_status, entry_order_total, entry_sales_rep_id, entry_promotion_id

    label_order_id = ctk.CTkLabel(parent, text="Order ID:")
    label_order_id.pack(padx=10, pady=5)
    entry_order_id = ctk.CTkEntry(parent)
    entry_order_id.pack(padx=10, pady=5)

    label_order_date = ctk.CTkLabel(parent, text="Order Date (YYYY-MM-DD HH:MM:SS):")
    label_order_date.pack(padx=10, pady=5)
    entry_order_date = ctk.CTkEntry(parent)
    entry_order_date.pack(padx=10, pady=5)

    label_order_mode = ctk.CTkLabel(parent, text="Order Mode:")
    label_order_mode.pack(padx=10, pady=5)
    entry_order_mode = ctk.CTkEntry(parent)
    entry_order_mode.pack(padx=10, pady=5)

    label_customer_id = ctk.CTkLabel(parent, text="Customer ID:")
    label_customer_id.pack(padx=10, pady=5)
    entry_customer_id = ctk.CTkEntry(parent)
    entry_customer_id.pack(padx=10, pady=5)

    label_order_status = ctk.CTkLabel(parent, text="Order Status:")
    label_order_status.pack(padx=10, pady=5)
    entry_order_status = ctk.CTkEntry(parent)
    entry_order_status.pack(padx=10, pady=5)

    label_order_total = ctk.CTkLabel(parent, text="Order Total:")
    label_order_total.pack(padx=10, pady=5)
    entry_order_total = ctk.CTkEntry(parent)
    entry_order_total.pack(padx=10, pady=5)

    label_sales_rep_id = ctk.CTkLabel(parent, text="Sales Rep ID:")
    label_sales_rep_id.pack(padx=10, pady=5)
    entry_sales_rep_id = ctk.CTkEntry(parent)
    entry_sales_rep_id.pack(padx=10, pady=5)

    label_promotion_id = ctk.CTkLabel(parent, text="Promotion ID:")
    label_promotion_id.pack(padx=10, pady=5)
    entry_promotion_id = ctk.CTkEntry(parent)
    entry_promotion_id.pack(padx=10, pady=5)

    button_insertar = ctk.CTkButton(parent, text="Insertar Orden", command=insertar_orden)
    button_insertar.pack(padx=10, pady=10)

    button_actualizar = ctk.CTkButton(parent, text="Actualizar Orden", command=actualizar_orden)
    button_actualizar.pack(padx=10, pady=10)

    button_eliminar = ctk.CTkButton(parent, text="Eliminar Orden", command=eliminar_orden)
    button_eliminar.pack(padx=10, pady=10)

    button_buscar = ctk.CTkButton(parent, text="Buscar Orden", command=buscar_orden)
    button_buscar.pack(padx=10, pady=10)
