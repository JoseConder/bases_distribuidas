import oracledb

# Detalles de la conexión
usuario = 'system'
contraseña = '123'
dsn = 'localhost:1522/xe'  # Usar el puerto 1522 y el servicio xepdb1

try:
    # Establecer conexión
    conexion = oracledb.connect(user=usuario, password=contraseña, dsn=dsn)
    print("Conexión establecida con éxito")

    # Usar la conexión
    cursor = conexion.cursor()

    # Ejecutar consulta
    #cursor.callproc('insert_customer', ['934', 'C. Thomas', 'Nolte', 600, 'C.Thomas.Nolte@PHOEBE.COM', 'H: 150,000 - 169,999', 'A'])
    cursor.callproc('insert_customer', ['980','Daniel','Loren','200','Daniel.Loren@REDSTART.COM','F: 110,000 - 129,999','A'])


    #hacer commit
    conexion.commit()
    resultados = cursor.execute("select * from customers")


    # Obtener los resultados de la consulta
    resultados = cursor.fetchall()
    for fila in resultados:
        print(fila)

    # Cerrar cursor y conexión
    cursor.close()
    conexion.close()
    print("Conexión cerrada correctamente")

except oracledb.DatabaseError as e:
    error, = e.args
    print(f"Error al conectar a la base de datos: {error.message}")
