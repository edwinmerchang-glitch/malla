# database.py
import sqlite3
import pandas as pd
import hashlib
from datetime import datetime

DB_NAME = "turnos_database.db"

# ======================================================
# CONEXIÓN
# ======================================================
def get_connection():
    return sqlite3.connect(DB_NAME, check_same_thread=False)

# ======================================================
# INICIALIZACIÓN DE BASE DE DATOS
# ======================================================
def init_db():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS usuarios (
        username TEXT PRIMARY KEY,
        password_hash TEXT NOT NULL,
        role TEXT NOT NULL,
        nombre TEXT NOT NULL,
        departamento TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS empleados (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        numero INTEGER NOT NULL,
        cargo TEXT NOT NULL,
        nombre_completo TEXT NOT NULL,
        cedula TEXT UNIQUE NOT NULL,
        departamento TEXT NOT NULL,
        estado TEXT NOT NULL,
        hora_inicio TEXT,
        hora_fin TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS codigos_turno (
        codigo TEXT PRIMARY KEY,
        nombre TEXT NOT NULL,
        color TEXT NOT NULL,
        horas INTEGER NOT NULL
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS malla_turnos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        empleado_id INTEGER NOT NULL,
        mes INTEGER NOT NULL,
        ano INTEGER NOT NULL,
        dia INTEGER NOT NULL,
        codigo_turno TEXT,
        UNIQUE(empleado_id, mes, ano, dia)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS configuracion (
        clave TEXT PRIMARY KEY,
        valor TEXT,
        tipo TEXT,
        descripcion TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        accion TEXT,
        detalles TEXT,
        usuario TEXT,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    conn.commit()
    conn.close()

# ======================================================
# DATOS INICIALES - ACTUALIZADO CON TODOS LOS DEPARTAMENTOS
# ======================================================
def inicializar_datos_bd():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM usuarios")
    if cursor.fetchone()[0] == 0:
        usuarios = [
            ("admin", hashlib.sha256("admin123".encode()).hexdigest(), "admin", "Administrador", "Administración"),
            ("supervisor", hashlib.sha256("super123".encode()).hexdigest(), "supervisor", "Supervisor", "Tienda"),
            ("empleado", hashlib.sha256("empleado123".encode()).hexdigest(), "empleado", "Empleado Demo", "Tienda")
        ]
        cursor.executemany(
            "INSERT INTO usuarios (username, password_hash, role, nombre, departamento) VALUES (?, ?, ?, ?, ?)",
            usuarios
        )

    cursor.executemany("""
    INSERT OR IGNORE INTO codigos_turno VALUES (?, ?, ?, ?)
    """, [
        ("20", "10 AM - 7 PM", "#FF6B6B", 8),
        ("15", "8 AM - 5 PM", "#4ECDC4", 8),
        ("VC", "Vacaciones", "#9B5DE5", 0),
        ("CP", "Cumpleaños", "#00F5D4", 0),
        ("PA", "Permiso", "#FF9E00", 0),
        ("-1", "Ausente", "#E0E0E0", 0)
    ])
    
    # Configuración inicial - CON TODOS LOS DEPARTAMENTOS COMPLETOS
    cursor.execute("SELECT COUNT(*) FROM configuracion WHERE clave = 'departamentos'")
    if cursor.fetchone()[0] == 0:
        configuraciones = [
            ("departamentos", "Administración,Tienda,Droguería,Cajas,Control Interno,Equipos Médicos,Domicilios", "list", "Departamentos de la empresa"),
            ("formato_hora", "24 horas", "text", "Formato de hora"),
            ("dias_vacaciones", "15", "number", "Días de vacaciones por año"),
            ("inicio_semana", "Lunes", "text", "Día de inicio de semana")
        ]
        cursor.executemany(
            "INSERT INTO configuracion (clave, valor, tipo, descripcion) VALUES (?, ?, ?, ?)",
            configuraciones
        )
    else:
        # Si ya existe, actualizar los departamentos
        cursor.execute("""
            UPDATE configuracion 
            SET valor = 'Administración,Tienda,Droguería,Cajas,Control Interno,Equipos Médicos,Domicilios'
            WHERE clave = 'departamentos'
        """)

    conn.commit()
    conn.close()

# ======================================================
# USUARIOS
# ======================================================
def get_usuarios():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM usuarios", conn)
    conn.close()
    return df

def autenticar_usuario(username, password):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT username, password_hash, role, nombre, departamento FROM usuarios WHERE username=?",
        (username,)
    )
    user = cursor.fetchone()
    conn.close()

    if user and hashlib.sha256(password.encode()).hexdigest() == user[1]:
        return {
            "username": user[0],
            "role": user[2],
            "nombre": user[3],
            "departamento": user[4]
        }
    return None

# ======================================================
# EMPLEADOS
# ======================================================
def get_empleados():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM empleados ORDER BY numero", conn)
    conn.close()
    return df

def guardar_empleados(df):
    """Guardar cambios en la tabla de empleados"""
    conn = get_connection()
    cursor = conn.cursor()
    
    for _, row in df.iterrows():
        cursor.execute('''
            UPDATE empleados 
            SET numero = ?, cargo = ?, nombre_completo = ?, cedula = ?,
                departamento = ?, estado = ?, hora_inicio = ?, hora_fin = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (
            row['numero'], row['cargo'], row['nombre_completo'], 
            row['cedula'], row['departamento'], row['estado'],
            row.get('hora_inicio', ''), row.get('hora_fin', ''),
            row['id']
        ))
    
    conn.commit()
    conn.close()

def get_empleado_por_username(username):
    """Obtener empleado por nombre de usuario"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Primero obtener el nombre del usuario
    cursor.execute("SELECT nombre FROM usuarios WHERE username = ?", (username,))
    usuario_data = cursor.fetchone()
    
    if not usuario_data:
        conn.close()
        return None
    
    nombre_usuario = usuario_data[0]
    
    # Buscar empleado por similitud en el nombre
    cursor.execute("SELECT * FROM empleados WHERE nombre_completo LIKE ?", (f'%{nombre_usuario}%',))
    
    resultado = cursor.fetchone()
    conn.close()
    
    if resultado:
        # Convertir a diccionario
        columnas = ['id', 'numero', 'cargo', 'nombre_completo', 'cedula', 'departamento', 
                   'estado', 'hora_inicio', 'hora_fin', 'created_at', 'updated_at']
        return dict(zip(columnas, resultado))
    return None

# ======================================================
# MALLA DE TURNOS
# ======================================================
def get_malla_turnos(mes, ano):
    conn = get_connection()
    empleados = get_empleados()

    if empleados.empty:
        return pd.DataFrame()

    dias = 31
    if mes in [4, 6, 9, 11]:
        dias = 30
    if mes == 2:
        dias = 28

    base = empleados.copy()
    for d in range(1, dias + 1):
        base[f"{d}/{mes}/{ano}"] = ""

    cursor = conn.cursor()
    cursor.execute("""
    SELECT empleado_id, dia, codigo_turno
    FROM malla_turnos
    WHERE mes=? AND ano=?
    """, (mes, ano))

    for emp_id, dia, codigo in cursor.fetchall():
        idx = base[base["id"] == emp_id].index
        if not idx.empty:
            base.loc[idx, f"{dia}/{mes}/{ano}"] = codigo or ""

    conn.close()
    
    # Renombrar columnas para consistencia
    if not base.empty:
        base = base.rename(columns={
            'numero': 'N°',
            'nombre_completo': 'APELLIDOS Y NOMBRES',
            'cedula': 'CC',
            'cargo': 'CARGO',
            'departamento': 'DEPARTAMENTO',
            'estado': 'ESTADO'
        })
    
    return base.drop(columns=["id"])

def guardar_malla_turnos(df, mes, ano):
    conn = get_connection()
    cursor = conn.cursor()

    # Obtener empleados para mapear
    empleados = get_empleados()
    map_ids = dict(zip(empleados["cedula"], empleados["id"]))

    for _, row in df.iterrows():
        emp_id = map_ids.get(row["CC"])
        if not emp_id:
            continue

        for col in df.columns:
            if "/" in col:
                dia = int(col.split("/")[0])
                cursor.execute("""
                INSERT OR REPLACE INTO malla_turnos
                (empleado_id, mes, ano, dia, codigo_turno)
                VALUES (?, ?, ?, ?, ?)
                """, (emp_id, mes, ano, dia, row[col] or None))

    conn.commit()
    conn.close()

def get_turnos_empleado_mes(empleado_id, mes, ano):
    """Obtener todos los turnos de un empleado para un mes específico"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT dia, codigo_turno 
        FROM malla_turnos 
        WHERE empleado_id = ? AND mes = ? AND ano = ?
        ORDER BY dia
    ''', (empleado_id, mes, ano))
    
    turnos = cursor.fetchall()
    conn.close()
    
    # Convertir a diccionario
    turnos_dict = {dia: codigo for dia, codigo in turnos}
    return turnos_dict

# ======================================================
# CÓDIGOS DE TURNO
# ======================================================
def get_codigos_turno():
    """Obtener todos los códigos de turno"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT codigo, nombre, color, horas FROM codigos_turno")
    codigos_data = cursor.fetchall()
    conn.close()
    
    # Convertir a diccionario
    codigos_dict = {}
    for codigo, nombre, color, horas in codigos_data:
        codigos_dict[codigo] = {
            "nombre": nombre,
            "color": color,
            "horas": horas
        }
    
    return codigos_dict

# ======================================================
# CONFIGURACIÓN
# ======================================================
def get_configuracion():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT clave, valor, tipo FROM configuracion")

    config = {}
    for k, v, t in cursor.fetchall():
        if t == "number":
            config[k] = int(v)
        elif t == "boolean":
            config[k] = v == "1"
        elif t == "list":
            # Filtrar elementos vacíos
            config[k] = [item.strip() for item in v.split(",") if item.strip()]
        else:
            config[k] = v

    conn.close()
    return config

# ======================================================
# LOGS
# ======================================================
def registrar_log(accion, detalles="", usuario="system"):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO logs (accion, detalles, usuario) VALUES (?, ?, ?)",
        (accion, detalles, usuario)
    )
def check_database():
    """Verificar y crear base de datos si no existe"""
    init_db()
    inicializar_datos_bd()
    return True
    conn.commit()
    conn.close()