# app.py - Sistema Completo de Gestión de Turnos con Autenticación y SQLite
# VERSIÓN OPTIMIZADA PARA STREAMLIT CLOUD

# ============================================================================
# IMPORTACIONES
# ============================================================================
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import plotly.express as px
import plotly.graph_objects as go
import io
import json
from pathlib import Path
import base64
import hashlib
import calendar
import sqlite3
import os
import streamlit.components.v1 as components
import shutil
import time
import pytz
import tempfile
import sys

# ============================================================================
# CONFIGURACIÓN INICIAL
# ============================================================================
st.set_page_config(
    page_title="Malla de Turnos - Gestión Completa",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# DETECTAR SI ESTAMOS EN STREAMLIT CLOUD
# ============================================================================
IS_STREAMLIT_CLOUD = os.environ.get('STREAMLIT_CLOUD') == 'true' or 'streamlit.runtime.scriptrunner.script_run_context' in sys.modules

if IS_STREAMLIT_CLOUD:
    # Usar directorio temporal para persistencia
    DB_PATH = Path(tempfile.gettempdir()) / "turnos_database.db"
    DB_NAME = str(DB_PATH)
    BACKUP_DIR = Path(tempfile.gettempdir()) / "turnos_backups"
    print(f"✅ Modo Streamlit Cloud activado")
    print(f"📁 Base de datos: {DB_NAME}")
    print(f"📁 Backups: {BACKUP_DIR}")
else:
    # Modo local
    DB_NAME = "turnos_database.db"
    BACKUP_DIR = Path("turnos_backups")

# Crear directorios necesarios
BACKUP_DIR.mkdir(exist_ok=True, parents=True)

# ============================================================================
# CSS PERSONALIZADO
# ============================================================================
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1E3A8A;
        text-align: center;
        margin-bottom: 2rem;
    }
    .auto-save-notice {
        background-color: #e8f4fd;
        color: #0c5460;
        padding: 10px;
        border-radius: 5px;
        border: 1px solid #bee5eb;
        margin: 10px 0;
        text-align: center;
    }
    .metric-card {
        background-color: #f8f9fa;
        padding: 15px;
        border-radius: 10px;
        border: 1px solid #dee2e6;
        margin-bottom: 10px;
    }
    .info-card {
        background-color: #f8f9fa;
        padding: 20px;
        border-radius: 10px;
        border: 1px solid #dee2e6;
    }
    .streamlit-cloud-warning {
        background-color: #fff3cd;
        color: #856404;
        padding: 10px;
        border-radius: 5px;
        border: 1px solid #ffeaa7;
        margin: 10px 0;
        text-align: center;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# CONSTANTES Y CONFIGURACIÓN
# ============================================================================
ZONA_HORARIA_COLOMBIA = "America/Bogota"

ROLES = {
    "admin": {
        "permissions": ["read", "write", "delete", "configure", "manage_users"],
        "description": "Acceso completo al sistema"
    },
    "supervisor": {
        "permissions": ["read", "write"],
        "description": "Puede ver y editar, pero no configurar"
    },
    "empleado": {
        "permissions": ["read"],
        "description": "Solo visualización de información"
    }
}

# ============================================================================
# FUNCIONES DE ZONA HORARIA (COLOMBIA)
# ============================================================================
def obtener_hora_colombia():
    """Obtener la hora actual de Colombia"""
    try:
        zona_colombia = pytz.timezone(ZONA_HORARIA_COLOMBIA)
        hora_colombia = datetime.now(zona_colombia)
        return hora_colombia
    except Exception as e:
        print(f"Error al obtener hora de Colombia: {e}")
        return datetime.now()

def formatear_hora_colombia(dt=None, formato="%Y-%m-%d %H:%M:%S"):
    """Formatear hora en formato Colombia"""
    if dt is None:
        dt = obtener_hora_colombia()
    
    if dt.tzinfo is None:
        zona_colombia = pytz.timezone(ZONA_HORARIA_COLOMBIA)
        dt = zona_colombia.localize(dt)
    
    return dt.strftime(formato)

# ============================================================================
# CONFIGURACIÓN DE BASE DE DATOS SQLite
# ============================================================================
def init_db():
    """Inicializar la base de datos y crear tablas si no existen"""
    print(f"📁 Inicializando base de datos en: {DB_NAME}")
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # Tabla de usuarios
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS usuarios (
            username TEXT PRIMARY KEY,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL,
            nombre TEXT NOT NULL,
            departamento TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Tabla de empleados
    cursor.execute('''
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
    ''')
    
    # Tabla de códigos de turno
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS codigos_turno (
            codigo TEXT PRIMARY KEY,
            nombre TEXT NOT NULL,
            color TEXT NOT NULL,
            horas INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Tabla de malla de turnos
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS malla_turnos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            empleado_id INTEGER NOT NULL,
            mes INTEGER NOT NULL,
            ano INTEGER NOT NULL,
            dia INTEGER NOT NULL,
            codigo_turno TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (empleado_id) REFERENCES empleados(id),
            FOREIGN KEY (codigo_turno) REFERENCES codigos_turno(codigo),
            UNIQUE(empleado_id, mes, ano, dia)
        )
    ''')
    
    # Tabla de logs
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            accion TEXT NOT NULL,
            detalles TEXT,
            usuario TEXT
        )
    ''')
    
    # Tabla de configuración
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS configuracion (
            clave TEXT PRIMARY KEY,
            valor TEXT,
            tipo TEXT,
            descripcion TEXT
        )
    ''')
    
    conn.commit()
    conn.close()
    print("✅ Base de datos inicializada correctamente")

def get_connection():
    """Obtener conexión a la base de datos"""
    return sqlite3.connect(DB_NAME)

def actualizar_estructura_bd():
    """Actualizar la estructura de la base de datos si faltan columnas"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("PRAGMA table_info(malla_turnos)")
        columnas = [col[1] for col in cursor.fetchall()]
        
        if 'updated_at' not in columnas:
            cursor.execute('ALTER TABLE malla_turnos ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
        
        cursor.execute("PRAGMA table_info(empleados)")
        columnas_emp = [col[1] for col in cursor.fetchall()]
        
        if 'updated_at' not in columnas_emp:
            cursor.execute('ALTER TABLE empleados ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
        
        conn.commit()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Error al actualizar estructura BD: {str(e)}")
        return False

# ============================================================================
# INICIALIZACIÓN DE DATOS
# ============================================================================
def inicializar_datos_bd():
    """Inicializar datos por defecto en la base de datos"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM usuarios")
    if cursor.fetchone()[0] == 0:
        print("📝 Inicializando datos por defecto...")
        
        # Usuarios por defecto
        usuarios_default = [
            ("admin", hashlib.sha256("admin123".encode()).hexdigest(), "admin", "Administrador Sistema", "Administración"),
            ("supervisor", hashlib.sha256("super123".encode()).hexdigest(), "supervisor", "Supervisor General", "Tienda"),
            ("empleado", hashlib.sha256("empleado123".encode()).hexdigest(), "empleado", "Juan Pérez García", "Tienda")
        ]
        
        cursor.executemany(
            "INSERT INTO usuarios (username, password_hash, role, nombre, departamento) VALUES (?, ?, ?, ?, ?)",
            usuarios_default
        )
        print("✅ Usuarios por defecto creados")
        
        # Códigos de turno por defecto
        codigos_default = [
            ("20", "10 AM - 7 PM", "#FF6B6B", 8),
            ("15", "8 AM - 5 PM", "#4ECDC4", 8),
            ("70", "9:00 AM - 7:30 PM", "#FFD166", 9),
            ("155", "11 AM - 7 PM", "#06D6A0", 7),
            ("151", "8 AM - 4 PM", "#118AB2", 7),
            ("177", "1:30 PM - 8:30 PM", "#EF476F", 7),
            ("149", "7 AM - 3 PM", "#073B4C", 7),
            ("26", "11 AM - 8:30 PM", "#7209B7", 9),
            ("158", "12:30 PM - 8:30 PM", "#F15BB5", 10),
            ("214", "1 PM - 8:30 PM", "#00BBF9", 8),
            ("VC", "Vacaciones", "#9B5DE5", 0),
            ("CP", "Cumpleaños", "#00F5D4", 0),
            ("PA", "Permiso Administrativo", "#FF9E00", 0),
            ("-1", "Ausente", "#E0E0E0", 0)
        ]
        
        cursor.executemany(
            "INSERT OR IGNORE INTO codigos_turno (codigo, nombre, color, horas) VALUES (?, ?, ?, ?)",
            codigos_default
        )
        print("✅ Códigos de turno por defecto creados")
        
        # Configuración por defecto
        config_default = [
            ("formato_hora", "24 horas", "text", "Formato de hora"),
            ("dias_vacaciones", "15", "number", "Días de vacaciones por año"),
            ("inicio_semana", "Lunes", "text", "Día de inicio de semana"),
            ("departamentos", "Administración,Tienda,Droguería,Cajas,Domicilios,Control Interno,Equipos Médicos", "list", "Departamentos disponibles"),
            ("auto_save", "1", "boolean", "Guardado automático"),
            ("zona_horaria", ZONA_HORARIA_COLOMBIA, "text", "Zona horaria del sistema"),
            ("auto_backup", "1", "boolean", "Backup automático en Streamlit Cloud"),
            ("max_backups", "5", "number", "Máximo de backups en Streamlit Cloud")
        ]
        
        cursor.executemany(
            "INSERT OR IGNORE INTO configuracion (clave, valor, tipo, descripcion) VALUES (?, ?, ?, ?)",
            config_default
        )
        print("✅ Configuración por defecto creada")
        
        # Empleados de ejemplo
        empleados_default = [
            (1, "SUBDIRECTOR/REGENTE", "MERCHAN EDWIN", "1055272480", "Administración", "Activo", "06:00", "14:00"),
            (2, "JEFE DE TIENDA", "SANCHEZ BEYANIDA", "23755474", "Tienda", "Activo", "05:00", "13:30"),
            (3, "COORDINADORA DE DROGUERIA", "ABRIL JOHANNA", "1000119978", "Droguería", "Activo", "06:00", "14:30")
        ]
        
        cursor.executemany(
            """INSERT INTO empleados 
            (numero, cargo, nombre_completo, cedula, departamento, estado, hora_inicio, hora_fin) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            empleados_default
        )
        print("✅ Empleados de ejemplo creados")
    
    conn.commit()
    conn.close()

# ============================================================================
# FUNCIONES DE ACCESO A DATOS
# ============================================================================
def get_usuarios():
    """Obtener todos los usuarios"""
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM usuarios", conn)
    conn.close()
    return df

def get_empleados():
    """Obtener todos los empleados"""
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM empleados ORDER BY numero", conn)
    conn.close()
    return df

def get_codigos_turno():
    """Obtener todos los códigos de turno"""
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM codigos_turno", conn)
    conn.close()
    
    codigos_dict = {"": {"color": "#FFFFFF", "nombre": "Sin Asignar", "horas": 0}}
    for _, row in df.iterrows():
        codigos_dict[row['codigo']] = {
            "color": row['color'],
            "nombre": row['nombre'],
            "horas": row['horas']
        }
    
    return codigos_dict

def get_configuracion():
    """Obtener configuración del sistema"""
    conn = get_connection()
    cursor = conn.cursor()
    
    config = {
        'formato_hora': '24 horas',
        'dias_vacaciones': 15,
        'inicio_semana': 'Lunes',
        'departamentos': [
            "Administración", "Tienda", "Droguería", 
            "Cajas", "Domicilios", "Control Interno", 
            "Equipos Médicos"
        ],
        'auto_save': True,
        'zona_horaria': ZONA_HORARIA_COLOMBIA,
        'auto_backup': True,
        'max_backups': 5
    }
    
    cursor.execute("SELECT clave, valor, tipo FROM configuracion")
    for clave, valor, tipo in cursor.fetchall():
        if tipo == 'number':
            config[clave] = int(valor)
        elif tipo == 'boolean':
            config[clave] = valor == '1'
        elif clave == 'departamentos':
            config[clave] = valor.split(',')
        else:
            config[clave] = valor
    
    conn.close()
    return config

def get_malla_turnos(mes, ano):
    """Obtener malla de turnos para un mes específico"""
    conn = get_connection()
    
    empleados_df = get_empleados()
    
    if empleados_df.empty:
        conn.close()
        return pd.DataFrame()
    
    df_base = empleados_df.copy()
    df_base = df_base.rename(columns={
        'id': 'ID',
        'numero': 'N°',
        'cargo': 'CARGO',
        'nombre_completo': 'APELLIDOS Y NOMBRES',
        'cedula': 'CC',
        'departamento': 'DEPARTAMENTO',
        'estado': 'ESTADO',
        'hora_inicio': 'HORA_INICIO',
        'hora_fin': 'HORA_FIN'
    })
    
    num_dias = calendar.monthrange(ano, mes)[1]
    
    cursor = conn.cursor()
    cursor.execute('''
        SELECT e.id, mt.dia, mt.codigo_turno 
        FROM empleados e
        LEFT JOIN malla_turnos mt ON e.id = mt.empleado_id AND mt.mes = ? AND mt.ano = ?
        ORDER BY e.numero
    ''', (mes, ano))
    
    turnos_data = cursor.fetchall()
    
    turnos_dict = {}
    for emp_id, dia, codigo in turnos_data:
        if emp_id not in turnos_dict:
            turnos_dict[emp_id] = {}
        if dia:
            turnos_dict[emp_id][dia] = codigo if codigo else ""
    
    for dia in range(1, num_dias + 1):
        col_name = f'{dia}/{mes}/{ano}'
        df_base[col_name] = ""
        
        for idx, row in df_base.iterrows():
            emp_id = row['ID']
            if emp_id in turnos_dict and dia in turnos_dict[emp_id]:
                df_base.at[idx, col_name] = turnos_dict[emp_id][dia]
    
    if 'ID' in df_base.columns:
        df_base = df_base.drop(columns=['ID'])
    
    conn.close()
    return df_base

def get_turnos_empleado_mes(empleado_id, mes, ano):
    """Obtener todos los turnos de un empleado para un mes específico"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT id FROM empleados WHERE id = ?', (empleado_id,))
        if not cursor.fetchone():
            conn.close()
            return {}
        
        cursor.execute('''
            SELECT dia, codigo_turno 
            FROM malla_turnos 
            WHERE empleado_id = ? AND mes = ? AND ano = ?
            ORDER BY dia
        ''', (empleado_id, mes, ano))
        
        turnos = cursor.fetchall()
        conn.close()
        
        turnos_dict = {}
        for dia, codigo in turnos:
            if codigo is not None and str(codigo).strip() != '':
                turnos_dict[int(dia)] = str(codigo).strip()
            else:
                turnos_dict[int(dia)] = ""
        
        return turnos_dict
        
    except Exception as e:
        print(f"Error en get_turnos_empleado_mes: {str(e)}")
        return {}

# ============================================================================
# FUNCIONES DE GUARDADO DE DATOS
# ============================================================================
def guardar_malla_turnos(df_malla, mes, ano):
    """Guardar malla de turnos en la base de datos"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        empleados_df = get_empleados()
        id_por_cedula = {}
        for _, emp in empleados_df.iterrows():
            id_por_cedula[str(emp['cedula'])] = emp['id']
        
        num_dias = calendar.monthrange(ano, mes)[1]
        cambios_guardados = 0
        
        for idx, row in df_malla.iterrows():
            cedula = str(row.get('CC', ''))
            if not cedula or cedula not in id_por_cedula:
                continue
            
            emp_id = id_por_cedula[cedula]
            
            for dia in range(1, num_dias + 1):
                col_name = f'{dia}/{mes}/{ano}'
                if col_name in row:
                    codigo = row[col_name]
                    
                    if pd.isna(codigo) or codigo is None or str(codigo).strip() == '':
                        codigo_valor = None
                    else:
                        codigo_valor = str(codigo).strip()
                    
                    cursor.execute('''
                        INSERT OR REPLACE INTO malla_turnos 
                        (empleado_id, mes, ano, dia, codigo_turno, updated_at)
                        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ''', (emp_id, mes, ano, dia, codigo_valor))
                    
                    if cursor.rowcount > 0:
                        cambios_guardados += 1
        
        conn.commit()
        conn.close()
        
        return cambios_guardados
        
    except Exception as e:
        st.error(f"❌ Error al guardar malla: {str(e)}")
        return 0

def guardar_empleados(df_editado):
    """Guardar empleados en la base de datos"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cambios_realizados = 0
        
        for _, row in df_editado.iterrows():
            if 'ID_OCULTO' in row and pd.notna(row['ID_OCULTO']):
                cursor.execute('''
                    UPDATE empleados 
                    SET cargo = ?, nombre_completo = ?, cedula = ?, 
                        departamento = ?, estado = ?, hora_inicio = ?, hora_fin = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (
                    str(row['CARGO']) if pd.notna(row['CARGO']) else '',
                    str(row['APELLIDOS Y NOMBRES']) if pd.notna(row['APELLIDOS Y NOMBRES']) else '',
                    str(row['CC']) if pd.notna(row['CC']) else '',
                    str(row['DEPARTAMENTO']) if pd.notna(row['DEPARTAMENTO']) else '',
                    str(row['ESTADO']) if pd.notna(row['ESTADO']) else 'Activo',
                    str(row['HORA_INICIO']) if pd.notna(row['HORA_INICio']) else None,
                    str(row['HORA_FIN']) if pd.notna(row['HORA_FIN']) else None,
                    int(row['ID_OCULTO'])
                ))
                
                cambios_realizados += cursor.rowcount
                
            else:
                cursor.execute("SELECT MAX(numero) FROM empleados")
                max_num = cursor.fetchone()[0]
                nuevo_numero = (max_num or 0) + 1
                
                cursor.execute('''
                    INSERT INTO empleados 
                    (numero, cargo, nombre_completo, cedula, departamento, estado, hora_inicio, hora_fin)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    nuevo_numero,
                    str(row.get('CARGO', '')),
                    str(row.get('APELLIDOS Y NOMBRES', '')),
                    str(row.get('CC', '')),
                    str(row.get('DEPARTAMENTO', '')),
                    str(row.get('ESTADO', 'Activo')),
                    str(row.get('HORA_INICIO', '')),
                    str(row.get('HORA_FIN', ''))
                ))
                
                cambios_realizados += 1
        
        conn.commit()
        conn.close()
        
        st.session_state.empleados_df = get_empleados()
        
        return cambios_realizados, []
        
    except Exception as e:
        print(f"❌ Error al guardar empleados: {str(e)}")
        return 0, []

def guardar_usuarios(edited_df, original_df):
    """Guardar cambios en usuarios"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cambios = 0
        
        for _, edited_row in edited_df.iterrows():
            username = edited_row['USUARIO']
            
            original_row = original_df[original_df['username'] == username]
            
            if not original_row.empty:
                original_row = original_row.iloc[0]
                
                cambios_detectados = False
                
                if str(edited_row['NOMBRE_COMPLETO']) != str(original_row['nombre']):
                    cambios_detectados = True
                elif str(edited_row['ROL']) != str(original_row['role']):
                    cambios_detectados = True
                elif str(edited_row['DEPARTAMENTO']) != str(original_row.get('departamento', '')):
                    cambios_detectados = True
                
                if cambios_detectados:
                    cursor.execute('''
                        UPDATE usuarios 
                        SET nombre = ?, role = ?, departamento = ?
                        WHERE username = ?
                    ''', (
                        str(edited_row['NOMBRE_COMPLETO']),
                        str(edited_row['ROL']),
                        str(edited_row['DEPARTAMENTO']),
                        username
                    ))
                    
                    cambios += cursor.rowcount
        
        conn.commit()
        conn.close()
        
        return cambios
        
    except Exception as e:
        print(f"❌ Error al guardar usuarios: {str(e)}")
        return 0

# ============================================================================
# SISTEMA DE BACKUP (OPTIMIZADO PARA STREAMLIT CLOUD)
# ============================================================================
def crear_backup_automatico():
    """Crear backup automático de la base de datos"""
    try:
        timestamp = obtener_hora_colombia().strftime("%Y%m%d_%H%M%S")
        
        backup_file = BACKUP_DIR / f"turnos_backup_{timestamp}.db"
        
        if os.path.exists(DB_NAME):
            shutil.copy2(DB_NAME, backup_file)
            
            # Mantener solo los últimos 5 backups en Streamlit Cloud
            backups = sorted(BACKUP_DIR.glob("turnos_backup_*.db"), key=os.path.getmtime, reverse=True)
            
            if IS_STREAMLIT_CLOUD:
                max_backups = st.session_state.configuracion.get('max_backups', 5) if 'configuracion' in st.session_state else 5
                if len(backups) > max_backups:
                    for old_backup in backups[max_backups:]:
                        try:
                            old_backup.unlink()
                            print(f"🗑️ Backup antiguo eliminado: {old_backup.name}")
                        except:
                            pass
            
            print(f"✅ Backup automático creado: {backup_file.name}")
            return backup_file
            
        else:
            print("⚠️ No se encontró la base de datos para hacer backup")
            return None
            
    except Exception as e:
        print(f"❌ Error en backup automático: {str(e)}")
        return None

def restaurar_backup(backup_file):
    """Restaurar base de datos desde backup"""
    try:
        timestamp = obtener_hora_colombia().strftime("%Y%m%d_%H%M%S_rescue")
        rescue_file = BACKUP_DIR / f"rescue_{timestamp}.db"
        
        if os.path.exists(DB_NAME):
            shutil.copy2(DB_NAME, rescue_file)
        
        shutil.copy2(backup_file, DB_NAME)
        
        # Actualizar session state
        st.session_state.empleados_df = get_empleados()
        st.session_state.codigos_turno = get_codigos_turno()
        st.session_state.configuracion = get_configuracion()
        
        st.success(f"✅ Backup restaurado desde: {backup_file.name}")
        st.info(f"⚠️ Se creó un archivo de rescate: {rescue_file.name}")
        
        registrar_log("restaurar_backup", f"Desde: {backup_file.name}")
        return True
        
    except Exception as e:
        st.error(f"❌ Error al restaurar backup: {str(e)}")
        return False

def guardar_malla_turnos_con_backup(df_malla, mes, ano):
    """Guardar malla de turnos con backup automático"""
    # Crear backup si está configurado
    if st.session_state.configuracion.get('auto_backup', True):
        crear_backup_automatico()
    
    resultado = guardar_malla_turnos(df_malla, mes, ano)
    
    if resultado > 0:
        st.session_state.last_save = obtener_hora_colombia()
    
    return resultado

# ============================================================================
# FUNCIONES DE AUTENTICACIÓN
# ============================================================================
def login(username, password):
    """Autenticar usuario desde base de datos"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute(
        "SELECT username, password_hash, role, nombre, departamento FROM usuarios WHERE username = ?",
        (username,)
    )
    
    result = cursor.fetchone()
    conn.close()
    
    if result:
        stored_hash = result[1]
        password_hash = hashlib.sha256(password.encode()).hexdigest()
        
        if stored_hash == password_hash:
            nombre_usuario = result[3]
            
            empleados_df = get_empleados()
            
            if empleados_df.empty:
                st.session_state.empleado_actual = None
            else:
                nombre_buscado = nombre_usuario.strip().upper()
                
                empleado_encontrado = empleados_df[
                    empleados_df['nombre_completo'].str.strip().str.upper() == nombre_buscado
                ]
                
                if empleado_encontrado.empty:
                    def buscar_coincidencia(nombre_empleado):
                        nombre_emp = str(nombre_empleado).upper()
                        return (nombre_buscado in nombre_emp or nombre_emp in nombre_buscado)
                    
                    mask = empleados_df['nombre_completo'].apply(buscar_coincidencia)
                    empleado_encontrado = empleados_df[mask]
                
                if not empleado_encontrado.empty:
                    st.session_state.empleado_actual = empleado_encontrado.iloc[0].to_dict()
                else:
                    st.session_state.empleado_actual = None
            
            st.session_state.auth = {
                'is_authenticated': True,
                'username': username,
                'role': result[2],
                'user_data': {
                    'nombre': nombre_usuario,
                    'departamento': result[4]
                }
            }
            
            registrar_log("login", f"Usuario {username} inició sesión")
            return True
    
    return False

def logout():
    """Cerrar sesión"""
    if 'auth' in st.session_state and st.session_state.auth['is_authenticated']:
        username = st.session_state.auth['username']
        registrar_log("logout", f"Usuario {username} cerró sesión")
    
    st.session_state.auth = {
        'is_authenticated': False,
        'username': None,
        'role': None,
        'user_data': None
    }
    st.session_state.empleado_actual = None
    st.rerun()

def check_permission(permission):
    """Verificar si el usuario tiene un permiso específico"""
    if not st.session_state.auth['is_authenticated']:
        return False
    
    role = st.session_state.auth['role']
    return permission in ROLES.get(role, {}).get('permissions', [])

def registrar_log(accion, detalles=""):
    """Registrar acciones importantes en la base de datos"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute(
        "INSERT INTO logs (accion, detalles, usuario) VALUES (?, ?, ?)",
        (accion, detalles, st.session_state.auth.get('username', 'anonymous'))
    )
    
    conn.commit()
    conn.close()

# ============================================================================
# FUNCIONES DE SESSION STATE (OPTIMIZADAS PARA STREAMLIT CLOUD)
# ============================================================================
def inicializar_session_state():
    """Inicializar todas las variables de session_state con persistencia mejorada"""
    print(f"🔄 Inicializando session_state (Streamlit Cloud: {IS_STREAMLIT_CLOUD})")
    
    # Inicializar base de datos
    init_db()
    actualizar_estructura_bd()
    inicializar_datos_bd()
    
    # Intentar restaurar desde último backup si estamos en Streamlit Cloud
    if IS_STREAMLIT_CLOUD:
        try:
            backups = sorted(BACKUP_DIR.glob("turnos_backup_*.db"), key=os.path.getmtime, reverse=True)
            if backups:
                print(f"📂 Encontrados {len(backups)} backups")
                # Usar el backup más reciente
                shutil.copy2(backups[0], DB_NAME)
                print(f"✅ Restaurado desde backup: {backups[0].name}")
        except Exception as e:
            print(f"⚠️ No se pudo restaurar backup: {e}")
    
    hora_colombia = obtener_hora_colombia()
    
    defaults = {
        'empleados_df': get_empleados(),
        'codigos_turno': get_codigos_turno(),
        'configuracion': get_configuracion(),
        'auth': {
            'is_authenticated': False,
            'username': None,
            'role': None,
            'user_data': None
        },
        'logs': [],
        'current_page': 'malla',
        'last_save': None,
        'mes_actual': hora_colombia.month,
        'ano_actual': hora_colombia.year,
        'malla_actual': pd.DataFrame(),
        'calendario_mes': hora_colombia.month,
        'calendario_ano': hora_colombia.year,
        'empleado_actual': None,
        'app_initialized': True
    }
    
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value
    
    # Crear backup inicial si no existe
    backups = list(BACKUP_DIR.glob("turnos_backup_*.db"))
    if not backups:
        crear_backup_automatico()
        print("✅ Backup inicial creado")

# ============================================================================
# PÁGINA DE LOGIN
# ============================================================================
def pagina_login():
    """Página de inicio de sesión"""
    st.markdown("<h1 class='main-header'>🔐 Malla de Turnos Locatel Restrepo</h1>", unsafe_allow_html=True)
    
    # Mostrar advertencia de Streamlit Cloud
    if IS_STREAMLIT_CLOUD:
        st.markdown("""
        <div class="streamlit-cloud-warning">
        ⚠️ MODO STREAMLIT CLOUD ACTIVADO<br>
        <small>Los datos se guardan en almacenamiento temporal. Exporta tus datos regularmente.</small>
        </div>
        """, unsafe_allow_html=True)
    
    with st.container():
        col1, col2, col3 = st.columns([1, 2, 1])
        
        with col2:
            st.markdown("""
            <div class="info-card">
                <h3 style="text-align: center; color: #1E3A8A;">Inicio de Sesión</h3>
                <p style="text-align: center; color: #6c757d;">Ingrese sus credenciales para acceder</p>
            </div>
            """, unsafe_allow_html=True)
            
            with st.form("login_form"):
                username = st.text_input("👤 Usuario", placeholder="Ingrese su usuario")
                password = st.text_input("🔒 Contraseña", type="password", placeholder="Ingrese su contraseña")
                
                submit = st.form_submit_button("🚀 Ingresar", use_container_width=True)
                
                if submit:
                    if login(username, password):
                        st.success(f"✅ Bienvenido, {username}!")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("❌ Usuario o contraseña incorrectos")
            
            # Credenciales de prueba
            with st.expander("🔑 Credenciales de prueba"):
                st.markdown("""
                **Administrador:**
                - Usuario: `admin`
                - Contraseña: `admin123`
                
                **Supervisor:**
                - Usuario: `supervisor`
                - Contraseña: `super123`
                
                **Empleado:**
                - Usuario: `empleado`
                - Contraseña: `empleado123`
                """)

# ============================================================================
# COMPONENTES DE INTERFAZ
# ============================================================================
def mostrar_barra_usuario():
    """Mostrar barra superior con información del usuario"""
    if st.session_state.auth['is_authenticated']:
        user_info = st.session_state.auth['user_data']
        
        col1, col2, col3, col4 = st.columns([3, 1, 1, 1])
        
        with col1:
            st.markdown(f"""
            <div style="background-color: #1E3A8A; color: white; padding: 10px 20px; 
                        border-radius: 5px; margin-bottom: 20px;">
                <strong>👤 {user_info['nombre']}</strong> | 
                <span>Rol: {st.session_state.auth['role'].title()}</span> |
                <span>Depto: {user_info.get('departamento', 'N/A')}</span>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            if st.session_state.last_save:
                tiempo_transcurrido = obtener_hora_colombia() - st.session_state.last_save
                minutos = int(tiempo_transcurrido.total_seconds() / 60)
                if minutos < 1:
                    tiempo_texto = "Ahora"
                elif minutos == 1:
                    tiempo_texto = "Hace 1 minuto"
                else:
                    tiempo_texto = f"Hace {minutos} minutos"
                st.metric("💾 Guardado", tiempo_texto)
        
        with col3:
            if st.button("🔄 Recargar", use_container_width=True):
                st.rerun()
        
        with col4:
            if st.button("🚪 Cerrar Sesión", use_container_width=True):
                logout()

def mostrar_sidebar():
    """Mostrar sidebar según el rol del usuario"""
    with st.sidebar:
        rol = st.session_state.auth['role']
        
        st.markdown(f"<h3 style='text-align: center;'>📊 {rol.title()}</h3>", unsafe_allow_html=True)
        st.markdown("---")
        
        # Mostrar hora Colombia
        hora_colombia = obtener_hora_colombia()
        st.markdown(f"""
        <div style="text-align: center; padding: 10px; background-color: #1E3A8A; 
                   color: white; border-radius: 5px; margin-bottom: 10px;">
            <div style="font-size: 1.1em; font-weight: bold;">
                {hora_colombia.strftime('%H:%M')}
            </div>
            <div style="font-size: 0.8em;">
                {hora_colombia.strftime('%d/%m')} • Colombia
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Opciones según rol
        if rol == "admin":
            opciones = [
                ("📅 Malla de Turnos", "malla"),
                ("👥 Gestión de Empleados", "empleados"),
                ("⚙️ Configuración", "config"),
                ("👑 Gestión de Usuarios", "usuarios"),
                ("📦 Backup", "backup"),
                ("🖥️ Info Sistema", "info_sistema")
            ]
        elif rol == "supervisor":
            opciones = [
                ("📅 Malla de Turnos", "malla"),
                ("👥 Empleados", "empleados")
            ]
        else:  # empleado
            opciones = [
                ("📅 Mis Turnos", "mis_turnos"),
                ("📆 Mi Calendario", "calendario"),
                ("👤 Mi Información", "mi_info")
            ]
        
        # Botones de navegación
        for icon_text, key in opciones:
            if st.button(icon_text, key=f"nav_{key}", use_container_width=True):
                st.session_state.current_page = key
        
        st.markdown("---")
        
        # Información del sistema
        if rol == "admin":
            total_empleados = len(st.session_state.empleados_df)
            activos = st.session_state.empleados_df[st.session_state.empleados_df['estado'] == 'Activo'].shape[0]
            
            st.markdown("**📈 Estadísticas**")
            st.info(f"""
            Empleados: {total_empleados}  
            Activos: {activos}  
            Códigos: {len(st.session_state.codigos_turno) - 1}
            """)
            
            # Estado de Streamlit Cloud
            if IS_STREAMLIT_CLOUD:
                backups = list(BACKUP_DIR.glob("turnos_backup_*.db"))
                st.markdown("**☁️ Streamlit Cloud**")
                st.warning(f"Backups: {len(backups)}/5")
                st.caption("Exporta datos regularmente")

def monitoreo_sistema():
    """Mostrar estado del sistema"""
    with st.sidebar.expander("📊 Estado del Sistema", expanded=False):
        if os.path.exists(DB_NAME):
            size_mb = os.path.getsize(DB_NAME) / (1024 * 1024)
            st.metric("Tamaño BD", f"{size_mb:.1f} MB")
        
        if BACKUP_DIR.exists():
            num_backups = len(list(BACKUP_DIR.glob("turnos_backup_*.db")))
            st.metric("Backups", num_backups)
        
        if st.session_state.last_save:
            tiempo = obtener_hora_colombia() - st.session_state.last_save
            minutos = int(tiempo.total_seconds() / 60)
            st.metric("Último guardado", f"Hace {minutos} min")
        
        # Estado de Streamlit Cloud
        if IS_STREAMLIT_CLOUD:
            st.warning("☁️ Modo Cloud")
            if st.button("🔄 Forzar Backup"):
                backup = crear_backup_automatico()
                if backup:
                    st.success(f"✅ Backup creado: {backup.name}")
                    st.rerun()

# ============================================================================
# FUNCIONES DE VISUALIZACIÓN
# ============================================================================
def aplicar_estilo_dataframe(df):
    """Aplicar estilos de colores al DataFrame"""
    def color_cell(val):
        if pd.isna(val) or val == '':
            return 'background-color: #FFFFFF; border: 1px solid #e0e0e0;'
        color = st.session_state.codigos_turno.get(str(val), {}).get("color", "#FFFFFF")
        return f'background-color: {color}; color: black; font-weight: bold; text-align: center; border: 1px solid #e0e0e0;'
    
    day_columns = [col for col in df.columns if '/' in str(col)]
    
    if day_columns:
        styled_df = df.style.applymap(color_cell, subset=day_columns)
        return styled_df
    return df.style

def mostrar_leyenda():
    """Mostrar leyenda de colores"""
    st.markdown("### 🎨 Leyenda de Turnos")
    
    cols = st.columns(4)
    items = list(st.session_state.codigos_turno.items())
    
    for idx, (codigo, info) in enumerate(items):
        if codigo == "":
            continue
            
        with cols[idx % 4]:
            st.markdown(f"""
            <div style="display: flex; align-items: center; margin-bottom: 10px; padding: 5px; background: white; border-radius: 5px; border: 1px solid #e0e0e0;">
                <div style="width: 20px; height: 20px; background-color: {info['color']}; margin-right: 10px; border-radius: 3px;"></div>
                <div>
                    <strong>{codigo}</strong><br>
                    <small>{info['nombre']} ({info['horas']}h)</small>
                </div>
            </div>
            """, unsafe_allow_html=True)

def generar_calendario_simple(mes, ano, turnos_dict):
    """Generar calendario simple"""
    nombres_meses = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", 
                    "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
    
    num_dias = calendar.monthrange(ano, mes)[1]
    dias_semana = ["Dom", "Lun", "Mar", "Mié", "Jue", "Vie", "Sáb"]
    
    primer_dia = date(ano, mes, 1)
    dia_semana_python = primer_dia.weekday()
    dia_inicio_semana = (dia_semana_python + 1) % 7
    
    st.markdown(f"### 📅 {nombres_meses[mes-1]} {ano}")
    
    # Encabezados
    cols_encabezado = st.columns(7)
    for idx, dia in enumerate(dias_semana):
        with cols_encabezado[idx]:
            if idx == 0:
                st.markdown(f"<div style='color: #d32f2f; text-align: center; font-weight: bold; padding: 8px 5px; background: #fff5f5; border-radius: 3px;'>DOM</div>", 
                           unsafe_allow_html=True)
            elif idx == 6:
                st.markdown(f"<div style='color: #1976d2; text-align: center; font-weight: bold; padding: 8px 5px; background: #f0f7ff; border-radius: 3px;'>SÁB</div>", 
                           unsafe_allow_html=True)
            else:
                st.markdown(f"<div style='text-align: center; font-weight: bold; padding: 8px 5px; background: #f8f9fa; border-radius: 3px;'>{dia.upper()}</div>", 
                           unsafe_allow_html=True)
    
    # Calendario
    dia_actual = 0
    celdas_totales = dia_inicio_semana + num_dias
    filas_necesarias = (celdas_totales + 6) // 7
    
    for fila in range(filas_necesarias):
        cols = st.columns(7)
        
        for columna in range(7):
            with cols[columna]:
                posicion_global = fila * 7 + columna
                num_dia_celda = posicion_global - dia_inicio_semana + 1
                
                if posicion_global < dia_inicio_semana or num_dia_celda > num_dias or num_dia_celda < 1:
                    st.markdown(f"""
                    <div style="height: 80px; display: flex; align-items: center; justify-content: center;
                               background-color: transparent;">
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    dia_num = num_dia_celda
                    codigo = turnos_dict.get(dia_num, "")
                    
                    if codigo and str(codigo).strip() != "":
                        codigo_str = str(codigo).strip()
                        turno_info = st.session_state.codigos_turno.get(codigo_str, 
                                                                       {"color": "#e0e0e0", "nombre": "Turno"})
                        color = turno_info["color"]
                        nombre_turno = turno_info.get("nombre", "")
                        tiene_turno = True
                    else:
                        color = "#ffffff"
                        tiene_turno = False
                        codigo_str = ""
                    
                    dia_semana_actual = (dia_inicio_semana + dia_num - 1) % 7
                    hoy = obtener_hora_colombia()
                    es_hoy = (dia_num == hoy.day and mes == hoy.month and ano == hoy.year)
                    
                    estilo_numero = "font-weight: bold;"
                    if dia_semana_actual == 0:
                        estilo_numero += " color: #d32f2f;"
                        borde_dia = "#d32f2f"
                    elif dia_semana_actual == 6:
                        estilo_numero += " color: #1976d2;"
                        borde_dia = "#1976d2"
                    else:
                        borde_dia = "#e0e0e0"
                    
                    if es_hoy:
                        borde_dia = "#FF5722"
                        estilo_numero += " text-shadow: 0 0 1px #FF5722;"
                    
                    if nombre_turno and len(nombre_turno) > 14:
                        if " " in nombre_turno[:14]:
                            partes = nombre_turno.split(" ")
                            nombre_corto = ""
                            for parte in partes:
                                if len(nombre_corto + " " + parte) <= 14:
                                    nombre_corto += " " + parte if nombre_corto else parte
                                else:
                                    break
                            nombre_mostrar = nombre_corto + ".." if nombre_corto else nombre_turno[:12] + ".."
                        else:
                            nombre_mostrar = nombre_turno[:12] + ".."
                    else:
                        nombre_mostrar = nombre_turno if tiene_turno else ""
                    
                    contenido = f"""
                    <div style="background-color: {color}; 
                               padding: 5px; 
                               border-radius: 5px; 
                               border: 2px solid {borde_dia};
                               text-align: center;
                               height: 80px;
                               display: flex;
                               flex-direction: column;
                               justify-content: center;
                               overflow: hidden;
                               box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
                        <div style="{estilo_numero} font-size: 1.3em; line-height: 1.2;">
                            {dia_num}
                        </div>
                        {f'<div style="font-size: 1.0em; font-weight: bold; color: #222; margin-top: 4px; line-height: 1.1;">{codigo_str}</div>' if tiene_turno else ''}
                        {f'<div style="font-size: 0.75em; color: #444; margin-top: 2px; line-height: 1.1; opacity: 0.9;">{nombre_mostrar}</div>' if tiene_turno and nombre_mostrar else ''}
                    </div>
                    """
                    
                    st.markdown(contenido, unsafe_allow_html=True)

# ============================================================================
# PÁGINAS PRINCIPALES (SOLO LAS MÁS IMPORTANTES)
# ============================================================================
def pagina_malla():
    """Página principal - Malla de turnos"""
    st.markdown("<h1 class='main-header'>📊 Malla de Turnos</h1>", unsafe_allow_html=True)
    
    # Advertencia de Streamlit Cloud
    if IS_STREAMLIT_CLOUD:
        st.warning("""
        ⚠️ **STREAMLIT CLOUD - IMPORTANTE**
        - Los datos se guardan en almacenamiento temporal
        - Exporta regularmente usando la opción de Backup
        - Se crean backups automáticos al guardar
        """)
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        meses = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", 
                "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
        mes_seleccionado = st.selectbox("Mes:", meses, index=st.session_state.mes_actual - 1)
        mes_numero = meses.index(mes_seleccionado) + 1
    
    with col2:
        ano = st.selectbox("Año:", [2026, 2025, 2024, 2027], index=0)
    
    with col3:
        if st.button("🔄 Cargar/Actualizar Malla", use_container_width=True):
            st.session_state.malla_actual = get_malla_turnos(mes_numero, ano)
            st.session_state.mes_actual = mes_numero
            st.session_state.ano_actual = ano
            st.success(f"Malla cargada para {mes_seleccionado} {ano}")
            registrar_log("cargar_malla", f"{mes_seleccionado} {ano}")
            st.rerun()
    
    with col4:
        if not st.session_state.malla_actual.empty:
            csv = st.session_state.malla_actual.to_csv(index=False)
            st.download_button(
                label="📥 Exportar CSV",
                data=csv,
                file_name=f"malla_turnos_{mes_seleccionado}_{ano}.csv",
                mime="text/csv",
                use_container_width=True
            )
    
    mostrar_leyenda()
    
    if st.session_state.malla_actual.empty:
        st.warning("⚠️ No hay malla de turnos cargada. Presiona 'Cargar Malla' para ver los datos.")
    else:
        st.markdown(f"### 📋 Malla de Turnos - {mes_seleccionado} {ano}")
        
        if check_permission("write"):
            st.markdown('<div class="auto-save-notice">💡 Los cambios se guardan automáticamente al salir de la celda</div>', unsafe_allow_html=True)
            
            malla_editable = st.session_state.malla_actual.copy()
            column_config = {}
            day_columns = [col for col in malla_editable.columns if '/' in str(col)]
            opciones_codigos = list(st.session_state.codigos_turno.keys())
            if "" in opciones_codigos:
                opciones_codigos.remove("")
            
            for col in malla_editable.columns:
                if col in day_columns:
                    column_config[col] = st.column_config.SelectboxColumn(
                        col,
                        width="small",
                        options=[""] + opciones_codigos,
                        help="Selecciona el código del turno"
                    )
                elif col in ['N°', 'CC']:
                    column_config[col] = st.column_config.Column(width="small", disabled=True)
                elif col == 'APELLIDOS Y NOMBRES':
                    column_config[col] = st.column_config.Column(width="medium", disabled=True)
                elif col in ['CARGO', 'DEPARTAMENTO', 'ESTADO']:
                    column_config[col] = st.column_config.Column(disabled=True)
            
            edited_df = st.data_editor(
                malla_editable,
                column_config=column_config,
                hide_index=True,
                use_container_width=True,
                height=600,
                num_rows="fixed",
                key=f"editor_malla_{mes_numero}_{ano}"
            )
            
            st.markdown("---")
            st.markdown("### 💾 Acciones de Guardado")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.button("💾 Guardar Cambios Ahora", use_container_width=True, type="primary"):
                    with st.spinner("Guardando cambios..."):
                        try:
                            cambios = guardar_malla_turnos_con_backup(edited_df, mes_numero, ano)
                            
                            if cambios > 0:
                                st.session_state.last_save = obtener_hora_colombia()
                                st.session_state.malla_actual = get_malla_turnos(mes_numero, ano)
                                
                                st.success(f"✅ {cambios} cambios guardados exitosamente!")
                                registrar_log("guardar_malla", f"{mes_seleccionado} {ano} - {cambios} cambios")
                                st.rerun()
                            else:
                                st.warning("⚠️ No se detectaron cambios para guardar")
                                
                        except Exception as e:
                            st.error(f"❌ Error al guardar: {str(e)}")
            
            with col2:
                if st.button("🔄 Recargar desde BD", use_container_width=True):
                    st.session_state.malla_actual = get_malla_turnos(mes_numero, ano)
                    st.success("✅ Malla recargada desde base de datos")
                    st.rerun()
            
            with col3:
                if st.button("🗑️ Limpiar Todos", use_container_width=True, type="secondary"):
                    if st.checkbox("¿Confirmar que quieres limpiar TODOS los turnos de este mes?"):
                        malla_vacia = edited_df.copy()
                        for col in day_columns:
                            malla_vacia[col] = ""
                        
                        cambios = guardar_malla_turnos_con_backup(malla_vacia, mes_numero, ano)
                        st.session_state.malla_actual = get_malla_turnos(mes_numero, ano)
                        st.success(f"✅ {cambios} turnos limpiados")
                        st.rerun()
            
        else:
            st.info("👁️ Vista de solo lectura - No puedes editar")
            styled_df = aplicar_estilo_dataframe(st.session_state.malla_actual)
            st.dataframe(styled_df, use_container_width=True, height=600)

def pagina_backup():
    """Página completa de backup y restauración"""
    st.markdown("<h1 class='main-header'>📦 Sistema de Backup y Restauración</h1>", unsafe_allow_html=True)
    
    # Información importante para Streamlit Cloud
    if IS_STREAMLIT_CLOUD:
        st.markdown("""
        <div class="streamlit-cloud-warning">
        ⚠️ **INFORMACIÓN IMPORTANTE - STREAMLIT CLOUD**
        
        **Cómo funciona el almacenamiento:**
        1. Los datos se guardan en almacenamiento temporal del servidor
        2. Se mantienen mientras la app esté activa
        3. Pueden borrarse después de ~24h de inactividad
        
        **Recomendaciones:**
        - ✅ Exporta tus datos regularmente (JSON o CSV)
        - ✅ Descarga backups frecuentemente
        - ✅ Mantén la app activa usándola diariamente
        - ❌ No confíes solo en el almacenamiento temporal
        </div>
        """, unsafe_allow_html=True)
    
    tab1, tab2 = st.tabs(["🗄️ Backups DB", "📄 Exportar/Importar JSON"])
    
    with tab1:
        st.markdown("### 🗄️ Backups de Base de Datos")
        
        backups = sorted(BACKUP_DIR.glob("turnos_backup_*.db"), key=os.path.getmtime, reverse=True)
        
        if backups:
            st.markdown(f"**📊 Total de backups:** {len(backups)}")
            
            backup_data = []
            for backup in backups:
                size_mb = os.path.getsize(backup) / (1024 * 1024)
                mod_time = datetime.fromtimestamp(os.path.getmtime(backup))
                backup_data.append({
                    "Archivo": backup.name,
                    "Tamaño (MB)": f"{size_mb:.2f}",
                    "Fecha": mod_time.strftime("%Y-%m-%d %H:%M:%S")
                })
            
            df_backups = pd.DataFrame(backup_data)
            st.dataframe(
                df_backups[["Archivo", "Tamaño (MB)", "Fecha"]],
                hide_index=True,
                use_container_width=True
            )
            
            st.markdown("### 🔄 Restaurar desde Backup")
            backup_opciones = [b.name for b in backups]
            selected_backup = st.selectbox("Seleccionar backup para restaurar", backup_opciones)
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("🔄 Restaurar este Backup", use_container_width=True):
                    if selected_backup:
                        backup_path = BACKUP_DIR / selected_backup
                        if restaurar_backup(backup_path):
                            st.success("✅ Base de datos restaurada correctamente")
                            st.rerun()
            
            with col2:
                if st.button("🗑️ Eliminar Backups Antiguos", use_container_width=True):
                    max_backups = st.session_state.configuracion.get('max_backups', 5)
                    if len(backups) > max_backups:
                        eliminados = 0
                        for old_backup in backups[max_backups:]:
                            try:
                                old_backup.unlink()
                                eliminados += 1
                            except:
                                pass
                        st.success(f"✅ Eliminados {eliminados} backups antiguos")
                        st.rerun()
                    else:
                        st.info(f"✅ Ya solo hay {len(backups)} backups (máximo: {max_backups})")
        
        else:
            st.warning("No hay backups disponibles.")
        
        st.markdown("---")
        st.markdown("### 💾 Crear Backup Manual")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("📦 Crear Backup Ahora", use_container_width=True):
                backup_file = crear_backup_automatico()
                if backup_file:
                    st.success(f"✅ Backup creado: {backup_file.name}")
                    st.rerun()
        
        with col2:
            if os.path.exists(DB_NAME):
                with open(DB_NAME, "rb") as f:
                    db_bytes = f.read()
                
                st.download_button(
                    label="📥 Descargar DB Actual",
                    data=db_bytes,
                    file_name=f"turnos_db_{obtener_hora_colombia().strftime('%Y%m%d_%H%M%S')}.db",
                    mime="application/octet-stream",
                    use_container_width=True
                )
    
    with tab2:
        st.markdown("### 📄 Exportar/Importar JSON")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 📤 Exportar a JSON")
            st.markdown("Exporta todos los datos a un archivo JSON portable.")
            
            if st.button("🔄 Generar JSON de Exportación", use_container_width=True):
                json_data = exportar_backup_json()
                
                if json_data:
                    # Mostrar información del backup
                    datos = json.loads(json_data)
                    
                    col_info1, col_info2 = st.columns(2)
                    with col_info1:
                        st.metric("Empleados", len(datos.get('empleados', [])))
                        st.metric("Turnos", len(datos.get('malla_turnos', [])))
                    with col_info2:
                        st.metric("Usuarios", len(datos.get('usuarios', [])))
                        st.metric("Códigos", len(datos.get('codigos_turno', [])))
                    
                    st.download_button(
                        label="📥 Descargar JSON Completo",
                        data=json_data,
                        file_name=f"turnos_backup_{obtener_hora_colombia().strftime('%Y%m%d_%H%M%S')}.json",
                        mime="application/json",
                        use_container_width=True
                    )
                else:
                    st.error("Error al exportar datos")
        
        with col2:
            st.markdown("#### 📥 Importar desde JSON")
            st.markdown("Importa datos desde un archivo JSON de backup.")
            
            st.warning("""
            ⚠️ **ADVERTENCIA:** 
            - Esta acción SOBREESCRIBIRÁ todos los datos actuales
            - Se creará un backup automático antes de importar
            """)
            
            uploaded_file = st.file_uploader("Seleccionar archivo JSON", type=['json'])
            
            if uploaded_file is not None:
                try:
                    json_str = uploaded_file.getvalue().decode('utf-8')
                    
                    if st.button("🚀 Importar Datos", use_container_width=True, type="primary"):
                        with st.spinner("Importando datos..."):
                            if importar_backup_json(json_str):
                                st.success("✅ Datos importados correctamente")
                                st.info("🔄 La página se recargará en 3 segundos...")
                                time.sleep(3)
                                st.rerun()
                            else:
                                st.error("❌ Error al importar datos")
                                
                except Exception as e:
                    st.error(f"❌ Error al leer el archivo: {str(e)}")

def exportar_backup_json():
    """Exportar todos los datos a JSON"""
    try:
        datos = {
            'empleados': get_empleados().to_dict('records'),
            'codigos_turno': pd.read_sql("SELECT * FROM codigos_turno", get_connection()).to_dict('records'),
            'usuarios': get_usuarios().to_dict('records'),
            'malla_turnos': pd.read_sql("SELECT * FROM malla_turnos", get_connection()).to_dict('records'),
            'configuracion': get_configuracion(),
            'export_date': obtener_hora_colombia().isoformat(),
            'version': '2.0',
            'streamlit_cloud': IS_STREAMLIT_CLOUD
        }
        
        return json.dumps(datos, indent=2, ensure_ascii=False)
        
    except Exception as e:
        print(f"❌ Error al exportar JSON: {str(e)}")
        return None

def importar_backup_json(json_str):
    """Importar datos desde JSON"""
    try:
        datos = json.loads(json_str)
        
        # Crear backup antes de importar
        crear_backup_automatico()
        
        conn = get_connection()
        cursor = conn.cursor()
        
        # Limpiar tablas
        cursor.execute("DELETE FROM malla_turnos")
        cursor.execute("DELETE FROM empleados")
        cursor.execute("DELETE FROM codigos_turno")
        cursor.execute("DELETE FROM usuarios")
        
        if 'empleados' in datos:
            for emp in datos['empleados']:
                cursor.execute('''
                    INSERT INTO empleados 
                    (id, numero, cargo, nombre_completo, cedula, departamento, estado, hora_inicio, hora_fin)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    emp.get('id'),
                    emp.get('numero'),
                    emp.get('cargo'),
                    emp.get('nombre_completo'),
                    emp.get('cedula'),
                    emp.get('departamento'),
                    emp.get('estado'),
                    emp.get('hora_inicio'),
                    emp.get('hora_fin')
                ))
        
        if 'codigos_turno' in datos:
            for codigo in datos['codigos_turno']:
                cursor.execute('''
                    INSERT INTO codigos_turno (codigo, nombre, color, horas)
                    VALUES (?, ?, ?, ?)
                ''', (
                    codigo.get('codigo'),
                    codigo.get('nombre'),
                    codigo.get('color'),
                    codigo.get('horas')
                ))
        
        if 'usuarios' in datos:
            for user in datos['usuarios']:
                password_hash = user.get('password_hash')
                if not password_hash:
                    # Si no hay hash, crear uno temporal
                    password_hash = hashlib.sha256("temp123".encode()).hexdigest()
                
                cursor.execute('''
                    INSERT INTO usuarios (username, password_hash, role, nombre, departamento)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    user.get('username'),
                    password_hash,
                    user.get('role'),
                    user.get('nombre'),
                    user.get('departamento', 'Administración')
                ))
        
        if 'malla_turnos' in datos:
            for turno in datos['malla_turnos']:
                cursor.execute('''
                    INSERT INTO malla_turnos (empleado_id, mes, ano, dia, codigo_turno)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    turno.get('empleado_id'),
                    turno.get('mes'),
                    turno.get('ano'),
                    turno.get('dia'),
                    turno.get('codigo_turno')
                ))
        
        conn.commit()
        conn.close()
        
        # Actualizar session state
        st.session_state.empleados_df = get_empleados()
        st.session_state.codigos_turno = get_codigos_turno()
        st.session_state.configuracion = get_configuracion()
        
        registrar_log("importar_json", "Datos importados desde JSON")
        return True
        
    except Exception as e:
        print(f"❌ Error al importar JSON: {str(e)}")
        return False

# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================
def pagina_empleados():
    """Página de gestión de empleados"""
    if not check_permission("write"):
        st.error("⛔ No tienes permisos para gestionar empleados")
        return
    
    st.markdown("<h1 class='main-header'>👥 Gestión de Empleados</h1>", unsafe_allow_html=True)
    
    # Advertencia de Streamlit Cloud
    if IS_STREAMLIT_CLOUD:
        st.warning("""
        ⚠️ **STREAMLIT CLOUD**
        - Exporta la lista de empleados regularmente
        - Los datos se guardan automáticamente en backups
        """)
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Empleados", len(st.session_state.empleados_df))
    with col2:
        activos = st.session_state.empleados_df[st.session_state.empleados_df['estado'] == 'Activo'].shape[0]
        st.metric("Activos", activos)
    with col3:
        vacaciones = st.session_state.empleados_df[st.session_state.empleados_df['estado'] == 'Vacaciones'].shape[0]
        st.metric("Vacaciones", vacaciones)
    with col4:
        departamentos = st.session_state.empleados_df['departamento'].nunique()
        st.metric("Departamentos", departamentos)
    
    # Agregar nuevo empleado
    st.markdown("### ➕ Agregar Nuevo Empleado")
    with st.expander("Click para expandir", expanded=False):
        if agregar_empleado():
            st.rerun()
    
    st.markdown("---")
    st.markdown("### 📋 Lista de Empleados")
    
    if st.session_state.empleados_df.empty:
        st.warning("No hay empleados registrados.")
    else:
        df_editable = st.session_state.empleados_df.copy()
        
        df_display = df_editable.rename(columns={
            'id': 'ID_OCULTO',
            'numero': 'N°',
            'cargo': 'CARGO',
            'nombre_completo': 'APELLIDOS Y NOMBRES',
            'cedula': 'CC',
            'departamento': 'DEPARTAMENTO',
            'estado': 'ESTADO',
            'hora_inicio': 'HORA_INICIO',
            'hora_fin': 'HORA_FIN',
            'created_at': 'FECHA_REGISTRO'
        })
        
        column_order = ['N°', 'CARGO', 'APELLIDOS Y NOMBRES', 'CC', 'DEPARTAMENTO', 
                       'ESTADO', 'HORA_INICIO', 'HORA_FIN', 'FECHA_REGISTRO']
        
        column_config = {
            "N°": st.column_config.NumberColumn("N°", width="small", disabled=True),
            "CARGO": st.column_config.TextColumn("Cargo", width="medium"),
            "APELLIDOS Y NOMBRES": st.column_config.TextColumn("Nombre", width="large"),
            "CC": st.column_config.TextColumn("Cédula", width="medium"),
            "DEPARTAMENTO": st.column_config.SelectboxColumn(
                "Departamento",
                options=st.session_state.configuracion['departamentos']
            ),
            "ESTADO": st.column_config.SelectboxColumn(
                "Estado",
                options=["Activo", "Vacaciones", "Licencia", "Inactivo"]
            ),
            "HORA_INICIO": st.column_config.TextColumn("Hora Inicio", width="small"),
            "HORA_FIN": st.column_config.TextColumn("Hora Fin", width="small"),
            "FECHA_REGISTRO": st.column_config.DatetimeColumn("Fecha Registro", disabled=True),
            "ID_OCULTO": st.column_config.NumberColumn("ID", disabled=True, width="small")
        }
        
        edited_df = st.data_editor(
            df_display[column_order + ['ID_OCULTO']],
            column_config=column_config,
            hide_index=True,
            use_container_width=True,
            num_rows="fixed",
            key="editor_empleados"
        )
        
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("💾 Guardar Cambios", use_container_width=True, key="btn_guardar_empleados"):
                try:
                    cambios = guardar_empleados(edited_df)
                    if cambios > 0:
                        st.success(f"✅ {cambios} cambios guardados correctamente")
                        # Crear backup automático
                        crear_backup_automatico()
                        st.info("📦 Backup automático creado")
                        st.rerun()
                    else:
                        st.warning("⚠️ No se realizaron cambios")
                except Exception as e:
                    st.error(f"❌ Error al guardar: {str(e)}")
        
        with col2:
            if st.button("🔄 Recargar desde BD", use_container_width=True, key="btn_recargar_empleados"):
                st.session_state.empleados_df = get_empleados()
                st.success("✅ Datos recargados desde base de datos")
                st.rerun()
        
        with col3:
            csv = df_display[column_order].to_csv(index=False)
            st.download_button(
                label="📥 Exportar CSV",
                data=csv,
                file_name="empleados.csv",
                mime="text/csv",
                use_container_width=True
            )

def agregar_empleado():
    """Agregar nuevo empleado a la base de datos"""
    with st.form("form_nuevo_empleado", clear_on_submit=True):
        st.markdown("#### 📝 Nuevo Empleado")
        
        col1, col2 = st.columns(2)
        
        with col1:
            cargo = st.text_input("Cargo*", placeholder="Ej: JEFE DE TIENDA", key="cargo_nuevo")
            nombre = st.text_input("Apellidos y Nombres*", placeholder="Ej: GARCIA JUAN", key="nombre_nuevo")
            cc = st.text_input("Cédula de Ciudadanía*", placeholder="Ej: 1234567890", key="cc_nuevo")
        
        with col2:
            departamento = st.selectbox("Departamento*", 
                                       st.session_state.configuracion['departamentos'],
                                       key="depto_nuevo")
            estado = st.selectbox("Estado*", 
                                 ["Activo", "Vacaciones", "Licencia", "Inactivo"],
                                 key="estado_nuevo")
            
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(numero) FROM empleados")
            max_num = cursor.fetchone()[0]
            nuevo_numero = (max_num or 0) + 1
            conn.close()
            
            st.info(f"**Número asignado:** {nuevo_numero}")
        
        col3, col4 = st.columns(2)
        with col3:
            hora_inicio = st.text_input("Hora Inicio", placeholder="Ej: 06:00", key="hora_ini_nuevo")
        with col4:
            hora_fin = st.text_input("Hora Fin", placeholder="Ej: 14:00", key="hora_fin_nuevo")
        
        submitted = st.form_submit_button("💾 Guardar Empleado", use_container_width=True)
        
        if submitted:
            if not all([cargo.strip(), nombre.strip(), cc.strip(), departamento]):
                st.error("❌ Por favor complete todos los campos obligatorios (*)")
                return False
            
            if not cc.strip().isdigit():
                st.error("❌ La cédula debe contener solo números")
                return False
            
            conn = get_connection()
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM empleados WHERE cedula = ?", (cc.strip(),))
            if cursor.fetchone()[0] > 0:
                st.error("❌ Ya existe un empleado con esta cédula")
                conn.close()
                return False
            
            cursor.execute('''
                INSERT INTO empleados 
                (numero, cargo, nombre_completo, cedula, departamento, estado, hora_inicio, hora_fin)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                nuevo_numero,
                cargo.upper().strip(),
                nombre.upper().strip(),
                cc.strip(),
                departamento,
                estado,
                hora_inicio.strip() if hora_inicio.strip() else None,
                hora_fin.strip() if hora_fin.strip() else None
            ))
            
            conn.commit()
            conn.close()
            
            st.success(f"✅ Empleado {nombre.upper()} agregado correctamente")
            registrar_log("agregar_empleado", f"{nombre.upper()} - {cargo} - CC: {cc}")
            st.session_state.empleados_df = get_empleados()
            
            # Crear backup después de agregar empleado
            crear_backup_automatico()
            
            return True
    
    return False

def pagina_configuracion():
    """Página de configuración"""
    if not check_permission("configure"):
        st.error("⛔ No tienes permisos para acceder a la configuración")
        return
    
    st.markdown("<h1 class='main-header'>⚙️ Configuración</h1>", unsafe_allow_html=True)
    
    tab1, tab2 = st.tabs(["Códigos de Turno", "General"])
    
    with tab1:
        st.markdown("### Configurar Códigos de Turno")
        
        conn = get_connection()
        codigos_df = pd.read_sql("SELECT * FROM codigos_turno ORDER BY codigo", conn)
        conn.close()
        
        # Mostrar información sobre los códigos actuales
        st.markdown(f"**📊 Total de códigos:** {len(codigos_df)}")
        
        column_config = {
            "codigo": st.column_config.TextColumn(
                "Código", 
                width="small", 
                required=True,
                help="Código único (ej: 20, 15, VC, CP)"
            ),
            "nombre": st.column_config.TextColumn(
                "Descripción", 
                width="medium", 
                required=True,
                help="Descripción del turno (ej: 10 AM - 7 PM)"
            ),
            "color": st.column_config.TextColumn(
                "Color (HEX)",
                width="small",
                help="Color en formato HEX (#RRGGBB)",
                required=True
            ),
            "horas": st.column_config.NumberColumn(
                "Horas", 
                min_value=0, 
                max_value=24, 
                required=True,
                help="Duración en horas (0 para días libres)"
            )
        }
        
        # Botón para agregar fila vacía antes del editor
        col_btn1, col_btn2 = st.columns(2)
        with col_btn1:
            if st.button("➕ Agregar Nuevo Código", key="btn_agregar_codigo", use_container_width=True):
                # Agregar una fila vacía al DataFrame
                nueva_fila = pd.DataFrame([{
                    'codigo': '',
                    'nombre': '',
                    'color': '#FFFFFF',
                    'horas': 0
                }])
                codigos_df = pd.concat([codigos_df, nueva_fila], ignore_index=True)
                st.session_state.codigos_temp = codigos_df
                st.rerun()
        
        with col_btn2:
            if st.button("🎨 Ver Colores Actuales", key="btn_ver_colores", use_container_width=True):
                st.info("**Colores actuales en uso:**")
                for idx, row in codigos_df.iterrows():
                    st.markdown(f"""
                    <div style="display: flex; align-items: center; margin: 5px 0;">
                        <div style="width: 20px; height: 20px; background-color: {row['color']}; 
                                 margin-right: 10px; border: 1px solid #ccc;"></div>
                        <span><strong>{row['codigo']}</strong>: {row['nombre']} - {row['color']}</span>
                    </div>
                    """, unsafe_allow_html=True)
        
        # Usar el DataFrame temporal si existe
        if 'codigos_temp' in st.session_state:
            df_editor = st.session_state.codigos_temp
        else:
            df_editor = codigos_df
        
        edited_codigos = st.data_editor(
            df_editor,
            column_config=column_config,
            num_rows="dynamic",  # Esto permite agregar filas
            use_container_width=True,
            key="editor_codigos"
        )
        
        st.markdown("""
        **💡 Tip para colores:** Usa formato HEX (#RRGGBB) o nombres CSS. 
        Ejemplos: `#FF0000` (rojo), `#00FF00` (verde), `#0000FF` (azul), 
        `#FFFF00` (amarillo), `#FF00FF` (magenta), `#00FFFF` (cian)
        """)
        
        st.markdown("---")
        st.markdown("### 💾 Acciones")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if st.button("💾 Guardar Códigos", use_container_width=True, type="primary"):
                try:
                    # Validar que no haya códigos duplicados
                    codigos_lista = []
                    for codigo in edited_codigos['codigo']:
                        if pd.notna(codigo):
                            codigo_str = str(codigo).strip().upper()
                            if codigo_str:
                                codigos_lista.append(codigo_str)
                    
                    if len(codigos_lista) != len(set(codigos_lista)):
                        st.error("❌ Hay códigos duplicados. Cada código debe ser único.")
                        return
                    
                    # Validar que no haya celdas vacías en campos requeridos
                    for idx, row in edited_codigos.iterrows():
                        if pd.isna(row['codigo']) or str(row['codigo']).strip() == '':
                            st.error(f"❌ Fila {idx+1}: El campo 'Código' no puede estar vacío.")
                            return
                        if pd.isna(row['nombre']) or str(row['nombre']).strip() == '':
                            st.error(f"❌ Fila {idx+1}: El campo 'Descripción' no puede estar vacío.")
                            return
                        if pd.isna(row['color']) or str(row['color']).strip() == '':
                            st.error(f"❌ Fila {idx+1}: El campo 'Color' no puede estar vacío.")
                            return
                    
                    conn = get_connection()
                    cursor = conn.cursor()
                    
                    # Limpiar la tabla
                    cursor.execute("DELETE FROM codigos_turno")
                    
                    # Insertar todos los códigos
                    for idx, row in edited_codigos.iterrows():
                        if pd.notna(row['codigo']) and pd.notna(row['nombre']):
                            # Asegurar que el color tenga formato HEX
                            color = str(row['color']).strip()
                            if not color.startswith('#'):
                                color = '#' + color
                            
                            # Asegurar formato HEX válido
                            if len(color) == 4:  # Formato corto #RGB
                                color = f'#{color[1]}{color[1]}{color[2]}{color[2]}{color[3]}{color[3]}'
                            elif len(color) == 7:  # Formato largo #RRGGBB
                                pass  # Ya está bien
                            else:
                                st.error(f"❌ Fila {idx+1}: Formato de color inválido: {color}")
                                conn.close()
                                return
                            
                            cursor.execute(
                                "INSERT INTO codigos_turno (codigo, nombre, color, horas) VALUES (?, ?, ?, ?)",
                                (
                                    str(row['codigo']).strip().upper(),
                                    str(row['nombre']).strip(),
                                    color,
                                    int(row['horas']) if pd.notna(row['horas']) else 0
                                )
                            )
                    
                    conn.commit()
                    conn.close()
                    
                    # Actualizar session state
                    st.session_state.codigos_turno = get_codigos_turno()
                    
                    # Limpiar DataFrame temporal
                    if 'codigos_temp' in st.session_state:
                        del st.session_state.codigos_temp
                    
                    st.success(f"✅ {len(edited_codigos)} códigos guardados exitosamente")
                    
                    # Crear backup automático
                    crear_backup_automatico()
                    
                    st.info("🔄 La página se recargará en 3 segundos...")
                    time.sleep(3)
                    st.rerun()
                    
                except ValueError as ve:
                    st.error(f"❌ Error de valor: {str(ve)}")
                except Exception as e:
                    st.error(f"❌ Error al guardar: {str(e)}")
                    st.error("Verifica que todos los campos estén completos y en el formato correcto.")
        
        with col2:
            if st.button("🔄 Recargar Códigos", use_container_width=True):
                # Limpiar DataFrame temporal
                if 'codigos_temp' in st.session_state:
                    del st.session_state.codigos_temp
                st.success("✅ Códigos recargados desde base de datos")
                st.rerun()
        
        with col3:
            if st.button("🔙 Restaurar Default", use_container_width=True):
                st.warning("⚠️ Esto reemplazará todos los códigos actuales por los valores predeterminados.")
                if st.checkbox("Confirmo que quiero restaurar los valores predeterminados"):
                    try:
                        conn = get_connection()
                        cursor = conn.cursor()
                        
                        cursor.execute("DELETE FROM codigos_turno")
                        
                        codigos_default = [
                            ("20", "10 AM - 7 PM", "#FF6B6B", 8),
                            ("15", "8 AM - 5 PM", "#4ECDC4", 8),
                            ("70", "9:00 AM - 7:30 PM", "#FFD166", 9),
                            ("155", "11 AM - 7 PM", "#06D6A0", 7),
                            ("151", "8 AM - 4 PM", "#118AB2", 7),
                            ("177", "1:30 PM - 8:30 PM", "#EF476F", 7),
                            ("149", "7 AM - 3 PM", "#073B4C", 7),
                            ("26", "11 AM - 8:30 PM", "#7209B7", 9),
                            ("158", "12:30 PM - 8:30 PM", "#F15BB5", 10),
                            ("214", "1 PM - 8:30 PM", "#00BBF9", 8),
                            ("VC", "Vacaciones", "#9B5DE5", 0),
                            ("CP", "Cumpleaños", "#00F5D4", 0),
                            ("PA", "Permiso Administrativo", "#FF9E00", 0),
                            ("-1", "Ausente", "#E0E0E0", 0)
                        ]
                        
                        cursor.executemany(
                            "INSERT INTO codigos_turno (codigo, nombre, color, horas) VALUES (?, ?, ?, ?)",
                            codigos_default
                        )
                        
                        conn.commit()
                        conn.close()
                        
                        # Actualizar session state
                        st.session_state.codigos_turno = get_codigos_turno()
                        
                        # Limpiar DataFrame temporal
                        if 'codigos_temp' in st.session_state:
                            del st.session_state.codigos_temp
                        
                        st.success("✅ Valores por defecto restaurados")
                        crear_backup_automatico()
                        st.info("🔄 La página se recargará en 3 segundos...")
                        time.sleep(3)
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"❌ Error al restaurar: {str(e)}")
        
        with col4:
            if not edited_codigos.empty:
                csv = edited_codigos.to_csv(index=False)
                st.download_button(
                    label="📥 Exportar CSV",
                    data=csv,
                    file_name="codigos_turno.csv",
                    mime="text/csv",
                    use_container_width=True
                )
    
    with tab2:
        st.markdown("### Configuración General")
        
        config = st.session_state.configuracion
        
        col1, col2 = st.columns(2)
        
        with col1:
            formato_hora = st.selectbox("Formato de hora", ["24 horas", "12 horas (AM/PM)"], 
                                      index=0 if config.get('formato_hora') == '24 horas' else 1)
            dias_vacaciones = st.number_input("Días de vacaciones por año", min_value=0, max_value=30, 
                                            value=config.get('dias_vacaciones', 15))
        
        with col2:
            inicio_semana = st.selectbox("Inicio de semana", ["Lunes", "Domingo"], 
                                       index=0 if config.get('inicio_semana') == 'Lunes' else 1)
            departamentos_text = st.text_area(
                "Departamentos (separados por comas)",
                value=",".join(config.get('departamentos', []))
            )
        
        # Configuración específica para Streamlit Cloud
        if IS_STREAMLIT_CLOUD:
            st.markdown("### ☁️ Configuración Streamlit Cloud")
            
            col_cloud1, col_cloud2 = st.columns(2)
            with col_cloud1:
                auto_backup = st.checkbox("Backup automático al guardar", 
                                        value=config.get('auto_backup', True))
            with col_cloud2:
                max_backups = st.number_input("Máximo de backups a mantener", 
                                            min_value=1, max_value=10, 
                                            value=config.get('max_backups', 5))
        
        if st.button("💾 Guardar Configuración", use_container_width=True, type="primary"):
            try:
                conn = get_connection()
                cursor = conn.cursor()
                
                # Actualizar configuración básica
                updates = [
                    ("formato_hora", formato_hora, "text"),
                    ("dias_vacaciones", str(dias_vacaciones), "number"),
                    ("inicio_semana", inicio_semana, "text"),
                    ("departamentos", departamentos_text, "list")
                ]
                
                # Agregar configuración de Streamlit Cloud si aplica
                if IS_STREAMLIT_CLOUD:
                    updates.append(("auto_backup", "1" if auto_backup else "0", "boolean"))
                    updates.append(("max_backups", str(max_backups), "number"))
                
                for clave, valor, tipo in updates:
                    cursor.execute('''
                        INSERT OR REPLACE INTO configuracion (clave, valor, tipo, descripcion)
                        VALUES (?, ?, ?, ?)
                    ''', (clave, valor, tipo, f"Configuración de {clave}"))
                
                conn.commit()
                conn.close()
                
                st.session_state.configuracion = get_configuracion()
                st.success("✅ Configuración guardada correctamente")
                crear_backup_automatico()
                st.rerun()
                
            except Exception as e:
                st.error(f"❌ Error al guardar configuración: {str(e)}")

def pagina_usuarios():
    """Página de gestión de usuarios para administradores"""
    if not check_permission("manage_users"):
        st.error("⛔ No tienes permisos para gestionar usuarios")
        return
    
    st.markdown("<h1 class='main-header'>👑 Gestión de Usuarios</h1>", unsafe_allow_html=True)
    
    st.markdown("### 📋 Usuarios del Sistema")
    
    usuarios_df = get_usuarios()
    
    if usuarios_df.empty:
        st.warning("No hay usuarios registrados en el sistema.")
    else:
        df_editable = usuarios_df.copy()
        
        if 'password_hash' in df_editable.columns:
            df_display = df_editable.drop(columns=['password_hash'])
        else:
            df_display = df_editable
        
        df_display = df_display.rename(columns={
            'username': 'USUARIO',
            'nombre': 'NOMBRE_COMPLETO',
            'role': 'ROL',
            'departamento': 'DEPARTAMENTO',
            'created_at': 'FECHA_CREACION'
        })
        
        column_config = {
            "USUARIO": st.column_config.TextColumn("Usuario", width="small", required=True),
            "NOMBRE_COMPLETO": st.column_config.TextColumn("Nombre", width="medium", required=True),
            "ROL": st.column_config.SelectboxColumn(
                "Rol",
                options=list(ROLES.keys()),
                width="small",
                required=True
            ),
            "DEPARTAMENTO": st.column_config.SelectboxColumn(
                "Departamento",
                options=st.session_state.configuracion['departamentos'],
                width="medium"
            ),
            "FECHA_CREACION": st.column_config.DatetimeColumn("Fecha Creación", disabled=True)
        }
        
        edited_df = st.data_editor(
            df_display,
            column_config=column_config,
            hide_index=True,
            use_container_width=True,
            num_rows="fixed",
            key="editor_usuarios"
        )
        
        if st.button("💾 Guardar Cambios de Usuarios", use_container_width=True):
            try:
                cambios = guardar_usuarios(edited_df, usuarios_df)
                if cambios > 0:
                    st.success(f"✅ {cambios} usuarios actualizados correctamente")
                    crear_backup_automatico()
                    st.rerun()
                else:
                    st.warning("⚠️ No se realizaron cambios")
            except Exception as e:
                st.error(f"❌ Error al guardar usuarios: {str(e)}")
    
    st.markdown("---")
    st.markdown("### ➕ Crear Nuevo Usuario")
    
    with st.form("form_nuevo_usuario", clear_on_submit=True):
        col1, col2 = st.columns(2)
        
        with col1:
            nuevo_username = st.text_input("Usuario*", placeholder="Ej: juan.perez")
            nuevo_nombre = st.text_input("Nombre Completo*", placeholder="Ej: Juan Pérez García")
        
        with col2:
            nuevo_rol = st.selectbox("Rol*", list(ROLES.keys()))
            nuevo_depto = st.selectbox("Departamento", st.session_state.configuracion['departamentos'])
        
        st.markdown("**Contraseña**")
        col3, col4 = st.columns(2)
        with col3:
            nueva_password = st.text_input("Contraseña*", type="password", placeholder="Mínimo 6 caracteres")
        with col4:
            confirm_password = st.text_input("Confirmar Contraseña*", type="password")
        
        submitted = st.form_submit_button("👑 Crear Nuevo Usuario", use_container_width=True)
        
        if submitted:
            if crear_nuevo_usuario(nuevo_username, nueva_password, confirm_password, nuevo_nombre, nuevo_rol, nuevo_depto):
                st.rerun()

def crear_nuevo_usuario(username, password, confirm_password, nombre, rol, departamento):
    """Crear nuevo usuario"""
    if not all([username.strip(), password, confirm_password, nombre.strip(), rol]):
        st.error("❌ Por favor complete todos los campos obligatorios (*)")
        return False
    
    if len(password) < 6:
        st.error("❌ La contraseña debe tener al menos 6 caracteres")
        return False
    
    if password != confirm_password:
        st.error("❌ Las contraseñas no coinciden")
        return False
    
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM usuarios WHERE username = ?", (username.strip(),))
    if cursor.fetchone()[0] > 0:
        st.error("❌ Ya existe un usuario con ese nombre")
        conn.close()
        return False
    
    password_hash = hashlib.sha256(password.encode()).hexdigest()
    
    cursor.execute('''
        INSERT INTO usuarios (username, password_hash, role, nombre, departamento)
        VALUES (?, ?, ?, ?, ?)
    ''', (
        username.strip(),
        password_hash,
        rol,
        nombre.strip(),
        departamento
    ))
    
    conn.commit()
    conn.close()
    
    st.success(f"✅ Usuario {username} creado correctamente")
    registrar_log("crear_usuario", f"Usuario: {username}, Rol: {rol}")
    crear_backup_automatico()
    
    return True

def pagina_mis_turnos():
    """Página para que los empleados vean SUS turnos"""
    st.markdown("<h1 class='main-header'>📅 Mis Turnos</h1>", unsafe_allow_html=True)
    
    if not st.session_state.empleado_actual:
        st.warning("⚠️ No se encontró tu registro como empleado.")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Ir a Mi Información", use_container_width=True):
                st.session_state.current_page = "mi_info"
                st.rerun()
        
        return
    
    empleado_info = st.session_state.empleado_actual
    
    with st.expander("👤 Mi Información", expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            st.write(f"**Nombre:** {empleado_info.get('nombre_completo', 'N/A')}")
            st.write(f"**Cargo:** {empleado_info.get('cargo', 'N/A')}")
        with col2:
            st.write(f"**Departamento:** {empleado_info.get('departamento', 'N/A')}")
            st.write(f"**Estado:** {empleado_info.get('estado', 'N/A')}")
    
    col1, col2 = st.columns(2)
    with col1:
        meses = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", 
                "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
        mes_seleccionado = st.selectbox("Mes:", meses, index=obtener_hora_colombia().month - 1)
        mes_numero = meses.index(mes_seleccionado) + 1
    
    with col2:
        ano = st.selectbox("Año:", [2026, 2025, 2024, 2027], index=0)
    
    if st.button("📅 Cargar Mis Turnos", use_container_width=True, type="primary"):
        try:
            empleado_id = empleado_info.get('id')
            
            if not empleado_id:
                st.error("❌ No se pudo obtener el ID del empleado")
                return
            
            turnos_dict = get_turnos_empleado_mes(empleado_id, mes_numero, ano)
            
            if not turnos_dict:
                st.info(f"ℹ️ No tienes turnos asignados para {mes_seleccionado} {ano}.")
                return
            
            turnos_lista = []
            for dia, codigo in sorted(turnos_dict.items()):
                if codigo:
                    turno_info = st.session_state.codigos_turno.get(str(codigo), {})
                    turnos_lista.append({
                        'Día': f"{dia}/{mes_numero}/{ano}",
                        'Código': codigo,
                        'Turno': turno_info.get('nombre', 'Desconocido'),
                        'Horas': turno_info.get('horas', 0)
                    })
            
            if turnos_lista:
                df_calendario = pd.DataFrame(turnos_lista)
                
                st.markdown(f"### 📋 Mis Turnos - {mes_seleccionado} {ano}")
                st.dataframe(
                    df_calendario[['Día', 'Turno', 'Horas']],
                    hide_index=True,
                    use_container_width=True
                )
                
                st.markdown("---")
                st.markdown("### 📈 Mis Estadísticas")
                
                total_horas = sum(t['Horas'] for t in turnos_lista)
                total_turnos = len(turnos_lista)
                
                num_dias = calendar.monthrange(ano, mes_numero)[1]
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Turnos Asignados", total_turnos)
                with col2:
                    st.metric("Horas Totales", total_horas)
                with col3:
                    promedio = total_horas / max(total_turnos, 1)
                    st.metric("Promedio Horas/Turno", f"{promedio:.1f}")
                with col4:
                    porcentaje = (total_turnos / num_dias * 100) if num_dias > 0 else 0
                    st.metric("Días con Turno", f"{porcentaje:.1f}%")
            else:
                st.info(f"ℹ️ No tienes turnos asignados para {mes_seleccionado} {ano}.")
                
        except Exception as e:
            st.error(f"❌ Error al cargar turnos: {str(e)}")

def pagina_calendario():
    """Página de calendario visual simplificada"""
    st.markdown("<h1 class='main-header'>📆 Mi Calendario</h1>", unsafe_allow_html=True)
    
    nombres_meses = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", 
                    "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
    
    if not st.session_state.empleado_actual:
        st.warning("⚠️ No se encontró tu registro como empleado.")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Ir a Mi Información", use_container_width=True):
                st.session_state.current_page = "mi_info"
                st.rerun()
        
        return
    
    empleado_info = st.session_state.empleado_actual
    
    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        mes_seleccionado = st.selectbox("Mes:", nombres_meses, 
                                       index=st.session_state.get('calendario_mes', obtener_hora_colombia().month) - 1)
        mes_numero = nombres_meses.index(mes_seleccionado) + 1
    
    with col2:
        ano = st.number_input("Año:", min_value=2023, max_value=2030, 
                             value=st.session_state.get('calendario_ano', obtener_hora_colombia().year))
    
    with col3:
        if st.button("📅 Generar Calendario", use_container_width=True, type="primary"):
            st.session_state.calendario_mes = mes_numero
            st.session_state.calendario_ano = ano
            st.rerun()
    
    if 'calendario_mes' in st.session_state and 'calendario_ano' in st.session_state:
        turnos_dict = get_turnos_empleado_mes(empleado_info['id'], mes_numero, ano)
        
        col_info1, col_info2 = st.columns(2)
        with col_info1:
            st.markdown(f"""
            <div class="info-card">
                <h4 style="margin-top: 0;">👤 Mi Información</h4>
                <p><strong>Nombre:</strong> {empleado_info.get('nombre_completo', 'N/A')}</p>
                <p><strong>Cargo:</strong> {empleado_info.get('cargo', 'N/A')}</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col_info2:
            st.markdown(f"""
            <div class="info-card">
                <h4 style="margin-top: 0;">💼 Datos Laborales</h4>
                <p><strong>Departamento:</strong> {empleado_info.get('departamento', 'N/A')}</p>
                <p><strong>Estado:</strong> {empleado_info.get('estado', 'N/A')}</p>
            </div>
            """, unsafe_allow_html=True)
        
        generar_calendario_simple(mes_numero, ano, turnos_dict)

def pagina_mi_info():
    """Página de información personal del empleado"""
    st.markdown("<h1 class='main-header'>👤 Mi Información</h1>", unsafe_allow_html=True)
    
    if not st.session_state.empleado_actual:
        st.warning("⚠️ No se encontró tu registro como empleado.")
        
        with st.expander("🛠️ Solucionar Problema de Asociación", expanded=True):
            st.markdown("""
            ### 🔍 Tu usuario no está asociado a un empleado
            
            **Posibles causas:**
            1. Tu nombre de usuario no coincide exactamente con tu nombre en la lista de empleados
            2. No estás registrado en la base de datos de empleados
            
            **Solución:**
            
            **Opción 1:** Contacta al administrador para que asocie tu usuario correctamente.
            
            **Opción 2:** Busca manualmente tu registro:
            """)
            
            nombre_buscar = st.text_input("Buscar por nombre:", 
                                         placeholder="Ingresa tu nombre o apellido")
            
            if nombre_buscar:
                empleados_df = get_empleados()
                resultados = empleados_df[
                    empleados_df['nombre_completo'].str.contains(nombre_buscar.upper(), case=False, na=False)
                ]
                
                if not resultados.empty:
                    st.success(f"✅ Se encontraron {len(resultados)} resultados:")
                    st.dataframe(resultados[['numero', 'nombre_completo', 'cargo', 'departamento']])
                    
                    opciones = resultados['nombre_completo'].tolist()
                    seleccion = st.selectbox("Selecciona tu nombre:", opciones)
                    
                    if st.button("👤 Usar este registro"):
                        empleado_seleccionado = resultados[resultados['nombre_completo'] == seleccion].iloc[0].to_dict()
                        st.session_state.empleado_actual = empleado_seleccionado
                        st.success("✅ Registro asociado correctamente")
                        st.rerun()
                else:
                    st.warning("No se encontraron empleados con ese nombre.")
        
        return
    
    empleado_info = st.session_state.empleado_actual
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📋 Datos Personales")
        st.markdown(f"""
        <div class="info-card">
            <p><strong>Nombre Completo:</strong> {empleado_info.get('nombre_completo', 'N/A')}</p>
            <p><strong>Cédula:</strong> {empleado_info.get('cedula', 'N/A')}</p>
            <p><strong>Cargo:</strong> {empleado_info.get('cargo', 'N/A')}</p>
            <p><strong>Departamento:</strong> {empleado_info.get('departamento', 'N/A')}</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("### ⚙️ Datos Laborales")
        st.markdown(f"""
        <div class="info-card">
            <p><strong>Estado:</strong> {empleado_info.get('estado', 'N/A')}</p>
            <p><strong>Número:</strong> {empleado_info.get('numero', 'N/A')}</p>
            <p><strong>Horario Base:</strong> {empleado_info.get('hora_inicio', 'N/A')} - {empleado_info.get('hora_fin', 'N/A')}</p>
            <p><strong>Fecha Registro:</strong> {empleado_info.get('created_at', 'N/A')}</p>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    col_nav1, col_nav2 = st.columns(2)
    with col_nav1:
        if st.button("📅 Ver Mis Turnos", use_container_width=True, type="primary"):
            st.session_state.current_page = "mis_turnos"
            st.rerun()
    with col_nav2:
        if st.button("📆 Ver Calendario", use_container_width=True):
            st.session_state.current_page = "calendario"
            st.rerun()

def pagina_info_sistema():
    """Página de información del sistema con hora Colombia"""
    if not check_permission("configure"):
        st.error("⛔ No tienes permisos para ver esta información")
        return
    
    st.markdown("<h1 class='main-header'>🖥️ Información del Sistema</h1>", unsafe_allow_html=True)
    
    hora_colombia = obtener_hora_colombia()
    hora_utc = datetime.now(pytz.utc)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown(f"""
        <div style="background-color: #1E3A8A; color: white; padding: 20px; 
                   border-radius: 10px; text-align: center;">
            <h3>🇨🇴 Colombia</h3>
            <div style="font-size: 1.5em; font-weight: bold;">
                {hora_colombia.strftime('%H:%M:%S')}
            </div>
            <div>{hora_colombia.strftime('%A, %d de %B de %Y')}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div style="background-color: #374151; color: white; padding: 20px; 
                   border-radius: 10px; text-align: center;">
            <h3>🌍 UTC</h3>
            <div style="font-size: 1.5em; font-weight: bold;">
                {hora_utc.strftime('%H:%M:%S')}
            </div>
            <div>{hora_utc.strftime('%A, %d de %B de %Y')}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        diferencia = hora_utc - hora_colombia
        horas_diferencia = diferencia.total_seconds() / 3600
        
        st.markdown(f"""
        <div style="background-color: #0F766E; color: white; padding: 20px; 
                   border-radius: 10px; text-align: center;">
            <h3>⏱️ Diferencia</h3>
            <div style="font-size: 1.5em; font-weight: bold;">
                UTC {horas_diferencia:+.0f}h
            </div>
            <div>Colombia está {abs(horas_diferencia)} horas {'' if horas_diferencia > 0 else 'adelantada'} respecto a UTC</div>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    st.markdown("### 📊 Información del Sistema")
    
    col_info1, col_info2 = st.columns(2)
    
    with col_info1:
        st.markdown(f"""
        <div class="info-card">
            <h4>📅 Configuración Horaria</h4>
            <p><strong>Zona horaria:</strong> America/Bogota</p>
            <p><strong>Código:</strong> COT (Colombia Time)</p>
            <p><strong>UTC offset:</strong> -5 horas</p>
            <p><strong>Horario de verano:</strong> No aplica</p>
            <p><strong>Streamlit Cloud:</strong> {'✅ ACTIVADO' if IS_STREAMLIT_CLOUD else '❌ DESACTIVADO'}</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col_info2:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM empleados")
        num_empleados = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM malla_turnos")
        num_turnos = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM usuarios")
        num_usuarios = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM codigos_turno")
        num_codigos = cursor.fetchone()[0]
        
        conn.close()
        
        st.markdown(f"""
        <div class="info-card">
            <h4>🗄️ Base de Datos</h4>
            <p><strong>Empleados:</strong> {num_empleados}</p>
            <p><strong>Turnos registrados:</strong> {num_turnos}</p>
            <p><strong>Usuarios:</strong> {num_usuarios}</p>
            <p><strong>Códigos de turno:</strong> {num_codigos}</p>
            <p><strong>Ubicación BD:</strong> {DB_NAME}</p>
        </div>
        """, unsafe_allow_html=True)
    
    # Información de backups
    if IS_STREAMLIT_CLOUD:
        st.markdown("---")
        st.markdown("### ☁️ Información de Streamlit Cloud")
        
        backups = list(BACKUP_DIR.glob("turnos_backup_*.db"))
        
        col_backup1, col_backup2 = st.columns(2)
        with col_backup1:
            st.markdown(f"""
            <div class="info-card">
                <h4>📦 Backups</h4>
                <p><strong>Backups disponibles:</strong> {len(backups)}</p>
                <p><strong>Máximo configurado:</strong> {st.session_state.configuracion.get('max_backups', 5)}</p>
                <p><strong>Auto-backup:</strong> {'✅ ACTIVADO' if st.session_state.configuracion.get('auto_backup', True) else '❌ DESACTIVADO'}</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col_backup2:
            if backups:
                ultimo_backup = max(backups, key=os.path.getmtime)
                tamaño_mb = os.path.getsize(ultimo_backup) / (1024 * 1024)
                fecha_mod = datetime.fromtimestamp(os.path.getmtime(ultimo_backup))
                
                st.markdown(f"""
                <div class="info-card">
                    <h4>🕐 Último Backup</h4>
                    <p><strong>Archivo:</strong> {ultimo_backup.name}</p>
                    <p><strong>Tamaño:</strong> {tamaño_mb:.2f} MB</p>
                    <p><strong>Fecha:</strong> {fecha_mod.strftime('%Y-%m-%d %H:%M:%S')}</p>
                </div>
                """, unsafe_allow_html=True)

def main():
    """Función principal que gestiona toda la aplicación"""
    # Inicializar session state (esto maneja la persistencia)
    if 'app_initialized' not in st.session_state:
        inicializar_session_state()
    
    if not st.session_state.auth['is_authenticated']:
        pagina_login()
        return
    
    mostrar_barra_usuario()
    mostrar_sidebar()
    
    if st.session_state.auth['role'] == 'admin':
        monitoreo_sistema()
    
    # Mapeo de páginas disponibles
    paginas_disponibles = {
        "admin": ["malla", "empleados", "config", "usuarios", "backup", "info_sistema"],
        "supervisor": ["malla", "empleados"],
        "empleado": ["mis_turnos", "calendario", "mi_info"]
    }
    
    rol = st.session_state.auth['role']
    pagina_actual = st.session_state.current_page
    
    # Verificar que la página actual sea válida para el rol
    if pagina_actual not in paginas_disponibles.get(rol, []):
        pagina_actual = paginas_disponibles[rol][0]
        st.session_state.current_page = pagina_actual
    
    # Diccionario de funciones de página
    paginas = {
        "malla": pagina_malla,
        "empleados": pagina_empleados,
        "config": pagina_configuracion,
        "usuarios": pagina_usuarios,
        "backup": pagina_backup,
        "mis_turnos": pagina_mis_turnos,
        "calendario": pagina_calendario,
        "mi_info": pagina_mi_info,
        "info_sistema": pagina_info_sistema
    }
    
    # Ejecutar la página actual
    if pagina_actual in paginas:
        paginas[pagina_actual]()
    else:
        # Página por defecto
        pagina_malla()
    
    # Footer
    st.markdown("---")
    hora_colombia = obtener_hora_colombia()
    
    footer_text = f"""
    <div style='text-align: center; color: #6c757d; padding: 20px;'>
    📊 Creado por Edwin Merchán | © 2026 | Versión 2.0 | 
    Hora Colombia: {hora_colombia.strftime('%H:%M')} UTC-5
    """
    
    if IS_STREAMLIT_CLOUD:
        backups = list(BACKUP_DIR.glob("turnos_backup_*.db"))
        footer_text += f" | ☁️ Backups: {len(backups)}"
    
    footer_text += "</div>"
    
    st.markdown(footer_text, unsafe_allow_html=True)
    
    # Auto-backup periódico (solo para admin en Streamlit Cloud)
    if IS_STREAMLIT_CLOUD and rol == "admin":
        # Crear backup automático cada 30 minutos
        if 'last_auto_backup' not in st.session_state:
            st.session_state.last_auto_backup = hora_colombia
        
        tiempo_desde_backup = hora_colombia - st.session_state.last_auto_backup
        if tiempo_desde_backup.total_seconds() > 1800:  # 30 minutos
            crear_backup_automatico()
            st.session_state.last_auto_backup = hora_colombia

# ============================================================================
# EJECUCIÓN
# ============================================================================
if __name__ == "__main__":
    main()