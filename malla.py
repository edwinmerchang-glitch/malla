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
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode
import shutil
import time
import pytz
import tempfile
import sys
import re

# ============================================================================
# CONFIGURACIÓN INICIAL
# ============================================================================
st.set_page_config(
    page_title="Malla de Turnos - Gestión Completa",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Agregar esta línea para mobile
st.markdown("""
<meta name="viewport" content="width=device-width, initial-scale=1.0">
""", unsafe_allow_html=True)

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
# CSS PERSONALIZADO (MOBILE-FIRST RESPONSIVE)
# ============================================================================
# ============================================================================
# CSS PERSONALIZADO (MOBILE-FIRST RESPONSIVE)
# ============================================================================
st.markdown("""
<style>
    /* Sincronización de scroll para tablas divididas */
    .synchronized-scroll {
        overflow-y: scroll;
        overflow-x: hidden;
        scroll-behavior: smooth;
    }
    
    /* Contenedor para tablas sincronizadas */
    .sync-container {
        display: flex;
        overflow: hidden;
        margin-bottom: 10px;
    }
    
    /* Tabla fija (izquierda) */
    .sync-fixed {
        flex: 0 0 35%;
        overflow-y: scroll;
        height: 600px;
        border-right: 2px solid #ddd;
    }
    
    /* Tabla desplazable (derecha) */
    .sync-scrollable {
        flex: 0 0 65%;
        overflow-y: scroll;
        overflow-x: auto;
        height: 600px;
    }
    
    /* Asegurar que las tablas tengan el mismo alto */
    .sync-table-wrapper {
        height: auto;
    }
    
    /* Estilos para el scroll personalizado */
    .sync-container::-webkit-scrollbar,
    .sync-fixed::-webkit-scrollbar,
    .sync-scrollable::-webkit-scrollbar {
        width: 8px;
        height: 8px;
    }
    
    .sync-container::-webkit-scrollbar-track,
    .sync-fixed::-webkit-scrollbar-track,
    .sync-scrollable::-webkit-scrollbar-track {
        background: #f1f1f1;
    }
    
    .sync-container::-webkit-scrollbar-thumb,
    .sync-fixed::-webkit-scrollbar-thumb,
    .sync-scrollable::-webkit-scrollbar-thumb {
        background: #888;
        border-radius: 4px;
    }
    
    .sync-container::-webkit-scrollbar-thumb:hover,
    .sync-fixed::-webkit-scrollbar-thumb:hover,
    .sync-scrollable::-webkit-scrollbar-thumb:hover {
        background: #555;
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
    """Obtener todos los turnos de un empleado para un mes específico - VERSIÓN MEJORADA"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Debug: Verificar que el empleado existe
        cursor.execute('SELECT id, nombre_completo FROM empleados WHERE id = ?', (empleado_id,))
        empleado_data = cursor.fetchone()
        
        if not empleado_data:
            print(f"⚠️ Empleado con ID {empleado_id} no encontrado")
            conn.close()
            return {}
        
        # Obtener turnos con manejo mejorado de valores nulos
        cursor.execute('''
            SELECT dia, codigo_turno 
            FROM malla_turnos 
            WHERE empleado_id = ? AND mes = ? AND ano = ?
            ORDER BY dia
        ''', (empleado_id, mes, ano))
        
        turnos = cursor.fetchall()
        conn.close()
        
        # Crear diccionario con todos los días del mes
        num_dias = calendar.monthrange(ano, mes)[1]
        turnos_dict = {}
        
        for dia in range(1, num_dias + 1):
            turnos_dict[dia] = ""  # Valor por defecto vacío
        
        # Llenar con los datos de la base de datos
        for dia, codigo in turnos:
            if codigo is not None and str(codigo).strip() != '' and str(codigo).strip().lower() != 'nan':
                turnos_dict[int(dia)] = str(codigo).strip()
            else:
                turnos_dict[int(dia)] = ""
        
        return turnos_dict
        
    except Exception as e:
        print(f"❌ Error en get_turnos_empleado_mes: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
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
        
        # Primero, asegurémonos de que las columnas existen
        columnas_necesarias = ['ID_OCULTO', 'CARGO', 'APELLIDOS Y NOMBRES', 'CC', 'DEPARTAMENTO', 'ESTADO', 'HORA_INICIO', 'HORA_FIN']
        for col in columnas_necesarias:
            if col not in df_editado.columns:
                st.error(f"❌ Columna faltante: {col}")
                return 0, []
        
        for idx, row in df_editado.iterrows():
            # Convertir la fila a un diccionario para acceso seguro
            row_dict = row.to_dict() if hasattr(row, 'to_dict') else dict(row)
            
            # Verificar si es una actualización o inserción
            if 'ID_OCULTO' in row_dict and pd.notna(row_dict['ID_OCULTO']) and row_dict['ID_OCULTO'] != '':
                try:
                    # Es una actualización
                    emp_id = int(row_dict['ID_OCULTO'])
                    
                    # Preparar valores, manejando posibles NaN
                    cargo = str(row_dict['CARGO']) if pd.notna(row_dict.get('CARGO', '')) else ''
                    nombre = str(row_dict['APELLIDOS Y NOMBRES']) if pd.notna(row_dict.get('APELLIDOS Y NOMBRES', '')) else ''
                    cc = str(row_dict['CC']) if pd.notna(row_dict.get('CC', '')) else ''
                    departamento = str(row_dict['DEPARTAMENTO']) if pd.notna(row_dict.get('DEPARTAMENTO', '')) else ''
                    estado = str(row_dict['ESTADO']) if pd.notna(row_dict.get('ESTADO', '')) else 'Activo'
                    
                    # Manejar horas (pueden ser NaN)
                    hora_inicio = str(row_dict['HORA_INICIO']) if pd.notna(row_dict.get('HORA_INICIO', '')) else None
                    hora_fin = str(row_dict['HORA_FIN']) if pd.notna(row_dict.get('HORA_FIN', '')) else None
                    
                    # Si hora_inicio es una cadena vacía, convertir a None
                    if hora_inicio == '' or hora_inicio == 'nan':
                        hora_inicio = None
                    if hora_fin == '' or hora_fin == 'nan':
                        hora_fin = None
                    
                    cursor.execute('''
                        UPDATE empleados 
                        SET cargo = ?, nombre_completo = ?, cedula = ?, 
                            departamento = ?, estado = ?, hora_inicio = ?, hora_fin = ?,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    ''', (
                        cargo,
                        nombre,
                        cc,
                        departamento,
                        estado,
                        hora_inicio,
                        hora_fin,
                        emp_id
                    ))
                    
                    if cursor.rowcount > 0:
                        cambios_realizados += 1
                        print(f"✅ Empleado actualizado: ID={emp_id}, CC={cc}")
                        
                except Exception as e:
                    print(f"❌ Error al actualizar empleado ID {row_dict.get('ID_OCULTO', 'N/A')}: {str(e)}")
                    continue
                    
            else:
                # Es una nueva inserción
                try:
                    # Obtener el próximo número
                    cursor.execute("SELECT MAX(numero) FROM empleados")
                    max_num = cursor.fetchone()[0]
                    nuevo_numero = (max_num or 0) + 1
                    
                    # Preparar valores
                    cargo = str(row_dict.get('CARGO', '')) if pd.notna(row_dict.get('CARGO', '')) else ''
                    nombre = str(row_dict.get('APELLIDOS Y NOMBRES', '')) if pd.notna(row_dict.get('APELLIDOS Y NOMBRES', '')) else ''
                    cc = str(row_dict.get('CC', '')) if pd.notna(row_dict.get('CC', '')) else ''
                    departamento = str(row_dict.get('DEPARTAMENTO', '')) if pd.notna(row_dict.get('DEPARTAMENTO', '')) else ''
                    estado = str(row_dict.get('ESTADO', 'Activo')) if pd.notna(row_dict.get('ESTADO', '')) else 'Activo'
                    
                    # Manejar horas
                    hora_inicio = str(row_dict.get('HORA_INICIO', '')) if pd.notna(row_dict.get('HORA_INICIO', '')) else None
                    hora_fin = str(row_dict.get('HORA_FIN', '')) if pd.notna(row_dict.get('HORA_FIN', '')) else None
                    
                    if hora_inicio == '' or hora_inicio == 'nan':
                        hora_inicio = None
                    if hora_fin == '' or hora_fin == 'nan':
                        hora_fin = None
                    
                    # Verificar si ya existe un empleado con esta cédula
                    cursor.execute("SELECT COUNT(*) FROM empleados WHERE cedula = ?", (cc,))
                    if cursor.fetchone()[0] > 0:
                        print(f"⚠️ Empleado con CC {cc} ya existe, omitiendo...")
                        continue
                    
                    cursor.execute('''
                        INSERT INTO empleados 
                        (numero, cargo, nombre_completo, cedula, departamento, estado, hora_inicio, hora_fin)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        nuevo_numero,
                        cargo,
                        nombre,
                        cc,
                        departamento,
                        estado,
                        hora_inicio,
                        hora_fin
                    ))
                    
                    if cursor.rowcount > 0:
                        cambios_realizados += 1
                        print(f"✅ Nuevo empleado insertado: N°={nuevo_numero}, CC={cc}")
                        
                except Exception as e:
                    print(f"❌ Error al insertar nuevo empleado: {str(e)}")
                    continue
        
        conn.commit()
        conn.close()
        
        # Actualizar session state
        st.session_state.empleados_df = get_empleados()
        
        return cambios_realizados, []
        
    except Exception as e:
        print(f"❌ Error general al guardar empleados: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
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
    """Autenticar usuario desde base de datos - VERSIÓN MEJORADA"""
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
                # Buscar coincidencia por nombre (más flexible)
                nombre_buscado = nombre_usuario.strip().upper()
                
                # Primero: búsqueda exacta
                empleado_encontrado = empleados_df[
                    empleados_df['nombre_completo'].str.strip().str.upper() == nombre_buscado
                ]
                
                # Si no se encuentra, buscar por coincidencias parciales
                if empleado_encontrado.empty:
                    # Separar nombre en partes para búsqueda más flexible
                    partes_nombre = nombre_buscado.split()
                    
                    def buscar_por_partes(nombre_empleado):
                        nombre_emp = str(nombre_empleado).upper()
                        # Verificar si alguna parte del nombre del usuario está en el nombre del empleado
                        for parte in partes_nombre:
                            if parte and parte in nombre_emp:
                                return True
                        return False
                    
                    mask = empleados_df['nombre_completo'].apply(buscar_por_partes)
                    empleado_encontrado = empleados_df[mask]
                
                # Si aún no se encuentra, buscar por coincidencia inversa
                if empleado_encontrado.empty:
                    def buscar_inversa(nombre_empleado):
                        nombre_emp = str(nombre_empleado).upper()
                        partes_empleado = nombre_emp.split()
                        for parte in partes_empleado:
                            if parte and parte in nombre_buscado:
                                return True
                        return False
                    
                    mask = empleados_df['nombre_completo'].apply(buscar_inversa)
                    empleado_encontrado = empleados_df[mask]
                
                if not empleado_encontrado.empty:
                    st.session_state.empleado_actual = empleado_encontrado.iloc[0].to_dict()
                    print(f"✅ Empleado asociado: {st.session_state.empleado_actual.get('nombre_completo')}")
                    print(f"   ID: {st.session_state.empleado_actual.get('id')}")
                    print(f"   CC: {st.session_state.empleado_actual.get('cedula')}")
                else:
                    st.session_state.empleado_actual = None
                    print(f"⚠️ No se encontró empleado para usuario: {nombre_usuario}")
            
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
    st.markdown("<h1 class='main-header'>Malla de Turnos Locatel Restrepo</h1>", unsafe_allow_html=True)
    
    # Mostrar advertencia de Streamlit Cloud
    #if IS_STREAMLIT_CLOUD:
    #   st.markdown("""
    #    <div class="streamlit-cloud-warning">
    #    ⚠️ MODO STREAMLIT CLOUD ACTIVADO<br>
    #    <small>Los datos se guardan en almacenamiento temporal. Exporta tus datos regularmente.</small>
    #    </div>
    #    """, unsafe_allow_html=True)
    
    with st.container():
        col1, col2, col3 = st.columns([1, 2, 1])
        
        with col2:
            st.markdown("""
            <div class="info-card">
                <h3 style="text-align: center; color: #1E3A8A;">Malla de Turnos Locatel Restrepo</h3>
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
            #with st.expander("🔑 Credenciales de prueba"):
            #    st.markdown("""
            #    **Administrador:**
            #    - Usuario: `admin`
            #    - Contraseña: `admin123`
            #    
            #    **Supervisor:**
            #    - Usuario: `supervisor`
            #    - Contraseña: `super123`
            #    
            #    **Empleado:**
            #    - Usuario: `empleado`
            #    - Contraseña: `empleado123`
            #    """)

# ============================================================================
# COMPONENTES DE INTERFAZ
# ============================================================================
def mostrar_barra_usuario():
    """Mostrar barra superior con información del usuario - Responsiva"""
    if st.session_state.auth['is_authenticated']:
        user_info = st.session_state.auth['user_data']
        
        # Layout responsivo - CORREGIDO: Definir todas las columnas necesarias
        col1, col2, col3, col4 = st.columns([3, 1, 1, 1])  # Añadidas col3 y col4
        
        with col1:
            st.markdown(f"""
            <div style="background-color: #1E3A8A; color: white; padding: 10px 15px; 
                       border-radius: 5px; margin-bottom: 10px; font-size: 0.9em;">
                <strong>👤 {user_info['nombre']}</strong><br>
                <small>Rol: {st.session_state.auth['role'].title()} | Depto: {user_info.get('departamento', 'N/A')}</small>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            # Botón de recarga
            if st.button("🔄 Recargar", use_container_width=True, key="btn_recargar"):
                st.rerun()
        
        with col3:
            # Mostrar info de último guardado (solo en desktop)
            if not st.session_state.get('is_mobile', False):
                if st.session_state.last_save:
                    tiempo_transcurrido = obtener_hora_colombia() - st.session_state.last_save
                    minutos = int(tiempo_transcurrido.total_seconds() / 60)
                    if minutos < 1:
                        tiempo_texto = "Ahora"
                    elif minutos == 1:
                        tiempo_texto = "Hace 1 min"
                    else:
                        tiempo_texto = f"Hace {minutos} min"
                    st.markdown(f"<div style='text-align: center; padding: 10px;'><small>💾 {tiempo_texto}</small></div>", 
                               unsafe_allow_html=True)
        
        with col4:
            # Botón de cerrar sesión
            if st.button("🚪 Cerrar", use_container_width=True, key="btn_logout", type="secondary"):
                logout()

def mostrar_sidebar():
    """Mostrar sidebar según el rol del usuario - Optimizado para móvil"""
    with st.sidebar:
        rol = st.session_state.auth['role']
        
        st.markdown(f"<h3 style='text-align: center; font-size: 1.2em;'>📊 {rol.title()}</h3>", unsafe_allow_html=True)
        st.markdown("---")
        
        # Mostrar hora Colombia (más compacta en móvil)
        hora_colombia = obtener_hora_colombia()
        st.markdown(f"""
        <div style="text-align: center; padding: 8px; background-color: #1E3A8A; 
                   color: white; border-radius: 5px; margin-bottom: 10px; font-size: 0.9em;">
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
                ("📅 Malla", "malla"),
                ("👥 Empleados", "empleados"),
                ("⚙️ Config", "config"),
                ("👑 Usuarios", "usuarios"),
                ("📦 Backup", "backup"),
                ("🖥️ Sistema", "info_sistema")
            ]
        elif rol == "supervisor":
            opciones = [
                ("📅 Malla", "malla"),
                ("👥 Empleados", "empleados")
            ]
        else:  # empleado
            opciones = [
                ("📅 Mis Turnos", "mis_turnos"),
                ("📆 Calendario", "calendario"),
                ("👤 Mi Info", "mi_info")
            ]
        
        # Botones de navegación más compactos para móvil
        for icon_text, key in opciones:
            if st.button(icon_text, key=f"nav_{key}", use_container_width=True, 
                        help=f"Ir a {icon_text.split(' ')[1]}",
                        type="secondary"):
                st.session_state.current_page = key
                st.rerun()
        
        st.markdown("---")
        
        # Información del sistema más compacta
        if rol == "admin":
            total_empleados = len(st.session_state.empleados_df)
            activos = st.session_state.empleados_df[st.session_state.empleados_df['estado'] == 'Activo'].shape[0]
            
            st.markdown("**📈 Stats**")
            st.info(f"""
            Empleados: {total_empleados}  
            Activos: {activos}
            """, icon="ℹ️")
            
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

def mostrar_malla_congelada(df):
    gb = GridOptionsBuilder.from_dataframe(df)

    gb.configure_default_column(
        resizable=True,
        sortable=True,
        filter=True,
        minWidth=80
    )

    # 🔒 Congelar la columna 3
    col_fija = df.columns[2]
    gb.configure_column(col_fija, pinned="left")

    # 🔒 Congelar la fila 1
    gb.configure_grid_options(
        pinnedTopRowData=[df.iloc[0].to_dict()]
    )

    gridOptions = gb.build()

    AgGrid(
        df,
        gridOptions=gridOptions,
        height=600,
        fit_columns_on_grid_load=False,
        data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
        update_mode=GridUpdateMode.NO_UPDATE,
        enable_enterprise_modules=True,
        theme="balham"
    )


def mostrar_leyenda(inside_expander=False):
    """Mostrar leyenda de colores - VERSIÓN SIMPLIFICADA
    
    Args:
        inside_expander (bool): Indica si ya estamos dentro de un expander
    """
    if 'codigos_turno' not in st.session_state or not st.session_state.codigos_turno:
        st.info("No hay códigos de turno configurados.")
        return
    
    codigos = st.session_state.codigos_turno
    
    # Filtrar código vacío
    items = [(codigo, info) for codigo, info in codigos.items() if codigo != ""]
    
    if not items:
        st.info("No hay códigos de turno configurados.")
        return
    
    # CORRECCIÓN: Siempre mostrar en un expander, no usar inside_expander
    with st.expander("🎨 Leyenda de códigos de turno", expanded=False):
        st.markdown("**Códigos disponibles (haz clic en una celda para seleccionar):**")
        
        # Organizar en columnas responsivas
        cols_per_row = 4
        num_items = len(items)
        num_rows = (num_items + cols_per_row - 1) // cols_per_row
        
        for row in range(num_rows):
            cols = st.columns(cols_per_row)
            start_idx = row * cols_per_row
            end_idx = min(start_idx + cols_per_row, num_items)
            
            for idx in range(start_idx, end_idx):
                codigo, info = items[idx]
                col_idx = idx % cols_per_row
                
                with cols[col_idx]:
                    color = info.get("color", "#FFFFFF")
                    nombre = info.get("nombre", "Sin nombre")
                    horas = info.get("horas", 0)
                    
                    st.markdown(f"""
                    <div style="margin-bottom: 8px; padding: 8px; background: #f9f9f9; 
                               border-radius: 4px; border-left: 4px solid {color};">
                        <div style="font-weight: bold; font-size: 0.95em;">{codigo}</div>
                        <div style="font-size: 0.8em; color: #666;">{nombre[:20]}{'...' if len(nombre) > 20 else ''}</div>
                        <div style="font-size: 0.75em; color: #888;">{horas}h</div>
                    </div>
                    """, unsafe_allow_html=True)

def extraer_horas_desde_codigo(codigo):
    """
    Extraer información de horas desde el código del turno.
    
    Intenta extraer horas de diferentes maneras:
    1. De la descripción del código (si contiene horas)
    2. De las horas configuradas
    3. Retorna el código si no se puede extraer hora
    """
    if not codigo or str(codigo).strip() == "" or str(codigo).strip() == "0":
        return ""
    
    codigo_str = str(codigo).strip()
    
    # Si el código está en la lista de códigos configurados
    if 'codigos_turno' in st.session_state and codigo_str in st.session_state.codigos_turno:
        info = st.session_state.codigos_turno[codigo_str]
        
        # Primero intentar extraer de la descripción
        nombre = info.get("nombre", "")
        
        # Buscar patrones de hora en la descripción
        patrones_hora = [
            r'(\d{1,2}[:.]\d{2}\s*[AP]?M?\s*[-–—]\s*\d{1,2}[:.]\d{2}\s*[AP]?M?)',  # 8:00-17:00
            r'(\d{1,2}\s*[AP]?M?\s*[-–—]\s*\d{1,2}\s*[AP]?M?)',  # 8 AM - 5 PM
            r'(\d{1,2}[:.]\d{2}\s*[-–—]\s*\d{1,2}[:.]\d{2})',  # 8.00-17.00
            r'(\d{1,2}\s*h\s*[-–—]\s*\d{1,2}\s*h)',  # 8h - 17h
        ]
        
        for patron in patrones_hora:
            match = re.search(patron, nombre, re.IGNORECASE)
            if match:
                hora_encontrada = match.group(1)
                # Limpiar y formatear
                hora_limpia = re.sub(r'\s+', ' ', hora_encontrada.strip())
                return hora_limpia
        
        # Si no se encuentra patrón de hora, usar las horas configuradas
        horas = info.get("horas", 0)
        if horas > 0:
            return f"{horas}h"
    
    # Para códigos especiales
    codigos_especiales = {
        "VC": "Vacaciones",
        "CP": "Cumpleaños",
        "PA": "Permiso",
        "-1": "Ausente"
    }
    
    if codigo_str in codigos_especiales:
        return codigos_especiales[codigo_str]
    
    # Si el código es solo un número (y no está en la lista de códigos)
    if codigo_str.isdigit() and int(codigo_str) == 0:
        return ""
    
    # Si no se puede extraer hora, devolver el código
    return codigo_str

def generar_calendario_simple(mes, ano, turnos_dict):
    """Versión ultra-simple y robusta"""
    nombres_meses = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", 
                    "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
    
    st.markdown(f"### 📅 {nombres_meses[mes-1]} {ano}")
    
    num_dias = calendar.monthrange(ano, mes)[1]
    primer_dia = date(ano, mes, 1)
    dia_semana = primer_dia.weekday()
    espacios_vacios = (dia_semana + 1) % 7
    
    # Crear calendario con HTML básico pero correcto
    html = '<div style="display: grid; grid-template-columns: repeat(7, 1fr); gap: 4px; margin-top: 15px;">'
    
    # Días de semana
    for dia in ["DOM", "LUN", "MAR", "MIÉ", "JUE", "VIE", "SÁB"]:
        html += f'<div style="text-align: center; font-weight: bold; padding: 8px; background: #f5f5f5; border-radius: 4px;">{dia}</div>'
    
    # Espacios vacíos
    for _ in range(espacios_vacios):
        html += '<div></div>'
    
    # Días del mes
    for dia in range(1, num_dias + 1):
        codigo = turnos_dict.get(dia, "")
        color = "#f8f9fa"
        texto = "-"
        codigo_mostrar = ""
        
        if codigo and str(codigo).strip() not in ["", "0"]:
            codigo_str = str(codigo).strip()
            if 'codigos_turno' in st.session_state and codigo_str in st.session_state.codigos_turno:
                info = st.session_state.codigos_turno[codigo_str]
                color = info.get("color", "#e0e0e0")
                texto = info.get("nombre", codigo_str)
                codigo_mostrar = codigo_str
        
        html += f'''
        <div style="background: {color}; border-radius: 6px; padding: 10px; min-height: 100px; border: 1px solid #e0e0e0;">
            <div style="font-weight: bold; font-size: 1.2em; text-align: right;">{dia}</div>
            <div style="text-align: center; margin-top: 20px;">
                <div style="font-weight: bold; font-size: 0.9em;">{texto}</div>
        '''
        
        if codigo_mostrar:
            html += f'<div style="font-size: 0.7em; opacity: 0.7; margin-top: 5px;">[{codigo_mostrar}]</div>'
        
        html += '</div></div>'
    
    html += '</div>'
    
    st.markdown(html, unsafe_allow_html=True)
# ============================================================================
# FUNCIONES DE ESTADÍSTICAS PARA ADMIN Y SUPERVISOR
# ============================================================================
def generar_estadisticas_turnos(mes, ano):
    """Generar estadísticas detalladas de turnos por día y departamento"""
    try:
        conn = get_connection()
        
        # 1. Estadísticas por día - SIN códigos usados
        query_dias = '''
            SELECT mt.dia, 
                   COUNT(mt.id) as total_turnos,
                   COUNT(CASE WHEN mt.codigo_turno IS NOT NULL AND mt.codigo_turno != '' THEN 1 END) as turnos_asignados,
                   COUNT(CASE WHEN mt.codigo_turno IS NULL OR mt.codigo_turno = '' THEN 1 END) as turnos_vacios
            FROM malla_turnos mt
            WHERE mt.mes = ? AND mt.ano = ?
            GROUP BY mt.dia
            ORDER BY mt.dia
        '''
        
        df_dias = pd.read_sql_query(query_dias, conn, params=(mes, ano))
        
        # 2. Estadísticas por departamento
        query_deptos = '''
            SELECT e.departamento,
                   COUNT(DISTINCT e.id) as total_empleados,
                   COUNT(CASE WHEN e.estado = 'Activo' THEN 1 END) as empleados_activos,
                   COUNT(mt.id) as total_turnos,
                   COUNT(CASE WHEN mt.codigo_turno IS NOT NULL AND mt.codigo_turno != '' THEN 1 END) as turnos_asignados,
                   ROUND(AVG(CASE WHEN ct.horas IS NOT NULL THEN ct.horas ELSE 0 END), 1) as promedio_horas
            FROM empleados e
            LEFT JOIN malla_turnos mt ON e.id = mt.empleado_id AND mt.mes = ? AND mt.ano = ?
            LEFT JOIN codigos_turno ct ON mt.codigo_turno = ct.codigo
            GROUP BY e.departamento
            ORDER BY e.departamento
        '''
        
        df_deptos = pd.read_sql_query(query_deptos, conn, params=(mes, ano))
        
        # 3. Estadísticas por código de turno
        query_codigos = '''
            SELECT ct.codigo,
                   ct.nombre,
                   ct.color,
                   COUNT(mt.id) as veces_asignado,
                   SUM(ct.horas) as total_horas,
                   COUNT(DISTINCT mt.empleado_id) as empleados_distintos,
                   COUNT(DISTINCT e.departamento) as departamentos_distintos
            FROM codigos_turno ct
            LEFT JOIN malla_turnos mt ON ct.codigo = mt.codigo_turno AND mt.mes = ? AND mt.ano = ?
            LEFT JOIN empleados e ON mt.empleado_id = e.id
            WHERE ct.codigo IS NOT NULL
            GROUP BY ct.codigo, ct.nombre, ct.color
            HAVING veces_asignado > 0
            ORDER BY veces_asignado DESC
        '''
        
        df_codigos = pd.read_sql_query(query_codigos, conn, params=(mes, ano))
        
        conn.close()
        
        return {
            'por_dia': df_dias,
            'por_departamento': df_deptos,
            'por_codigo': df_codigos
        }
        
    except Exception as e:
        print(f"❌ Error al generar estadísticas: {str(e)}")
        return None

def mostrar_estadisticas_rapidas(mes, ano):
    """Mostrar un resumen rápido de estadísticas"""
    try:
        estadisticas = generar_estadisticas_turnos(mes, ano)
        
        if estadisticas is None:
            return
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            # Total de turnos asignados
            if not estadisticas['por_dia'].empty:
                total_asignados = estadisticas['por_dia']['turnos_asignados'].sum()
                st.metric("Turnos Asignados", f"{total_asignados:,}")
        
        with col2:
            # Porcentaje de asignación
            total_empleados = len(get_empleados())
            num_dias = calendar.monthrange(ano, mes)[1]
            total_posibles = total_empleados * num_dias
            
            if total_posibles > 0 and not estadisticas['por_dia'].empty:
                total_asignados = estadisticas['por_dia']['turnos_asignados'].sum()
                porcentaje = (total_asignados / total_posibles) * 100
                st.metric("Asignación", f"{porcentaje:.1f}%")
        
        with col3:
            # Departamento con más turnos
            if not estadisticas['por_departamento'].empty:
                depto_top = estadisticas['por_departamento'].nlargest(1, 'turnos_asignados')
                if not depto_top.empty:
                    st.metric("Depto. Más Ocupado", 
                            f"{depto_top.iloc[0]['departamento'][:10]}...")
        
        with col4:
            # Día más ocupado
            if not estadisticas['por_dia'].empty:
                dia_top = estadisticas['por_dia'].nlargest(1, 'turnos_asignados')
                if not dia_top.empty:
                    st.metric("Día Más Ocupado", f"Día {dia_top.iloc[0]['dia']}")
        
        # Gráfico mini de tendencia
        if not estadisticas['por_dia'].empty and len(estadisticas['por_dia']) > 1:
            fig = px.line(
                estadisticas['por_dia'],
                x='dia',
                y='turnos_asignados',
                title='Tendencia Diaria',
                height=150
            )
            
            fig.update_layout(
                margin=dict(l=0, r=0, t=30, b=0),
                xaxis_title=None,
                yaxis_title=None,
                showlegend=False
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
    except Exception as e:
        st.error(f"Error al cargar estadísticas: {str(e)}")

def mostrar_estadisticas_avanzadas(mes, ano):
    """Mostrar panel de estadísticas avanzadas"""
    st.markdown("---")
    st.markdown("### 📊 Estadísticas Avanzadas")
    
    # Generar estadísticas
    estadisticas = generar_estadisticas_turnos(mes, ano)
    
    if estadisticas is None:
        st.warning("No se pudieron generar las estadísticas.")
        return
    
    # Crear pestañas para diferentes vistas
    tab1, tab2, tab3, tab4 = st.tabs(["📅 Por Día", "🏢 Por Departamento", "🔢 Por Código", "📈 Gráficas"])
    
    # ==============================
    # PESTAÑA 1: Por Día
    # ==============================
    with tab1:
        # Estadísticas por día
        if not estadisticas['por_dia'].empty:
            st.markdown("#### 📅 Distribución de Turnos por Día")
            
            df_dias = estadisticas['por_dia']
            num_dias = calendar.monthrange(ano, mes)[1]
            
            # Calcular métricas generales
            total_turnos_posibles = len(get_empleados()) * num_dias
            total_turnos_asignados = df_dias['turnos_asignados'].sum()
            porcentaje_asignacion = (total_turnos_asignados / total_turnos_posibles * 100) if total_turnos_posibles > 0 else 0
            
            # Mostrar métricas generales
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Días del mes", num_dias)
            with col2:
                st.metric("Turnos totales", total_turnos_asignados)
            with col3:
                st.metric("Turnos posibles", total_turnos_posibles)
            with col4:
                st.metric("Asignación", f"{porcentaje_asignacion:.1f}%")
            
            # Mostrar tabla detallada (SIN columna de códigos usados)
            st.dataframe(
                df_dias.rename(columns={
                    'dia': 'Día',
                    'total_turnos': 'Total Turnos',
                    'turnos_asignados': 'Asignados',
                    'turnos_vacios': 'Vacíos'
                }),
                use_container_width=True,
                hide_index=True
            )
            
            # Gráfico de turnos por día
            fig = px.bar(
                df_dias,
                x='dia',
                y=['turnos_asignados', 'turnos_vacios'],
                title=f'Turnos por Día - {mes}/{ano}',
                labels={'dia': 'Día del mes', 'value': 'Cantidad de Turnos', 'variable': 'Estado'},
                barmode='stack',
                color_discrete_map={'turnos_asignados': '#4CAF50', 'turnos_vacios': '#FF9800'}
            )
            
            fig.update_layout(
                xaxis=dict(tickmode='linear', dtick=1),
                yaxis_title="Cantidad de Turnos",
                legend_title="Estado"
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No hay datos de turnos para mostrar por día.")
    
    # ==============================
    # PESTAÑA 2: Por Departamento
    # ==============================
    with tab2:
        # Estadísticas por departamento
        if not estadisticas['por_departamento'].empty:
            st.markdown("#### 🏢 Estadísticas por Departamento")
            
            df_deptos = estadisticas['por_departamento']
            
            # Mostrar tabla
            st.dataframe(
                df_deptos.rename(columns={
                    'departamento': 'Departamento',
                    'total_empleados': 'Total Empleados',
                    'empleados_activos': 'Empleados Activos',
                    'total_turnos': 'Total Turnos',
                    'turnos_asignados': 'Turnos Asignados',
                    'promedio_horas': 'Promedio Horas'
                }),
                use_container_width=True,
                hide_index=True
            )
            
            # Gráfico de torta por departamento
            fig = px.pie(
                df_deptos,
                values='turnos_asignados',
                names='departamento',
                title='Distribución de Turnos por Departamento',
                hole=0.4,
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig, use_container_width=True)
            
            # Gráfico de barras horizontales
            fig2 = px.bar(
                df_deptos,
                y='departamento',
                x='turnos_asignados',
                title='Turnos Asignados por Departamento',
                orientation='h',
                color='promedio_horas',
                color_continuous_scale='Blues',
                labels={'turnos_asignados': 'Turnos Asignados', 'promedio_horas': 'Promedio Horas'}
            )
            
            fig2.update_layout(yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("No hay datos de turnos por departamento.")
    
    # ==============================
    # PESTAÑA 3: Por Código
    # ==============================
    with tab3:
        # Estadísticas por código de turno
        if not estadisticas['por_codigo'].empty:
            st.markdown("#### 🔢 Uso de Códigos de Turno")
            
            df_codigos = estadisticas['por_codigo']
            
            # Mostrar tabla con colores
            def color_row(val):
                if isinstance(val, str) and val.startswith('#'):
                    return f'background-color: {val}; color: white;'
                return ''
            
            # Primero renombrar las columnas
            df_codigos_renombrado = df_codigos.rename(columns={
                'codigo': 'Código',
                'nombre': 'Descripción',
                'color': 'Color',
                'veces_asignado': 'Veces Asignado',
                'total_horas': 'Total Horas',
                'empleados_distintos': 'Empleados',
                'departamentos_distintos': 'Departamentos'
            })
            
            # Luego aplicar estilos
            styled_df = df_codigos_renombrado.style.applymap(color_row, subset=['Color'])
            
            st.dataframe(
                styled_df,
                use_container_width=True,
                hide_index=True
            )
            
            # Gráfico de códigos más usados
            fig = px.bar(
                df_codigos.head(10),
                x='codigo',
                y='veces_asignado',
                title='Top 10 Códigos Más Usados',
                color='veces_asignado',
                color_continuous_scale='Viridis',
                labels={'codigo': 'Código', 'veces_asignado': 'Veces Asignado'}
            )
            
            fig.update_layout(xaxis={'categoryorder': 'total descending'})
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No hay códigos de turno asignados en este período.")
    
    # ==============================
    # PESTAÑA 4: Gráficas
    # ==============================
    with tab4:
        # Gráficas avanzadas
        st.markdown("#### 📈 Análisis Avanzado")
        
        if not estadisticas['por_dia'].empty and not estadisticas['por_departamento'].empty:
            col1, col2 = st.columns(2)
            
            with col1:
                # Heatmap de asignación por día y departamento (simplificado)
                st.markdown("##### Calor de Asignación por Día")
                
                df_dias = estadisticas['por_dia']
                
                fig = go.Figure(data=go.Heatmap(
                    z=[df_dias['turnos_asignados']],
                    x=df_dias['dia'],
                    y=['Turnos Asignados'],
                    colorscale='Blues',
                    showscale=True,
                    hovertemplate='Día: %{x}<br>Turnos: %{z}<extra></extra>'
                ))
                
                fig.update_layout(
                    title='Intensidad de Asignación por Día',
                    xaxis_title='Día del Mes',
                    height=200
                )
                
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Comparativa de asignación
                st.markdown("##### Eficiencia de Asignación")
                
                df_deptos = estadisticas['por_departamento']
                
                # Calcular porcentaje de asignación por departamento
                if 'total_turnos' in df_deptos.columns and 'turnos_asignados' in df_deptos.columns:
                    df_deptos['porcentaje_asignacion'] = (df_deptos['turnos_asignados'] / df_deptos['total_turnos'] * 100).round(1)
                    
                    fig = px.bar(
                        df_deptos,
                        y='departamento',
                        x='porcentaje_asignacion',
                        title='Porcentaje de Asignación por Depto',
                        orientation='h',
                        color='porcentaje_asignacion',
                        color_continuous_scale='RdYlGn',
                        range_color=[0, 100]
                    )
                    
                    fig.update_layout(
                        xaxis_title='Porcentaje de Asignación (%)',
                        xaxis_range=[0, 100]
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
        
        # Resumen ejecutivo
        st.markdown("##### 📋 Resumen Ejecutivo")
        
        if not estadisticas['por_dia'].empty:
            df_dias = estadisticas['por_dia']
            df_deptos = estadisticas['por_departamento']
            
            # Calcular métricas clave
            total_empleados = len(get_empleados())
            dias_con_mas_turnos = df_dias.nlargest(3, 'turnos_asignados')[['dia', 'turnos_asignados']]
            depto_mas_ocupado = df_deptos.nlargest(1, 'turnos_asignados')[['departamento', 'turnos_asignados']]
            
            col_sum1, col_sum2, col_sum3 = st.columns(3)
            
            with col_sum1:
                st.markdown(f"""
                <div class="metric-card">
                    <h4>👥 Empleados</h4>
                    <p><strong>Total:</strong> {total_empleados}</p>
                    <p><strong>Activos:</strong> {get_empleados()[get_empleados()['estado'] == 'Activo'].shape[0]}</p>
                </div>
                """, unsafe_allow_html=True)
            
            with col_sum2:
                dias_text = ""
                for _, row in dias_con_mas_turnos.iterrows():
                    dias_text += f"Día {row['dia']}: {row['turnos_asignados']} turnos<br>"
                
                st.markdown(f"""
                <div class="metric-card">
                    <h4>📅 Días más ocupados</h4>
                    {dias_text}
                </div>
                """, unsafe_allow_html=True)
            
            with col_sum3:
                if not depto_mas_ocupado.empty:
                    depto_info = depto_mas_ocupado.iloc[0]
                    st.markdown(f"""
                    <div class="metric-card">
                        <h4>🏢 Depto. más ocupado</h4>
                        <p><strong>{depto_info['departamento']}</strong></p>
                        <p>{depto_info['turnos_asignados']} turnos asignados</p>
                    </div>
                    """, unsafe_allow_html=True)
            
            with col_sum2:
                dias_text = ""
                for _, row in dias_con_mas_turnos.iterrows():
                    dias_text += f"Día {row['dia']}: {row['turnos_asignados']} turnos<br>"
                
                st.markdown(f"""
                <div class="metric-card">
                    <h4>📅 Días más ocupados</h4>
                    {dias_text}
                </div>
                """, unsafe_allow_html=True)
            
            with col_sum3:
                if not depto_mas_ocupado.empty:
                    depto_info = depto_mas_ocupado.iloc[0]
                    st.markdown(f"""
                    <div class="metric-card">
                        <h4>🏢 Depto. más ocupado</h4>
                        <p><strong>{depto_info['departamento']}</strong></p>
                        <p>{depto_info['turnos_asignados']} turnos asignados</p>
                    </div>
                    """, unsafe_allow_html=True)
# ============================================================================
# PÁGINAS PRINCIPALES (SOLO LAS MÁS IMPORTANTES)
# ============================================================================
def pagina_malla():
    """Página principal - Malla de turnos CON ESTADÍSTICAS - Optimizada para móvil"""
    st.markdown("<h1 class='main-header'>📊 Malla de Turnos</h1>", unsafe_allow_html=True)
    
    # En móvil, usar columnas apiladas
    if st.session_state.is_mobile:
        col1, col2 = st.columns(2)
        col3, col4 = st.columns(2)
    else:
        col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        meses = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", 
                "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
        mes_seleccionado = st.selectbox("Mes:", meses, index=st.session_state.mes_actual - 1)
        mes_numero = meses.index(mes_seleccionado) + 1
    
    with col2:
        ano = st.selectbox("Año:", [2026, 2025, 2024, 2027], index=0)
    
    with col3:
        if st.button("🔄 Cargar Malla", use_container_width=True, 
                    help="Cargar o actualizar la malla de turnos"):
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
                label="📥 Exportar",
                data=csv,
                file_name=f"malla_{mes_seleccionado}_{ano}.csv",
                mime="text/csv",
                use_container_width=True,
                help="Descargar como archivo CSV"
            )
    
    # En móvil, mostrar leyenda como expander por defecto
    if st.session_state.is_mobile:
        with st.expander("📋 Códigos de Turno", expanded=False):
            mostrar_leyenda(inside_expander=True)
    else:
        mostrar_leyenda(inside_expander=True)
    
    # AQUÍ ESTÁ EL CAMBIO PRINCIPAL - CORREGIR LA INDENTACIÓN
    if st.session_state.malla_actual.empty:
        st.warning("⚠️ No hay malla de turnos cargada. Presiona 'Cargar Malla' para ver los datos.")
    else:
        st.markdown(f"### 📋 Malla de Turnos - {mes_seleccionado} {ano}")
        
        # Aplicar estilos para tablas sincronizadas
        aplicar_estilos_tabla_sincronizada()
        
        rol = st.session_state.auth['role']
        
        # AQUÍ ESTÁ EL CAMBIO: Solo mostrar tabla dividida para quienes pueden editar
        if check_permission("write"):  # Admin y supervisor - TABLA DIVIDIDA CON SCROLL SINCRONIZADO
            st.markdown('<div class="auto-save-notice">💡 Los cambios se guardan automáticamente al salir de la celda</div>', unsafe_allow_html=True)
            
            df = st.session_state.malla_actual.copy()
            
            # ... resto del código para la tabla editable ...
            # (aquí debe ir el código específico para mostrar la tabla editable)
            
            # Por ejemplo:
            st.info("🔄 Esta sección está en desarrollo - Tabla editable con scroll sincronizado")
            
        else:  # Empleados con solo lectura
            st.info("👁️ Vista de solo lectura - No puedes editar")
            
            df = st.session_state.malla_actual.copy()
            
            # Mostrar tabla completa normal
            st.dataframe(
                df,
                height=600,
                use_container_width=True
            )
            
            # Botón para descargar la tabla completa
            st.markdown("---")
            csv = df.to_csv(index=False, encoding='utf-8-sig')
            st.download_button(
                label="📥 Descargar tabla completa (CSV)",
                data=csv,
                file_name=f"malla_{mes_seleccionado}_{ano}_completa.csv",
                mime="text/csv",
                use_container_width=True
            )

def pagina_backup():
    """Página completa de backup y restauración"""
    st.markdown("<h1 class='main-header'>📦 Sistema de Backup y Restauración</h1>", unsafe_allow_html=True)
    
    # Información importante para Streamlit Cloud
    #if IS_STREAMLIT_CLOUD:
    #    st.markdown("""
    #    <div class="streamlit-cloud-warning">
    #    ⚠️ **INFORMACIÓN IMPORTANTE - STREAMLIT CLOUD**
    #    
    #    **Cómo funciona el almacenamiento:**
    #    1. Los datos se guardan en almacenamiento temporal del servidor
    #    2. Se mantienen mientras la app esté activa
    #    3. Pueden borrarse después de ~24h de inactividad
    #    
    #    **Recomendaciones:**
    #    - ✅ Exporta tus datos regularmente (JSON o CSV)
    #    - ✅ Descarga backups frecuentemente
    #    - ✅ Mantén la app activa usándola diariamente
    #    - ❌ No confíes solo en el almacenamiento temporal
    #    </div>
    #   """, unsafe_allow_html=True)
    
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
    #if IS_STREAMLIT_CLOUD:
    #    st.warning("""
    #    ⚠️ **STREAMLIT CLOUD**
    #    - Exporta la lista de empleados regularmente
    #    - Los datos se guardan automáticamente en backups
    #    """)
    
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
        
        # Asegurar que tenemos las columnas necesarias
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
        
        # Asegurar que todas las columnas tengan valores no nulos
        df_display = df_display.fillna({
            'CARGO': '',
            'APELLIDOS Y NOMBRES': '',
            'CC': '',
            'DEPARTAMENTO': '',
            'ESTADO': 'Activo',
            'HORA_INICIO': '',
            'HORA_FIN': '',
            'FECHA_REGISTRO': ''
        })
        
        column_order = ['N°', 'CARGO', 'APELLIDOS Y NOMBRES', 'CC', 'DEPARTAMENTO', 
                       'ESTADO', 'HORA_INICIO', 'HORA_FIN', 'FECHA_REGISTRO', 'ID_OCULTO']
        
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
            df_display[column_order],
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
                    cambios, errores = guardar_empleados(edited_df)
                    if cambios > 0:
                        st.success(f"✅ {cambios} cambios guardados correctamente")
                        # Crear backup automático
                        crear_backup_automatico()
                        st.info("📦 Backup automático creado")
                        st.rerun()
                    else:
                        if errores:
                            for error in errores:
                                st.error(error)
                        else:
                            st.warning("⚠️ No se realizaron cambios")
                except Exception as e:
                    st.error(f"❌ Error al guardar: {str(e)}")
                    import traceback
                    st.error(f"Detalles: {traceback.format_exc()}")
        
        with col2:
            if st.button("🔄 Recargar desde BD", use_container_width=True, key="btn_recargar_empleados"):
                st.session_state.empleados_df = get_empleados()
                st.success("✅ Datos recargados desde base de datos")
                st.rerun()
        
        with col3:
            csv = df_display[['N°', 'CARGO', 'APELLIDOS Y NOMBRES', 'CC', 'DEPARTAMENTO', 
                             'ESTADO', 'HORA_INICIO', 'HORA_FIN', 'FECHA_REGISTRO']].to_csv(index=False)
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
    """Página de configuración - VERSIÓN CORREGIDA"""
    if not check_permission("configure"):
        st.error("⛔ No tienes permisos para acceder a la configuración")
        return
    
    st.markdown("<h1 class='main-header'>⚙️ Configuración</h1>", unsafe_allow_html=True)
    
    # Crear pestañas
    tab1, tab2 = st.tabs(["Códigos de Turno", "General"])
    
    with tab1:
        st.markdown("### Configurar Códigos de Turno")
        
        conn = get_connection()
        codigos_df = pd.read_sql("SELECT * FROM codigos_turno ORDER BY codigo", conn)
        conn.close()
        
        # Mostrar información sobre los códigos actuales
        st.markdown(f"**📊 Total de códigos:** {len(codigos_df)}")
        
        # Mostrar vista previa de colores
        st.markdown("#### 🎨 Códigos Actuales")
        if not codigos_df.empty:
            cols = st.columns(4)
            for idx, (_, row) in enumerate(codigos_df.iterrows()):
                with cols[idx % 4]:
                    st.markdown(f"""
                    <div style="display: flex; align-items: center; margin: 5px 0; padding: 5px; background: #f8f9fa; border-radius: 5px;">
                        <div style="width: 25px; height: 25px; background-color: {row['color']}; 
                                 margin-right: 10px; border: 1px solid #ccc; border-radius: 3px;"></div>
                        <div>
                            <strong>{row['codigo']}</strong><br>
                            <small style="color: #666;">{row['nombre']}</small><br>
                            <small style="color: #666;">{row['horas']}h - {row['color']}</small>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # AGREGAR NUEVO CÓDIGO - VERSIÓN CORREGIDA
        st.markdown("#### ➕ Agregar Nuevo Código")
        with st.form("form_nuevo_codigo", clear_on_submit=True):
            col1, col2 = st.columns(2)
            
            with col1:
                nuevo_codigo = st.text_input("Código*", placeholder="Ej: 30, NOCHE, LIBRE")
                nuevo_nombre = st.text_input("Descripción*", placeholder="Ej: Turno Noche 10PM-6AM")
            
            with col2:
                # Usar color picker en lugar de text input
                nuevo_color = st.color_picker("Seleccionar Color*", value="#FF6B6B")
                nuevo_horas = st.number_input("Horas*", min_value=0, max_value=24, value=8)
            
            # Mostrar vista previa
            if nuevo_color:
                st.markdown(f"""
                <div style="display: flex; align-items: center; margin: 10px 0; padding: 10px; background: #f8f9fa; border-radius: 5px;">
                    <div style="width: 50px; height: 50px; background-color: {nuevo_color}; 
                             margin-right: 15px; border: 2px solid #ccc; border-radius: 5px;"></div>
                    <div>
                        <strong>Vista previa:</strong><br>
                        Código: <strong>{nuevo_codigo.upper() if nuevo_codigo else '[Nuevo]'}</strong><br>
                        Descripción: {nuevo_nombre if nuevo_nombre else '[Sin descripción]'}<br>
                        Color: {nuevo_color} | Horas: {nuevo_horas}
                    </div>
                </div>
                """, unsafe_allow_html=True)
            
            submitted = st.form_submit_button("➕ Agregar Nuevo Código", use_container_width=True, type="primary")
            
            if submitted:
                if not all([nuevo_codigo.strip(), nuevo_nombre.strip()]):
                    st.error("❌ Los campos con * son obligatorios")
                else:
                    # Validar formato de color
                    if not nuevo_color.startswith('#'):
                        nuevo_color = '#' + nuevo_color
                    
                    # Validar que sea un color HEX válido
                    import re
                    hex_pattern = re.compile(r'^#([A-Fa-f0-9]{6}|[A-Fa-f0-9]{3})$')
                    if not hex_pattern.match(nuevo_color):
                        st.error("❌ Formato de color inválido. Usa formato HEX (#RRGGBB o #RGB)")
                    else:
                        # Verificar si el código ya existe
                        if nuevo_codigo.upper() in codigos_df['codigo'].str.upper().values:
                            st.error(f"❌ El código '{nuevo_codigo}' ya existe")
                        else:
                            try:
                                conn = get_connection()
                                cursor = conn.cursor()
                                
                                cursor.execute(
                                    "INSERT INTO codigos_turno (codigo, nombre, color, horas) VALUES (?, ?, ?, ?)",
                                    (nuevo_codigo.upper().strip(), 
                                     nuevo_nombre.strip(), 
                                     nuevo_color.upper(), 
                                     int(nuevo_horas))
                                )
                                
                                conn.commit()
                                conn.close()
                                
                                st.success(f"✅ Código '{nuevo_codigo}' agregado correctamente")
                                st.session_state.codigos_turno = get_codigos_turno()
                                crear_backup_automatico()
                                st.rerun()
                                
                            except Exception as e:
                                st.error(f"❌ Error al agregar código: {str(e)}")
        
        st.markdown("---")
        
        # Editar códigos existentes
        if not codigos_df.empty:
            st.markdown("#### ✏️ Editar Códigos Existentes")
            
            for idx, row in codigos_df.iterrows():
                with st.expander(f"📝 {row['codigo']}: {row['nombre']}", expanded=False):
                    col_edit1, col_edit2 = st.columns(2)
                    
                    with col_edit1:
                        nuevo_nombre_edit = st.text_input(
                            "Descripción",
                            value=row['nombre'],
                            key=f"nombre_{row['codigo']}"
                        )
                        nuevo_color_edit = st.color_picker(
                            "Color",
                            value=row['color'],
                            key=f"color_{row['codigo']}"
                        )
                    
                    with col_edit2:
                        nuevo_horas_edit = st.number_input(
                            "Horas",
                            min_value=0,
                            max_value=24,
                            value=int(row['horas']),
                            key=f"horas_{row['codigo']}"
                        )
                    
                    col_btn1, col_btn2 = st.columns(2)
                    with col_btn1:
                        if st.button("💾 Guardar Cambios", key=f"guardar_{row['codigo']}", use_container_width=True):
                            try:
                                conn = get_connection()
                                cursor = conn.cursor()
                                
                                cursor.execute(
                                    "UPDATE codigos_turno SET nombre = ?, color = ?, horas = ? WHERE codigo = ?",
                                    (nuevo_nombre_edit.strip(),
                                     nuevo_color_edit.strip().upper(),
                                     int(nuevo_horas_edit),
                                     row['codigo'])
                                )
                                
                                conn.commit()
                                conn.close()
                                
                                st.success(f"✅ Código '{row['codigo']}' actualizado")
                                st.session_state.codigos_turno = get_codigos_turno()
                                crear_backup_automatico()
                                st.rerun()
                                
                            except Exception as e:
                                st.error(f"❌ Error al actualizar: {str(e)}")
                    
                    with col_btn2:
                        if st.button("🗑️ Eliminar", key=f"eliminar_{row['codigo']}", use_container_width=True, type="secondary"):
                            st.warning(f"⚠️ ¿Eliminar el código '{row['codigo']}'?")
                            if st.button(f"✅ Confirmar Eliminación de '{row['codigo']}'", key=f"confirmar_eliminar_{row['codigo']}"):
                                try:
                                    conn = get_connection()
                                    cursor = conn.cursor()
                                    
                                    # Eliminar el código
                                    cursor.execute("DELETE FROM codigos_turno WHERE codigo = ?", (row['codigo'],))
                                    
                                    # También eliminar turnos asociados
                                    cursor.execute("UPDATE malla_turnos SET codigo_turno = NULL WHERE codigo_turno = ?", 
                                                 (row['codigo'],))
                                    
                                    conn.commit()
                                    conn.close()
                                    
                                    st.success(f"✅ Código '{row['codigo']}' eliminado")
                                    st.session_state.codigos_turno = get_codigos_turno()
                                    crear_backup_automatico()
                                    st.rerun()
                                    
                                except Exception as e:
                                    st.error(f"❌ Error al eliminar: {str(e)}")
    
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
    """Página para que los empleados vean SUS turnos - VERSIÓN FINAL"""
    st.markdown("<h1 class='main-header'>📅 Mis Turnos</h1>", unsafe_allow_html=True)
    
    if not st.session_state.empleado_actual:
        st.warning("⚠️ No se encontró tu registro como empleado.")
        
        with st.expander("🔍 Buscar mi registro", expanded=True):
            nombre_buscar = st.text_input("Ingresa tu nombre o cédula:", 
                                         placeholder="Ej: REYES EDWIN o 74339325")
            
            if nombre_buscar:
                empleados_df = get_empleados()
                
                # Búsqueda flexible
                mask = (
                    empleados_df['nombre_completo'].str.contains(nombre_buscar.upper(), case=False, na=False) |
                    empleados_df['cedula'].astype(str).str.contains(nombre_buscar, na=False)
                )
                
                resultados = empleados_df[mask]
                
                if not resultados.empty:
                    st.success(f"✅ Se encontraron {len(resultados)} resultados:")
                    
                    for _, row in resultados.iterrows():
                        st.markdown(f"""
                        <div class="info-card" style="margin-bottom: 10px;">
                            <strong>{row['nombre_completo']}</strong><br>
                            <small>Cédula: {row['cedula']}</small><br>
                            <small>Cargo: {row['cargo']}</small><br>
                            <small>Departamento: {row['departamento']}</small>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        if st.button(f"👤 Usar este registro: {row['nombre_completo']}", 
                                   key=f"usar_{row['id']}", use_container_width=True):
                            st.session_state.empleado_actual = row.to_dict()
                            st.success(f"✅ Empleado asociado: {row['nombre_completo']}")
                            st.rerun()
                else:
                    st.warning("No se encontraron empleados con esa información.")
        
        return
    
    empleado_info = st.session_state.empleado_actual
    
    # Mostrar información del empleado
    st.markdown("### 👤 Mi Información")
    
    col_info1, col_info2 = st.columns(2)
    
    with col_info1:
        st.markdown(f"""
        <div class="info-card">
            <p><strong>Nombre:</strong> {empleado_info.get('nombre_completo', 'N/A')}</p>
            <p><strong>Cargo:</strong> {empleado_info.get('cargo', 'N/A')}</p>
            <p><strong>Departamento:</strong> {empleado_info.get('departamento', 'N/A')}</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col_info2:
        st.markdown(f"""
        <div class="info-card">
            <p><strong>Estado:</strong> {empleado_info.get('estado', 'N/A')}</p>
            <p><strong>Cédula:</strong> {empleado_info.get('cedula', 'N/A')}</p>
            <p><strong>Número:</strong> {empleado_info.get('numero', 'N/A')}</p>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    st.markdown("### 📅 Buscar Mis Turnos")
    
    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        meses = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", 
                "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
        mes_seleccionado = st.selectbox("Mes:", meses, index=1)  # Febrero por defecto
        mes_numero = meses.index(mes_seleccionado) + 1
    
    with col2:
        ano = st.selectbox("Año:", [2026, 2025, 2024, 2027], index=0)
    
    with col3:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("🔍 Buscar Mis Turnos", use_container_width=True, type="primary"):
            st.rerun()
    
    # Cargar turnos automáticamente
    empleado_id = empleado_info.get('id')
    
    if not empleado_id:
        st.error("❌ No se pudo obtener el ID del empleado")
        return
    
    try:
        # Obtener turnos
        turnos_dict = get_turnos_empleado_mes(empleado_id, mes_numero, ano)
        
        # Filtrar solo días con códigos válidos (no None ni vacíos)
        turnos_con_codigo = {dia: codigo for dia, codigo in turnos_dict.items() 
                            if codigo and str(codigo).strip() != '' and str(codigo).strip().lower() != 'none'}
        
        if not turnos_con_codigo:
            st.info(f"ℹ️ No tienes turnos asignados para {mes_seleccionado} {ano}.")
            
            # Mostrar todos los días aunque estén vacíos
            st.markdown(f"### 📋 Días del mes (todos)")
            datos_todos = []
            for dia, codigo in sorted(turnos_dict.items()):
                datos_todos.append({
                    'Día': f"{dia:02d}/{mes_numero:02d}/{ano}",
                    'Código': codigo if codigo else "Sin asignar",
                    'Estado': 'Sin turno' if not codigo or str(codigo).strip() == '' else 'Con turno'
                })
            
            df_todos = pd.DataFrame(datos_todos)
            st.dataframe(df_todos[['Día', 'Código', 'Estado']], use_container_width=True)
            
        else:
            st.success(f"✅ Tienes {len(turnos_con_codigo)} días con turnos asignados en {mes_seleccionado} {ano}")
            
            # Mostrar tabla detallada PRIMERO
            st.markdown("#### 📋 Lista de Turnos")
            
            total_horas = 0
            turnos_detallados = []
            
            for dia, codigo in sorted(turnos_con_codigo.items()):
                turno_info = st.session_state.codigos_turno.get(str(codigo), {})
                horas = turno_info.get("horas", 0)
                total_horas += horas
                
                turnos_detallados.append({
                    'Día': f"{dia:02d}/{mes_numero:02d}/{ano}",
                    'Código': codigo,
                    'Turno': turno_info.get("nombre", "Desconocido"),
                    'Horas': horas,
                    'Color': turno_info.get("color", "#FFFFFF")
                })
            
            df_turnos = pd.DataFrame(turnos_detallados)
            
            # Crear tabla con colores
            def aplicar_color_fila(row):
                color = df_turnos.loc[row.name, 'Color'] if row.name in df_turnos.index else '#FFFFFF'
                return [f'background-color: {color}' for _ in row]
            
            # Seleccionar columnas para mostrar
            df_display = df_turnos[['Día', 'Código', 'Turno', 'Horas']].copy()
            
            # Aplicar el estilo al DataFrame de visualización
            styled_df = df_display.style.apply(aplicar_color_fila, axis=1)
            
            # Mostrar el DataFrame
            st.dataframe(
                styled_df,
                use_container_width=True,
                hide_index=True
            )
            
            # MOSTRAR ESTADÍSTICAS AL FINAL
            st.markdown("---")
            st.markdown("#### 📊 Estadísticas del Mes")
            
            col_stats1, col_stats2, col_stats3, col_stats4 = st.columns(4)
            
            with col_stats1:
                st.metric("Días con turno", len(turnos_con_codigo))
            
            with col_stats2:
                st.metric("Horas totales", total_horas)
            
            with col_stats3:
                promedio = total_horas / len(turnos_con_codigo) if turnos_con_codigo else 0
                st.metric("Promedio/día", f"{promedio:.1f}h")
            
            with col_stats4:
                num_dias = calendar.monthrange(ano, mes_numero)[1]
                porcentaje = (len(turnos_con_codigo) / num_dias) * 100
                st.metric("Cobertura", f"{porcentaje:.1f}%")
            
            # Mostrar leyenda después de las estadísticas
            st.markdown("---")
            with st.expander("🎨 Leyenda de códigos", expanded=False):
                mostrar_leyenda(inside_expander=True)
            
            # Opción para exportar al final
            st.markdown("---")
            csv = df_turnos[['Día', 'Código', 'Turno', 'Horas']].to_csv(index=False)
            st.download_button(
                label="📥 Descargar mis turnos (CSV)",
                data=csv,
                file_name=f"mis_turnos_{mes_seleccionado}_{ano}.csv",
                mime="text/csv",
                use_container_width=True
            )
            
    except Exception as e:
        st.error(f"❌ Error al cargar turnos: {str(e)}")

def pagina_calendario():
    """Página de calendario visual"""
    st.markdown("<h1 class='main-header'>📆 Mi Calendario</h1>", unsafe_allow_html=True)
    
    if not st.session_state.empleado_actual:
        st.warning("⚠️ No se encontró tu registro como empleado.")
        
        if st.button("Ir a Mi Información", use_container_width=True):
            st.session_state.current_page = "mi_info"
            st.rerun()
        return
    
    empleado = st.session_state.empleado_actual
    
    # Mostrar información básica
    col_info1, col_info2 = st.columns(2)
    
    with col_info1:
        st.markdown(f"""
        <div class="info-card">
            <h4>👤 Mi Información</h4>
            <p><strong>Nombre:</strong> {empleado.get('nombre_completo', 'N/A')}</p>
            <p><strong>Cargo:</strong> {empleado.get('cargo', 'N/A')}</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col_info2:
        st.markdown(f"""
        <div class="info-card">
            <h4>💼 Datos Laborales</h4>
            <p><strong>Departamento:</strong> {empleado.get('departamento', 'N/A')}</p>
            <p><strong>Estado:</strong> {empleado.get('estado', 'N/A')}</p>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Selector de fecha
    nombres_meses = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", 
                    "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
    
    col_fecha1, col_fecha2, col_fecha3 = st.columns([2, 2, 1])
    
    with col_fecha1:
        mes_actual = obtener_hora_colombia().month
        mes = st.selectbox("Mes:", nombres_meses, index=mes_actual-1)
        mes_numero = nombres_meses.index(mes) + 1
    
    with col_fecha2:
        ano_actual = obtener_hora_colombia().year
        ano = st.number_input("Año:", min_value=2020, max_value=2030, value=ano_actual)
    
    with col_fecha3:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("📅 Ver Calendario", use_container_width=True, type="primary"):
            st.rerun()
    
    # Obtener ID del empleado
    empleado_id = empleado.get('id')
    if not empleado_id:
        st.error("❌ No se pudo obtener el ID del empleado.")
        return
    
    # Obtener y mostrar turnos
    try:
        turnos = get_turnos_empleado_mes(empleado_id, mes_numero, ano)
        
        # Contar días con turnos
        dias_con_turno = sum(1 for codigo in turnos.values() 
                           if codigo and str(codigo).strip() != "")
        
        if dias_con_turno == 0:
            st.info(f"📭 No tienes turnos asignados para {mes} {ano}.")
        else:
            st.success(f"✅ Tienes {dias_con_turno} días con turnos asignados en {mes} {ano}")
            
            # Mostrar leyenda de colores si hay turnos - CORRECCIÓN APLICADA
            # NO usar inside_expander=True aquí ya que no estamos dentro de un expander
            mostrar_leyenda()
        
        # Generar calendario
        generar_calendario_simple(mes_numero, ano, turnos)
        
    except Exception as e:
        st.error(f"❌ Error al cargar el calendario: {str(e)}")

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

# ============================================================================
# FUNCIÓN PRINCIPAL - VERSIÓN CORREGIDA
# ============================================================================
def aplicar_estilos_tabla_sincronizada():
    """Aplicar estilos CSS adicionales para tablas sincronizadas"""
    st.markdown("""
    <style>
        /* Estilos adicionales para mejor sincronización */
        .stDataFrame {
            scrollbar-width: thin;
        }
        
        .stDataFrame table {
            width: 100%;
            table-layout: fixed;
        }
        
        /* Asegurar que las filas tengan la misma altura */
        .stDataFrame tbody tr {
            height: 40px !important;
        }
        
        /* Contenedor principal para tablas sincronizadas */
        .table-sync-wrapper {
            display: flex;
            border: 1px solid #ddd;
            border-radius: 5px;
            overflow: hidden;
            margin: 10px 0;
        }
        
        .table-fixed-pane {
            flex: 0 0 40%;
            overflow-y: auto;
            max-height: 600px;
            border-right: 2px solid #1E3A8A;
        }
        
        .table-scrollable-pane {
            flex: 0 0 60%;
            overflow: auto;
            max-height: 600px;
        }
        
        /* Estilos para las celdas */
        .stDataFrame td, .stDataFrame th {
            padding: 8px !important;
            border: 1px solid #e0e0e0 !important;
        }
    </style>
    """, unsafe_allow_html=True)
def main():
    """Función principal que gestiona toda la aplicación"""
    # Inicializar session state
    if 'app_initialized' not in st.session_state:
        inicializar_session_state()
    
    # NO usar detección de dispositivo móvil - usar CSS responsivo en su lugar
    # Solo inicializar la variable si no existe
    if 'is_mobile' not in st.session_state:
        st.session_state.is_mobile = False  # Por defecto, asumir desktop
    
    # Asegurar que codigos_turno esté inicializado
    if 'codigos_turno' not in st.session_state:
        st.session_state.codigos_turno = get_codigos_turno()
    
    if not st.session_state.auth['is_authenticated']:
        pagina_login()
        return
    
    # Mostrar barra de usuario y sidebar
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
    
    # Footer responsivo
    footer_text = f"""
    <div style='text-align: center; color: #6c757d; padding: 10px; font-size: 0.9em;'>
    📊 Malla de Turnos Locatel | {hora_colombia.strftime('%H:%M')} 🇨🇴
    </div>
    """
    
    if IS_STREAMLIT_CLOUD:
        backups = list(BACKUP_DIR.glob("turnos_backup_*.db"))
        footer_text = f"""
        <div style='text-align: center; color: #6c757d; padding: 10px; font-size: 0.9em;'>
        📊 Malla de Turnos Locatel | Hora: {hora_colombia.strftime('%H:%M')} | Backups: {len(backups)}
        </div>
        """
    
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