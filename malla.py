# app.py - Sistema Completo de Gestión de Turnos con Autenticación y SQLite

# ============================================================================
# IMPORTACIONES - DEBE SER LO PRIMERO
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

# ============================================================================
# CONFIGURACIÓN INICIAL - DEBE SER SEGUNDO, DESPUÉS DE LAS IMPORTACIONES
# ============================================================================
st.set_page_config(
    page_title="Malla de Turnos - Gestión Completa",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# CSS PERSONALIZADO - DESPUÉS DE set_page_config()
# ============================================================================
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1E3A8A;
        text-align: center;
        margin-bottom: 2rem;
    }
    .stButton button {
        width: 100%;
    }
    .employee-form {
        background-color: #f8f9fa;
        padding: 20px;
        border-radius: 10px;
        border: 1px solid #dee2e6;
        margin-bottom: 20px;
    }
    .success-message {
        background-color: #d4edda;
        color: #155724;
        padding: 10px;
        border-radius: 5px;
        margin: 10px 0;
    }
    .error-message {
        background-color: #f8d7da;
        color: #721c24;
        padding: 10px;
        border-radius: 5px;
        margin: 10px 0;
    }
    .info-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 20px;
        border-radius: 10px;
        margin: 10px 0;
    }
    .readonly-badge {
        background-color: #6c757d;
        color: white;
        padding: 2px 8px;
        border-radius: 10px;
        font-size: 0.8em;
        margin-left: 10px;
    }
    .login-container {
        max-width: 400px;
        margin: 0 auto;
        padding: 40px 20px;
    }
    .user-bar {
        background-color: #1E3A8A;
        color: white;
        padding: 10px 20px;
        border-radius: 5px;
        margin-bottom: 20px;
        display: flex;
        justify-content: space-between;
        align-items: center;
    }
    .auto-save-notice {
        background-color: #d1ecf1;
        color: #0c5460;
        padding: 10px;
        border-radius: 5px;
        margin: 10px 0;
        font-size: 0.9em;
    }
    /* ESTILOS ESPECÍFICOS PARA CALENDARIO */
    .calendar-grid {
        display: grid;
        grid-template-columns: repeat(7, 1fr);
        gap: 5px;
        margin-top: 10px;
        margin-bottom: 20px;
        background-color: #f8f9fa;
        padding: 10px;
        border-radius: 10px;
        border: 1px solid #dee2e6;
    }
    .calendar-header {
        font-weight: bold;
        background-color: #1E3A8A;
        color: white;
        padding: 10px 5px;
        text-align: center;
        border-radius: 5px;
        font-size: 0.9em;
    }
    .calendar-cell {
        padding: 8px 5px;
        text-align: center;
        border-radius: 5px;
        min-height: 70px;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        align-items: center;
        border: 1px solid #e0e0e0;
        transition: all 0.3s ease;
        background-color: white;
    }
    .calendar-cell:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.1);
    }
    .day-number {
        font-weight: bold;
        font-size: 1.1em;
        color: #333;
        margin-bottom: 3px;
    }
    .day-turno {
        font-size: 0.7em;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        color: #555;
        width: 100%;
        padding: 0 2px;
    }
    .empty-cell {
        background-color: #f8f9fa;
        border: 1px dashed #dee2e6;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# CONFIGURACIÓN DE BASE DE DATOS SQLite
# ============================================================================
DB_NAME = "turnos_database.db"

def init_db():
    """Inicializar la base de datos y crear tablas si no existen"""
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

def get_connection():
    """Obtener conexión a la base de datos"""
    return sqlite3.connect(DB_NAME)

# ============================================================================
# FUNCIONES DE BASE DE DATOS
# ============================================================================
def inicializar_datos_bd():
    """Inicializar datos por defecto en la base de datos"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Verificar si ya hay usuarios
    cursor.execute("SELECT COUNT(*) FROM usuarios")
    if cursor.fetchone()[0] == 0:
        # Insertar usuarios por defecto
        usuarios_default = [
            ("admin", hashlib.sha256("admin123".encode()).hexdigest(), "admin", "Administrador Sistema", "Administración"),
            ("supervisor", hashlib.sha256("super123".encode()).hexdigest(), "supervisor", "Supervisor General", "Tienda"),
            ("empleado", hashlib.sha256("empleado123".encode()).hexdigest(), "empleado", "Juan Pérez García", "Tienda")
        ]
        
        cursor.executemany(
            "INSERT INTO usuarios (username, password_hash, role, nombre, departamento) VALUES (?, ?, ?, ?, ?)",
            usuarios_default
        )
        
        # Insertar códigos de turno por defecto
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
        
        # Insertar configuración por defecto
        config_default = [
            ("formato_hora", "24 horas", "text", "Formato de hora"),
            ("dias_vacaciones", "15", "number", "Días de vacaciones por año"),
            ("inicio_semana", "Lunes", "text", "Día de inicio de semana"),
            ("departamentos", "Administración,Tienda,Droguería,Cajas,Domicilios,Control Interno,Equipos Médicos", "list", "Departamentos disponibles"),
            ("auto_save", "1", "boolean", "Guardado automático")
        ]
        
        cursor.executemany(
            "INSERT OR IGNORE INTO configuracion (clave, valor, tipo, descripcion) VALUES (?, ?, ?, ?)",
            config_default
        )
        
        # Insertar empleados de ejemplo
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
    
    # Convertir a diccionario para compatibilidad
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
        'auto_save': True
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
    
    # Obtener empleados
    empleados_df = get_empleados()
    
    if empleados_df.empty:
        conn.close()
        return pd.DataFrame()
    
    # Crear estructura base
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
    
    # Determinar número de días en el mes
    if mes == 2 and ano == 2026:
        num_dias = 28
    elif mes in [4, 6, 9, 11]:
        num_dias = 30
    else:
        num_dias = 31
    
    # Obtener turnos existentes para este mes
    cursor = conn.cursor()
    cursor.execute('''
        SELECT e.id, mt.dia, mt.codigo_turno 
        FROM empleados e
        LEFT JOIN malla_turnos mt ON e.id = mt.empleado_id AND mt.mes = ? AND mt.ano = ?
        ORDER BY e.numero
    ''', (mes, ano))
    
    turnos_data = cursor.fetchall()
    
    # Crear diccionario de turnos
    turnos_dict = {}
    for emp_id, dia, codigo in turnos_data:
        if emp_id not in turnos_dict:
            turnos_dict[emp_id] = {}
        if dia:
            turnos_dict[emp_id][dia] = codigo if codigo else ""
    
    # Añadir columnas de días
    for dia in range(1, num_dias + 1):
        col_name = f'{dia}/{mes}/{ano}'
        df_base[col_name] = ""
        
        # Llenar con datos existentes
        for idx, row in df_base.iterrows():
            emp_id = row['ID']
            if emp_id in turnos_dict and dia in turnos_dict[emp_id]:
                df_base.at[idx, col_name] = turnos_dict[emp_id][dia]
    
    # Eliminar columna ID
    if 'ID' in df_base.columns:
        df_base = df_base.drop(columns=['ID'])
    
    conn.close()
    return df_base

def guardar_malla_turnos(df_malla, mes, ano):
    """Guardar malla de turnos en la base de datos"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Obtener empleados
    empleados_df = get_empleados()
    id_por_cedula = dict(zip(empleados_df['cedula'], empleados_df['id']))
    
    # Determinar número de días en el mes
    if mes == 2 and ano == 2026:
        num_dias = 28
    elif mes in [4, 6, 9, 11]:
        num_dias = 30
    else:
        num_dias = 31
    
    # Para cada empleado y cada día, guardar el turno
    for _, row in df_malla.iterrows():
        cedula = row['CC']
        if cedula not in id_por_cedula:
            continue
        
        emp_id = id_por_cedula[cedula]
        
        for dia in range(1, num_dias + 1):
            col_name = f'{dia}/{mes}/{ano}'
            if col_name in row:
                codigo = row[col_name]
                # Manejar valores NaN o vacíos de manera consistente
                if pd.isna(codigo) or codigo == '':
                    codigo = None
                else:
                    codigo = str(codigo).strip()
                
                # Insertar o actualizar
                cursor.execute('''
                    INSERT OR REPLACE INTO malla_turnos (empleado_id, mes, ano, dia, codigo_turno)
                    VALUES (?, ?, ?, ?, ?)
                ''', (emp_id, mes, ano, dia, codigo))
    
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

# ============================================================================
# ROLES Y PERMISOS
# ============================================================================
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
# INICIALIZACIÓN DE SESSION STATE
# ============================================================================
def inicializar_session_state():
    """Inicializar todas las variables de session_state"""
    # Inicializar base de datos
    init_db()
    inicializar_datos_bd()
    
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
        'mes_actual': datetime.now().month,
        'ano_actual': datetime.now().year,
        'malla_actual': pd.DataFrame(),
        'calendario_mes': datetime.now().month,
        'calendario_ano': datetime.now().year,
        'empleado_actual': None
    }
    
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

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
            st.session_state.auth = {
                'is_authenticated': True,
                'username': username,
                'role': result[2],
                'user_data': {
                    'nombre': result[3],
                    'departamento': result[4]
                }
            }
            
            # Buscar empleado correspondiente
            empleados_df = st.session_state.empleados_df
            empleado_encontrado = empleados_df[
                empleados_df['nombre_completo'].str.upper() == result[3].upper()
            ]
            
            if not empleado_encontrado.empty:
                st.session_state.empleado_actual = empleado_encontrado.iloc[0].to_dict()
            
            # Registrar log
            registrar_log("login", f"Usuario {username} inició sesión")
            return True
    
    return False

def logout():
    """Cerrar sesión"""
    registrar_log("logout", f"Usuario {st.session_state.auth['username']} cerró sesión")
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
# FUNCIONES DE GESTIÓN DE DATOS
# ============================================================================
def aplicar_estilo_dataframe(df):
    """Aplicar estilos de colores al DataFrame"""
    def color_cell(val):
        if pd.isna(val) or val == '':
            return 'background-color: #FFFFFF; border: 1px solid #e0e0e0;'
        color = st.session_state.codigos_turno.get(str(val), {}).get("color", "#FFFFFF")
        return f'background-color: {color}; color: black; font-weight: bold; text-align: center; border: 1px solid #e0e0e0;'
    
    # Identificar columnas de días
    day_columns = [col for col in df.columns if '/' in str(col)]
    
    if day_columns:
        styled_df = df.style.applymap(color_cell, subset=day_columns)
        return styled_df
    return df.style

# ============================================================================
# PÁGINA DE LOGIN
# ============================================================================
def pagina_login():
    """Página de inicio de sesión"""
    st.markdown("<h1 class='main-header'>🔐 Malla de Turnos Locatel Restrepo</h1>", unsafe_allow_html=True)
    
    with st.container():
        col1, col2, col3 = st.columns([1, 2, 1])
        
        with col2:
            st.markdown("""
            <div style="background-color: #f8f9fa; padding: 30px; border-radius: 10px; border: 1px solid #dee2e6;">
                <h3 style="text-align: center; color: #1E3A8A;">Inicio de Sesión</h3>
                <p style="text-align: center; color: #6c757d;">Ingrese sus credenciales para acceder</p>
            </div>
            """, unsafe_allow_html=True)
            
            with st.form("login_form"):
                username = st.text_input("👤 Usuario", placeholder="Ingrese su usuario")
                password = st.text_input("🔒 Contraseña", type="password", placeholder="Ingrese su contraseña")
                
                col_btn1, col_btn2, col_btn3 = st.columns(3)
                with col_btn1:
                    submit = st.form_submit_button("🚀 Ingresar", use_container_width=True)
                with col_btn2:
                    demo_empleado = st.form_submit_button("👨‍💼 Empleado", use_container_width=True)
                with col_btn3:
                    demo_admin = st.form_submit_button("👑 Admin", use_container_width=True)
                
                if submit:
                    if login(username, password):
                        st.success(f"✅ Bienvenido, {username}!")
                        st.rerun()
                    else:
                        st.error("❌ Usuario o contraseña incorrectos")
                
                if demo_empleado:
                    if login("empleado", "empleado123"):
                        st.success("✅ Modo empleado activado")
                        st.rerun()
                
                if demo_admin:
                    if login("admin", "admin123"):
                        st.success("✅ Modo administrador activado")
                        st.rerun()
    
    # Información de acceso
    #with st.expander("ℹ️ Credenciales de prueba", expanded=False):
    #   st.markdown("""
    #    ### Usuarios de demostración:
    #    
    #    | Usuario | Contraseña | Rol | Descripción |
    #    |---------|------------|-----|-------------|
    #    | **admin** | admin123 | Administrador | Acceso completo al sistema |
    #    | **supervisor** | super123 | Supervisor | Puede editar turnos |
    #    | **empleado** | empleado123 | Empleado | Solo lectura de información |
    #    
    #    *Para uso en producción, cambie las contraseñas por defecto.*
    #    """)

# ============================================================================
# BARRA DE USUARIO
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
                tiempo_transcurrido = datetime.now() - st.session_state.last_save
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

# ============================================================================
# SIDEBAR SEGÚN ROL
# ============================================================================
def mostrar_sidebar():
    """Mostrar sidebar según el rol del usuario"""
    with st.sidebar:
        rol = st.session_state.auth['role']
        
        st.markdown(f"<h3 style='text-align: center;'>📊 {rol.title()}</h3>", unsafe_allow_html=True)
        st.markdown("---")
        
        # Opciones según rol
        if rol == "admin":
            opciones = [
                ("📅 Malla de Turnos", "malla"),
                ("👥 Gestión de Empleados", "empleados"),
                ("⚙️ Configuración", "config"),
                ("👑 Gestión de Usuarios", "usuarios")
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
        
        # Acciones rápidas según rol
        if check_permission("write"):
            if st.button("🔄 Generar Nueva Malla", use_container_width=True):
                st.session_state.malla_actual = pd.DataFrame()
                st.success("Listo para generar nueva malla")
                st.session_state.current_page = "malla"
                st.rerun()
        
        if check_permission("configure"):
            if st.button("🔄 Reinicializar Datos", use_container_width=True):
                conn = get_connection()
                cursor = conn.cursor()
                cursor.execute("DELETE FROM empleados WHERE id > 3")
                cursor.execute("DELETE FROM malla_turnos")
                conn.commit()
                conn.close()
                
                st.session_state.empleados_df = get_empleados()
                st.session_state.codigos_turno = get_codigos_turno()
                st.session_state.configuracion = get_configuracion()
                st.session_state.malla_actual = pd.DataFrame()
                
                st.success("Datos reinicializados")
                st.rerun()

# ============================================================================
# PÁGINAS PARA ADMINISTRADORES
# ============================================================================
def pagina_malla():
    """Página principal - Malla de turnos (editable)"""
    st.markdown("<h1 class='main-header'>📊 Malla de Turnos</h1>", unsafe_allow_html=True)
    
    # Selector de mes y año
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
    
    # Mostrar leyenda
    mostrar_leyenda()
    
    # Verificar si hay malla cargada
    if st.session_state.malla_actual.empty:
        st.warning("⚠️ No hay malla de turnos cargada. Presiona 'Cargar Malla' para ver los datos.")
    else:
        # Mostrar malla
        st.markdown(f"### 📋 Malla de Turnos - {mes_seleccionado} {ano}")
        
        if check_permission("write"):
            # Notificación de guardado
            st.markdown('<div class="auto-save-notice">💡 Los cambios se guardan automáticamente al salir de la celda</div>', unsafe_allow_html=True)
            
            # Crear copia editable
            malla_editable = st.session_state.malla_actual.copy()
            
            # Identificar columnas editables (solo las de días)
            column_config = {}
            day_columns = [col for col in malla_editable.columns if '/' in str(col)]
            
            for col in malla_editable.columns:
                if col in day_columns:
                    column_config[col] = st.column_config.SelectboxColumn(
                        col,
                        width="small",
                        options=[""] + list(st.session_state.codigos_turno.keys()),
                        help="Selecciona el código del turno"
                    )
                elif col in ['N°', 'CC']:
                    column_config[col] = st.column_config.Column(width="small", disabled=True)
                elif col == 'APELLIDOS Y NOMBRES':
                    column_config[col] = st.column_config.Column(width="medium", disabled=True)
                elif col in ['CARGO', 'DEPARTAMENTO', 'ESTADO']:
                    column_config[col] = st.column_config.Column(disabled=True)
            
            # Mostrar editor de datos
            edited_df = st.data_editor(
                malla_editable,
                column_config=column_config,
                hide_index=True,
                use_container_width=True,
                height=600,
                num_rows="fixed",
                key="editor_malla"
            )
            
            # Botones de acción
            col1, col2 = st.columns(2)
            with col1:
                if st.button("💾 Guardar Cambios", use_container_width=True):
                    guardar_malla_turnos(edited_df, mes_numero, ano)
                    st.session_state.last_save = datetime.now()
                    st.session_state.malla_actual = edited_df
                    st.success("✅ Cambios guardados en la base de datos")
                    registrar_log("guardar_malla", f"{mes_seleccionado} {ano}")
                    st.rerun()
            
            with col2:
                if st.button("🔄 Recargar desde BD", use_container_width=True):
                    st.session_state.malla_actual = get_malla_turnos(mes_numero, ano)
                    st.success("✅ Malla recargada desde base de datos")
                    st.rerun()
        else:
            # Modo solo lectura
            st.info("👁️ Vista de solo lectura - No puedes editar")
            styled_df = aplicar_estilo_dataframe(st.session_state.malla_actual)
            st.dataframe(styled_df, use_container_width=True, height=600)
        
        # Estadísticas
        mostrar_estadisticas_malla()

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

def mostrar_estadisticas_malla():
    """Mostrar estadísticas de la malla"""
    if not st.session_state.malla_actual.empty:
        day_columns = [col for col in st.session_state.malla_actual.columns if '/' in str(col)]
        
        if day_columns:
            # Calcular estadísticas
            total_empleados = len(st.session_state.malla_actual)
            
            # Contar turnos por tipo
            turnos_por_tipo = {}
            for col in day_columns:
                for turno in st.session_state.malla_actual[col]:
                    if pd.notna(turno) and turno != "":
                        turnos_por_tipo[turno] = turnos_por_tipo.get(turno, 0) + 1
            
            total_turnos = sum(turnos_por_tipo.values())
            
            # Calcular horas totales
            horas_totales = 0
            for turno, cantidad in turnos_por_tipo.items():
                horas = st.session_state.codigos_turno.get(str(turno), {}).get("horas", 0)
                horas_totales += horas * cantidad
            
            # Mostrar métricas
            st.markdown("---")
            st.markdown("### 📈 Estadísticas de la Malla")
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Empleados", total_empleados)
            with col2:
                st.metric("Total Turnos", total_turnos)
            with col3:
                st.metric("Horas Totales", horas_totales)
            with col4:
                empleados_activos = st.session_state.malla_actual[
                    st.session_state.malla_actual['ESTADO'] == 'Activo'
                ].shape[0]
                st.metric("Empleados Activos", empleados_activos)

# ============================================================================
# PÁGINA DE GESTIÓN DE EMPLEADOS (CORREGIDA)
# ============================================================================
def pagina_empleados():
    """Página de gestión de empleados - VERSIÓN CORREGIDA"""
    if not check_permission("write"):
        st.error("⛔ No tienes permisos para gestionar empleados")
        return
    
    st.markdown("<h1 class='main-header'>👥 Gestión de Empleados</h1>", unsafe_allow_html=True)
    
    # Estadísticas rápidas
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
    
    # Sección para agregar nuevo empleado
    st.markdown("### ➕ Agregar Nuevo Empleado")
    with st.expander("Click para expandir", expanded=False):
        if agregar_empleado():
            st.rerun()
    
    st.markdown("---")
    
    # Lista de empleados existentes
    st.markdown("### 📋 Lista de Empleados")
    
    if st.session_state.empleados_df.empty:
        st.warning("No hay empleados registrados.")
    else:
        # Crear DataFrame para edición SIN la columna 'id'
        df_editable = st.session_state.empleados_df.copy()
        
        # Guardar los IDs en una variable separada
        ids_empleados = df_editable['id'].tolist()
        
        # Quitar la columna 'id' del DataFrame de edición
        df_editable = df_editable.drop(columns=['id'])
        
        # Renombrar columnas
        df_editable = df_editable.rename(columns={
            'numero': 'N°',
            'cargo': 'CARGO',
            'nombre_completo': 'APELLIDOS Y NOMBRES',
            'cedula': 'CC',
            'departamento': 'DEPARTAMENTO',
            'estado': 'ESTADO',
            'hora_inicio': 'HORA_INICIO',
            'hora_fin': 'HORA_FIN'
        })
        
        # Configurar columnas para el editor SIN la columna 'id'
        column_config = {
            "N°": st.column_config.NumberColumn("N°", width="small"),
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
            "HORA_FIN": st.column_config.TextColumn("Hora Fin", width="small")
        }
        
        # Mostrar editor de datos
        edited_df = st.data_editor(
            df_editable,
            column_config=column_config,
            hide_index=True,
            use_container_width=True,
            num_rows="fixed",
            key="editor_empleados"
        )
        
        # Botones de acción
        col1, col2 = st.columns(2)
        with col1:
            if st.button("💾 Guardar Cambios", use_container_width=True):
                # Agregar los IDs de vuelta al DataFrame editado
                edited_df_with_ids = edited_df.copy()
                edited_df_with_ids['id'] = ids_empleados[:len(edited_df)]
                
                guardar_empleados(edited_df_with_ids)
                st.success("✅ Cambios guardados")
                st.rerun()
        
        with col2:
            if st.button("📥 Exportar Empleados", use_container_width=True):
                csv = df_editable.to_csv(index=False)
                st.download_button(
                    label="Descargar CSV",
                    data=csv,
                    file_name="empleados.csv",
                    mime="text/csv",
                    use_container_width=True
                )

def guardar_empleados(df_editado):
    """Guardar empleados en la base de datos"""
    conn = get_connection()
    cursor = conn.cursor()
    
    for _, row in df_editado.iterrows():
        # Verificar que la fila tenga un ID
        if 'id' not in row or pd.isna(row['id']):
            continue
            
        cursor.execute('''
            UPDATE empleados 
            SET numero = ?, cargo = ?, nombre_completo = ?, cedula = ?, 
                departamento = ?, estado = ?, hora_inicio = ?, hora_fin = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (
            row['N°'], row['CARGO'], row['APELLIDOS Y NOMBRES'], row['CC'],
            row['DEPARTAMENTO'], row['ESTADO'], row['HORA_INICIO'], row['HORA_FIN'],
            int(row['id'])
        ))
    
    conn.commit()
    conn.close()
    
    # Actualizar session state
    st.session_state.empleados_df = get_empleados()
    st.session_state.last_save = datetime.now()
    registrar_log("actualizar_empleados", f"{len(df_editado)} empleados")

def agregar_empleado():
    """Agregar nuevo empleado a la base de datos"""
    with st.form("form_nuevo_empleado"):
        st.markdown("### 📝 Agregar Nuevo Empleado")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Obtener siguiente número
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(numero) FROM empleados")
            max_num = cursor.fetchone()[0]
            nuevo_numero = (max_num or 0) + 1
            conn.close()
            
            cargo = st.text_input("Cargo*", placeholder="Ej: JEFE DE TIENDA")
            nombre = st.text_input("Apellidos y Nombres*", placeholder="Ej: GARCIA JUAN")
        
        with col2:
            cc = st.text_input("Cédula de Ciudadanía*", placeholder="Ej: 1234567890")
            departamento = st.selectbox("Departamento*", st.session_state.configuracion['departamentos'])
            estado = st.selectbox("Estado*", ["Activo", "Vacaciones", "Licencia", "Inactivo"])
        
        # Horarios opcionales
        st.markdown("**Horarios (Opcional)**")
        col3, col4 = st.columns(2)
        with col3:
            hora_inicio = st.text_input("Hora Inicio", placeholder="Ej: 06:00")
        with col4:
            hora_fin = st.text_input("Hora Fin", placeholder="Ej: 14:00")
        
        submitted = st.form_submit_button("💾 Guardar Empleado")
        
        if submitted:
            # Validar campos obligatorios
            if not all([cargo, nombre, cc, departamento]):
                st.error("Por favor complete todos los campos obligatorios (*)")
                return False
            
            # Verificar si la cédula ya existe
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM empleados WHERE cedula = ?", (cc,))
            if cursor.fetchone()[0] > 0:
                st.error("❌ Ya existe un empleado con esta cédula")
                conn.close()
                return False
            
            # Insertar nuevo empleado
            cursor.execute('''
                INSERT INTO empleados 
                (numero, cargo, nombre_completo, cedula, departamento, estado, hora_inicio, hora_fin)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                nuevo_numero, cargo.upper(), nombre.upper(), cc, 
                departamento, estado, hora_inicio, hora_fin
            ))
            
            conn.commit()
            conn.close()
            
            st.success(f"✅ Empleado {nombre.upper()} agregado correctamente")
            registrar_log("agregar_empleado", f"{nombre.upper()} - {cargo}")
            
            # Actualizar datos en session state
            st.session_state.empleados_df = get_empleados()
            return True
    
    return False

# ============================================================================
# PÁGINA DE GESTIÓN DE USUARIOS
# ============================================================================
def pagina_usuarios():
    """Página de gestión de usuarios para administradores"""
    if not check_permission("manage_users"):
        st.error("⛔ No tienes permisos para gestionar usuarios")
        return
    
    st.markdown("<h1 class='main-header'>👑 Gestión de Usuarios</h1>", unsafe_allow_html=True)
    
    # Mostrar usuarios existentes
    st.markdown("### 📋 Usuarios del Sistema")
    
    usuarios_df = get_usuarios()
    
    if usuarios_df.empty:
        st.warning("No hay usuarios registrados en el sistema.")
    else:
        # Mostrar tabla de usuarios
        st.dataframe(
            usuarios_df[['username', 'nombre', 'role', 'departamento']],
            hide_index=True,
            use_container_width=True,
            column_config={
                "username": st.column_config.TextColumn("Usuario", width="small"),
                "nombre": st.column_config.TextColumn("Nombre", width="medium"),
                "role": st.column_config.SelectboxColumn(
                    "Rol",
                    options=list(ROLES.keys()),
                    width="small"
                ),
                "departamento": st.column_config.SelectboxColumn(
                    "Departamento",
                    options=st.session_state.configuracion['departamentos'],
                    width="medium"
                )
            }
        )
    
    # Sección para agregar/editar usuario
    st.markdown("---")
    st.markdown("### ➕ Agregar/Editar Usuario")
    
    with st.form("form_usuario"):
        col1, col2 = st.columns(2)
        
        with col1:
            username = st.text_input("Usuario*", placeholder="Ej: juan.perez")
            nombre = st.text_input("Nombre Completo*", placeholder="Ej: Juan Pérez García")
            rol = st.selectbox("Rol*", list(ROLES.keys()))
        
        with col2:
            password = st.text_input("Contraseña*", type="password", placeholder="Mínimo 6 caracteres")
            confirm_password = st.text_input("Confirmar Contraseña*", type="password")
            departamento = st.selectbox("Departamento", st.session_state.configuracion['departamentos'])
        
        submitted = st.form_submit_button("💾 Guardar Usuario")
        
        if submitted:
            # Validaciones
            if not all([username, nombre, password, confirm_password, rol]):
                st.error("Por favor complete todos los campos obligatorios (*)")
                return
            
            if len(password) < 6:
                st.error("La contraseña debe tener al menos 6 caracteres")
                return
            
            if password != confirm_password:
                st.error("Las contraseñas no coinciden")
                return
            
            conn = get_connection()
            cursor = conn.cursor()
            
            # Verificar si el usuario ya existe
            cursor.execute("SELECT COUNT(*) FROM usuarios WHERE username = ?", (username,))
            user_exists = cursor.fetchone()[0] > 0
            
            password_hash = hashlib.sha256(password.encode()).hexdigest()
            
            if user_exists:
                # Actualizar usuario
                cursor.execute('''
                    UPDATE usuarios 
                    SET password_hash = ?, role = ?, nombre = ?, departamento = ?
                    WHERE username = ?
                ''', (password_hash, rol, nombre, departamento, username))
                mensaje = f"✅ Usuario {username} actualizado correctamente"
                accion_log = "actualizar_usuario"
            else:
                # Crear usuario
                cursor.execute('''
                    INSERT INTO usuarios (username, password_hash, role, nombre, departamento)
                    VALUES (?, ?, ?, ?, ?)
                ''', (username, password_hash, rol, nombre, departamento))
                mensaje = f"✅ Usuario {username} creado correctamente"
                accion_log = "crear_usuario"
            
            conn.commit()
            conn.close()
            
            st.success(mensaje)
            registrar_log(accion_log, f"Usuario: {username}, Rol: {rol}")
            st.rerun()
    
    # Sección para eliminar usuario
    st.markdown("---")
    st.markdown("### ❌ Eliminar Usuario")
    
    if not usuarios_df.empty:
        usuarios_lista = usuarios_df['username'].tolist()
        # No permitir eliminar al administrador principal
        if 'admin' in usuarios_lista:
            usuarios_lista.remove('admin')
        
        if usuarios_lista:
            usuario_a_eliminar = st.selectbox("Seleccionar usuario para eliminar", usuarios_lista)
            
            if st.button("🗑️ Eliminar Usuario", use_container_width=True):
                if usuario_a_eliminar != st.session_state.auth['username']:
                    conn = get_connection()
                    cursor = conn.cursor()
                    cursor.execute("DELETE FROM usuarios WHERE username = ?", (usuario_a_eliminar,))
                    conn.commit()
                    conn.close()
                    
                    st.success(f"✅ Usuario {usuario_a_eliminar} eliminado")
                    registrar_log("eliminar_usuario", f"Usuario: {usuario_a_eliminar}")
                    st.rerun()
                else:
                    st.error("No puedes eliminarte a ti mismo")
        else:
            st.info("No hay usuarios que puedan ser eliminados (excepto admin)")
    else:
        st.info("No hay usuarios registrados")

# ============================================================================
# PÁGINA DE CONFIGURACIÓN
# ============================================================================
def pagina_configuracion():
    """Página de configuración"""
    if not check_permission("configure"):
        st.error("⛔ No tienes permisos para acceder a la configuración")
        return
    
    st.markdown("<h1 class='main-header'>⚙️ Configuración</h1>", unsafe_allow_html=True)
    
    tab1, tab2, tab3 = st.tabs(["Códigos de Turno", "General", "Backup"])
    
    with tab1:
        st.markdown("### Configurar Códigos de Turno")
        
        # Obtener códigos actuales
        conn = get_connection()
        codigos_df = pd.read_sql("SELECT * FROM codigos_turno", conn)
        conn.close()
        
        if not codigos_df.empty:
            # Opción A: Usar TextColumn (compatible con versiones antiguas)
            edited_codigos = st.data_editor(
                codigos_df,
                column_config={
                    "codigo": st.column_config.TextColumn("Código", width="small", required=True),
                    "nombre": st.column_config.TextColumn("Descripción", width="medium", required=True),
                    # REEMPLAZADO: ColorPickerColumn por TextColumn
                    "color": st.column_config.TextColumn("Color (hex: #RRGGBB)", width="medium", required=True),
                    "horas": st.column_config.NumberColumn("Horas", min_value=0, max_value=24, required=True)
                },
                num_rows="fixed",
                use_container_width=True,
                key="editor_codigos"
            )
            
            # Botón para guardar cambios
            if st.button("💾 Guardar Configuración de Códigos", use_container_width=True):
                conn = get_connection()
                cursor = conn.cursor()
                
                # Eliminar todos los códigos existentes (excepto los que se están editando)
                cursor.execute("DELETE FROM codigos_turno")
                
                # Insertar los códigos editados
                for _, row in edited_codigos.iterrows():
                    cursor.execute(
                        "INSERT INTO codigos_turno (codigo, nombre, color, horas) VALUES (?, ?, ?, ?)",
                        (row['codigo'], row['nombre'], row['color'], int(row['horas']))
                    )
                
                conn.commit()
                conn.close()
                
                # Actualizar session state
                st.session_state.codigos_turno = get_codigos_turno()
                st.success("✅ Configuración de códigos guardada")
                registrar_log("actualizar_codigos", f"{len(edited_codigos)} códigos")
                st.rerun()
        
        # Formulario para agregar nuevo código con color picker manual
        st.markdown("---")
        st.markdown("#### Agregar Nuevo Código")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            nuevo_codigo = st.text_input("Código", placeholder="Ej: 99", key="nuevo_codigo")
        with col2:
            nuevo_nombre = st.text_input("Nombre", placeholder="Ej: Turno Especial", key="nuevo_nombre")
        with col3:
            # Color picker manual usando st.color_picker (si está disponible)
            if hasattr(st, 'color_picker'):
                nuevo_color = st.color_picker("Color", "#000000", key="nuevo_color")
            else:
                # Fallback: input de texto para color hex
                nuevo_color = st.text_input("Color (hex)", "#000000", key="nuevo_color")
        
        horas_nuevo = st.number_input("Horas", min_value=0, max_value=24, value=8, key="horas_nuevo")
        
        if st.button("➕ Agregar Código", key="btn_agregar_codigo"):
            if nuevo_codigo and nuevo_nombre:
                conn = get_connection()
                cursor = conn.cursor()
                
                cursor.execute(
                    "INSERT OR REPLACE INTO codigos_turno (codigo, nombre, color, horas) VALUES (?, ?, ?, ?)",
                    (nuevo_codigo, nuevo_nombre, nuevo_color, horas_nuevo)
                )
                
                conn.commit()
                conn.close()
                
                # Actualizar session state
                st.session_state.codigos_turno = get_codigos_turno()
                st.success(f"✅ Código {nuevo_codigo} agregado")
                st.rerun()
    
    with tab2:
        st.markdown("### Configuración General")
        
        col1, col2 = st.columns(2)
        
        with col1:
            formato_hora = st.selectbox("Formato de hora", ["24 horas", "12 horas (AM/PM)"])
            dias_vacaciones = st.number_input("Días de vacaciones por año", min_value=0, max_value=30, value=15)
        
        with col2:
            inicio_semana = st.selectbox("Inicio de semana", ["Lunes", "Domingo"])
            departamentos_text = st.text_area(
                "Departamentos (separados por comas)",
                value=",".join(st.session_state.configuracion['departamentos'])
            )
        
        if st.button("💾 Guardar Configuración General", use_container_width=True):
            conn = get_connection()
            cursor = conn.cursor()
            
            # Actualizar configuración
            config_updates = [
                ("formato_hora", formato_hora, "text"),
                ("dias_vacaciones", str(dias_vacaciones), "number"),
                ("inicio_semana", inicio_semana, "text"),
                ("departamentos", departamentos_text, "list")
            ]
            
            for clave, valor, tipo in config_updates:
                cursor.execute('''
                    INSERT OR REPLACE INTO configuracion (clave, valor, tipo)
                    VALUES (?, ?, ?)
                ''', (clave, valor, tipo))
            
            conn.commit()
            conn.close()
            
            # Actualizar session state
            st.session_state.configuracion = get_configuracion()
            st.success("✅ Configuración general guardada")
            registrar_log("actualizar_configuracion", "configuración general")
    
    with tab3:
        st.markdown("### 📦 Sistema de Backup")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Exportar Datos")
            if st.button("📥 Exportar Todos los Datos", use_container_width=True):
                # Recopilar todos los datos
                datos = {
                    'empleados': get_empleados().to_dict('records'),
                    'codigos_turno': pd.read_sql("SELECT * FROM codigos_turno", get_connection()).to_dict('records'),
                    'usuarios': get_usuarios().to_dict('records'),
                    'configuracion': get_configuracion(),
                    'export_date': datetime.now().isoformat()
                }
                
                json_str = json.dumps(datos, indent=2, ensure_ascii=False)
                
                st.download_button(
                    label="Descargar Backup",
                    data=json_str,
                    file_name=f"backup_turnos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    mime="application/json",
                    use_container_width=True
                )
        
        with col2:
            st.markdown("#### Importar Datos")
            uploaded_file = st.file_uploader("Seleccionar archivo JSON", type=['json'])
            
            if uploaded_file is not None:
                try:
                    datos = json.load(uploaded_file)
                    
                    if st.button("🔄 Restaurar Backup", use_container_width=True):
                        conn = get_connection()
                        cursor = conn.cursor()
                        
                        # Restaurar datos
                        if 'empleados' in datos:
                            # Limpiar tabla de empleados
                            cursor.execute("DELETE FROM empleados")
                            # Insertar empleados
                            for emp in datos['empleados']:
                                cursor.execute('''
                                    INSERT INTO empleados 
                                    (numero, cargo, nombre_completo, cedula, departamento, estado, hora_inicio, hora_fin)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                ''', (
                                    emp.get('numero'), emp.get('cargo'), emp.get('nombre_completo'),
                                    emp.get('cedula'), emp.get('departamento'), emp.get('estado'),
                                    emp.get('hora_inicio'), emp.get('hora_fin')
                                ))
                        
                        if 'codigos_turno' in datos:
                            # Limpiar tabla de códigos
                            cursor.execute("DELETE FROM codigos_turno")
                            # Insertar códigos
                            for codigo in datos['codigos_turno']:
                                cursor.execute('''
                                    INSERT INTO codigos_turno (codigo, nombre, color, horas)
                                    VALUES (?, ?, ?, ?)
                                ''', (
                                    codigo.get('codigo'), codigo.get('nombre'),
                                    codigo.get('color'), codigo.get('horas')
                                ))
                        
                        conn.commit()
                        conn.close()
                        
                        # Actualizar session state
                        st.session_state.empleados_df = get_empleados()
                        st.session_state.codigos_turno = get_codigos_turno()
                        
                        st.success("✅ Backup restaurado correctamente")
                        st.rerun()
                except Exception as e:
                    st.error(f"❌ Error al leer el archivo JSON: {str(e)}")

# ============================================================================
# PÁGINAS PARA EMPLEADOS (SOLO LECTURA)
# ============================================================================
def pagina_mis_turnos():
    """Página para que los empleados vean SUS turnos (solo lectura)"""
    st.markdown("<h1 class='main-header'>📅 Mis Turnos</h1>", unsafe_allow_html=True)
    
    if not st.session_state.empleado_actual:
        st.warning("⚠️ No se encontró tu registro como empleado.")
        return
    
    empleado_info = st.session_state.empleado_actual
    
    # Seleccionar mes y año
    col1, col2 = st.columns(2)
    with col1:
        meses = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", 
                "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
        mes_seleccionado = st.selectbox("Mes:", meses, index=st.session_state.mes_actual - 1)
        mes_numero = meses.index(mes_seleccionado) + 1
    
    with col2:
        ano = st.selectbox("Año:", [2026, 2025, 2024, 2027], index=0)
    
    # Cargar turnos del empleado
    if st.button("📅 Cargar Mis Turnos", use_container_width=True):
        # Guardar mes y año para el calendario
        st.session_state.calendario_mes = mes_numero
        st.session_state.calendario_ano = ano
        
        # Obtener turnos del empleado
        turnos_dict = get_turnos_empleado_mes(empleado_info['id'], mes_numero, ano)
        
        if not turnos_dict:
            st.info(f"ℹ️ No tienes turnos asignados para {mes_seleccionado} {ano}.")
            return
        
        # Mostrar información personal
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Cargo", empleado_info['cargo'])
        with col2:
            st.metric("Departamento", empleado_info['departamento'])
        with col3:
            st.metric("Estado", empleado_info['estado'])
        
        # Convertir a lista para mostrar
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
            
            # Mostrar como tabla
            st.markdown(f"### 📋 Mis Turnos - {mes_seleccionado} {ano}")
            st.dataframe(
                df_calendario[['Día', 'Turno', 'Horas']],
                hide_index=True,
                use_container_width=True
            )
            
            # Estadísticas personales
            st.markdown("---")
            st.markdown("### 📈 Mis Estadísticas")
            
            total_horas = sum(t['Horas'] for t in turnos_lista)
            total_turnos = len(turnos_lista)
            
            # Determinar número de días en el mes
            if mes_numero == 2 and ano == 2026:
                dias_mes = 28
            elif mes_numero in [4, 6, 9, 11]:
                dias_mes = 30
            else:
                dias_mes = 31
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Turnos Asignados", total_turnos)
            with col2:
                st.metric("Horas Totales", total_horas)
            with col3:
                promedio = total_horas / max(total_turnos, 1)
                st.metric("Promedio Horas/Turno", f"{promedio:.1f}")
            with col4:
                porcentaje = (total_turnos / dias_mes * 100) if dias_mes > 0 else 0
                st.metric("Días con Turno", f"{porcentaje:.1f}%")
        else:
            st.info(f"ℹ️ No tienes turnos asignados para {mes_seleccionado} {ano}.")

# ============================================================================
# PÁGINA DE CALENDARIO SIMPLIFICADA (USANDO STREAMLIT NATIVO)
# ============================================================================
def generar_calendario_simple(mes, ano, turnos_dict):
    """Generar calendario simple usando componentes nativos de Streamlit"""
    # Determinar número de días en el mes
    if mes == 2 and ano == 2026:
        num_dias = 28
    elif mes in [4, 6, 9, 11]:
        num_dias = 30
    else:
        num_dias = 31
    
    # Días de la semana
    dias_semana = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"]
    
    # Primer día del mes
    primer_dia = date(ano, mes, 1)
    dia_inicio_semana = primer_dia.weekday()  # 0=Lunes, 6=Domingo
    
    # Crear calendario con columnas
    st.markdown("#### 🗓️ Calendario del Mes")
    
    # Encabezados de días
    cols = st.columns(7)
    for idx, dia in enumerate(dias_semana):
        with cols[idx]:
            st.markdown(f"**{dia}**")
    
    # Espacios en blanco para días antes del primer día
    if dia_inicio_semana > 0:
        cols = st.columns(7)
        for i in range(dia_inicio_semana):
            with cols[i]:
                st.markdown(" ")
    
    # Días del mes
    dia_actual = 0
    for dia in range(1, num_dias + 1):
        # Crear nuevas columnas cada 7 días
        if dia_actual % 7 == 0:
            cols = st.columns(7)
        
        col_idx = dia_actual % 7
        
        with cols[col_idx]:
            codigo = turnos_dict.get(dia, "")
            turno_info = st.session_state.codigos_turno.get(str(codigo), {"color": "#FFFFFF", "nombre": "Sin turno"})
            color = turno_info["color"]
            nombre_turno = turno_info.get("nombre", "Sin turno")
            
            # Mostrar día con color según turno
            st.markdown(f"""
            <div style="background-color: {color}; padding: 8px; border-radius: 5px; 
                       text-align: center; margin: 2px; border: 1px solid #e0e0e0;">
                <div style="font-weight: bold; font-size: 1.1em;">{dia}</div>
                <div style="font-size: 0.7em;">{nombre_turno if codigo else "Libre"}</div>
            </div>
            """, unsafe_allow_html=True)
        
        dia_actual += 1

def pagina_calendario():
    """Página de calendario visual simplificada"""
    st.markdown("<h1 class='main-header'>📆 Mi Calendario</h1>", unsafe_allow_html=True)
    
    if not st.session_state.empleado_actual:
        st.warning("⚠️ No se encontró tu registro como empleado.")
        return
    
    empleado_info = st.session_state.empleado_actual
    
    # Selector de mes y año
    col1, col2 = st.columns(2)
    with col1:
        meses = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", 
                "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
        mes_seleccionado = st.selectbox("Mes:", meses, index=st.session_state.calendario_mes - 1)
        mes_numero = meses.index(mes_seleccionado) + 1
    
    with col2:
        ano = st.selectbox("Año:", [2026, 2025, 2024, 2027], index=0)
    
    # Obtener turnos del empleado para el mes seleccionado
    turnos_dict = get_turnos_empleado_mes(empleado_info['id'], mes_numero, ano)
    
    # Generar y mostrar calendario
    generar_calendario_simple(mes_numero, ano, turnos_dict)
    
    # Información del empleado
    st.markdown("---")
    st.markdown("### 👤 Mi Información")
    
    col1, col2 = st.columns(2)
    with col1:
        st.info(f"""
        **Nombre:** {empleado_info['nombre_completo']}  
        **Cargo:** {empleado_info['cargo']}  
        **Departamento:** {empleado_info['departamento']}
        """)
    
    with col2:
        st.info(f"""
        **Cédula:** {empleado_info['cedula']}  
        **Estado:** {empleado_info['estado']}  
        **Horario base:** {empleado_info.get('hora_inicio', 'N/A')} - {empleado_info.get('hora_fin', 'N/A')}
        """)

# ============================================================================
# PÁGINA DE INFORMACIÓN PERSONAL
# ============================================================================
def pagina_mi_info():
    """Página de información personal del empleado"""
    st.markdown("<h1 class='main-header'>👤 Mi Información</h1>", unsafe_allow_html=True)
    
    if not st.session_state.empleado_actual:
        st.warning("⚠️ No se encontró tu registro como empleado.")
        return
    
    empleado_info = st.session_state.empleado_actual
    
    # Mostrar información en tarjetas
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📋 Datos Personales")
        st.markdown(f"""
        <div style="background-color: #f8f9fa; padding: 20px; border-radius: 10px; border: 1px solid #dee2e6;">
            <p><strong>Nombre Completo:</strong> {empleado_info['nombre_completo']}</p>
            <p><strong>Cédula:</strong> {empleado_info['cedula']}</p>
            <p><strong>Cargo:</strong> {empleado_info['cargo']}</p>
            <p><strong>Departamento:</strong> {empleado_info['departamento']}</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("### ⚙️ Datos Laborales")
        st.markdown(f"""
        <div style="background-color: #f8f9fa; padding: 20px; border-radius: 10px; border: 1px solid #dee2e6;">
            <p><strong>Estado:</strong> {empleado_info['estado']}</p>
            <p><strong>Número:</strong> {empleado_info['numero']}</p>
            <p><strong>Horario Base:</strong> {empleado_info.get('hora_inicio', 'N/A')} - {empleado_info.get('hora_fin', 'N/A')}</p>
            <p><strong>Fecha Registro:</strong> {empleado_info.get('created_at', 'N/A')}</p>
        </div>
        """, unsafe_allow_html=True)
    
    # Turnos del mes actual (vista rápida)
    st.markdown("---")
    st.markdown("### 📅 Mis Turnos del Mes Actual")
    
    mes_actual = datetime.now().month
    ano_actual = datetime.now().year
    
    turnos_actuales = get_turnos_empleado_mes(empleado_info['id'], mes_actual, ano_actual)
    
    if turnos_actuales:
        # Calcular estadísticas
        total_turnos = len(turnos_actuales)
        horas_totales = 0
        
        for dia, codigo in turnos_actuales.items():
            if codigo:
                horas = st.session_state.codigos_turno.get(str(codigo), {}).get("horas", 0)
                horas_totales += horas
        
        # Mostrar estadísticas
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Turnos Asignados", total_turnos)
        with col2:
            st.metric("Horas Totales", horas_totales)
        with col3:
            if total_turnos > 0:
                promedio = horas_totales / total_turnos
                st.metric("Promedio Horas/Turno", f"{promedio:.1f}")
            else:
                st.metric("Promedio Horas/Turno", "0")
        
        # Lista de turnos próximos (próximos 7 días)
        st.markdown("#### 🔜 Próximos Turnos")
        hoy = datetime.now().day
        
        proximos_turnos = []
        for dia, codigo in turnos_actuales.items():
            if dia >= hoy and codigo:
                turno_info = st.session_state.codigos_turno.get(str(codigo), {})
                proximos_turnos.append({
                    'Día': f"{dia}/{mes_actual}/{ano_actual}",
                    'Turno': turno_info.get('nombre', 'Desconocido'),
                    'Horas': turno_info.get('horas', 0)
                })
        
        if proximos_turnos:
            df_proximos = pd.DataFrame(proximos_turnos[:7])  # Mostrar solo 7 próximos
            st.dataframe(df_proximos, hide_index=True, use_container_width=True)
        else:
            st.info("No tienes turnos asignados para los próximos días.")
    else:
        st.info(f"No tienes turnos asignados para {mes_actual}/{ano_actual}.")

# ============================================================================
# RUTA PRINCIPAL DE LA APLICACIÓN
# ============================================================================
def main():
    """Función principal que gestiona toda la aplicación"""
    # Inicializar session state
    inicializar_session_state()
    
    # Verificar autenticación
    if not st.session_state.auth['is_authenticated']:
        pagina_login()
        return
    
    # Mostrar barra de usuario
    mostrar_barra_usuario()
    
    # Mostrar sidebar según rol
    mostrar_sidebar()
    
    # Mostrar página según selección
    pagina_actual = st.session_state.current_page
    
    if pagina_actual == "malla":
        pagina_malla()
    elif pagina_actual == "empleados":
        pagina_empleados()
    elif pagina_actual == "config":
        pagina_configuracion()
    elif pagina_actual == "usuarios":
        pagina_usuarios()
    elif pagina_actual == "mis_turnos":
        pagina_mis_turnos()
    elif pagina_actual == "calendario":
        pagina_calendario()
    elif pagina_actual == "mi_info":
        pagina_mi_info()
    
    # Pie de página
    st.markdown("---")
    st.markdown(
        "<div style='text-align: center; color: #6c757d; padding: 20px;'>"
        "📊 Creado por Edwin Merchán | © 2026 | Versión 2.0"
        "</div>",
        unsafe_allow_html=True
    )

# ============================================================================
# EJECUCIÓN PRINCIPAL
# ============================================================================
if __name__ == "__main__":
    main()