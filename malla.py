# app.py - Sistema Completo de Gestión de Turnos con Autenticación y SQLite
# VERSIÓN CORREGIDA CON BACKUP Y GUARDADO PERMANENTE

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
import shutil
import time

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
# CSS PERSONALIZADO (Mismo que antes)
# ============================================================================
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1E3A8A;
        text-align: center;
        margin-bottom: 2rem;
    }
    /* ... (todo el CSS anterior se mantiene igual) ... */
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

def get_connection():
    """Obtener conexión a la base de datos"""
    return sqlite3.connect(DB_NAME)

def actualizar_estructura_bd():
    """Actualizar la estructura de la base de datos si faltan columnas"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Verificar si existe la columna 'updated_at' en malla_turnos
        cursor.execute("PRAGMA table_info(malla_turnos)")
        columnas = cursor.fetchall()
        columnas_nombres = [col[1] for col in columnas]
        
        if 'updated_at' not in columnas_nombres:
            st.warning("⚠️ Actualizando estructura de la base de datos...")
            # Agregar columna updated_at
            cursor.execute('''
                ALTER TABLE malla_turnos 
                ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ''')
            st.success("✅ Columna 'updated_at' agregada a malla_turnos")
        
        # También verificar otras columnas importantes
        cursor.execute("PRAGMA table_info(empleados)")
        columnas_empleados = cursor.fetchall()
        columnas_emp_nombres = [col[1] for col in columnas_empleados]
        
        if 'updated_at' not in columnas_emp_nombres:
            cursor.execute('''
                ALTER TABLE empleados 
                ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ''')
            st.success("✅ Columna 'updated_at' agregada a empleados")
        
        conn.commit()
        conn.close()
        return True
        
    except Exception as e:
        st.error(f"❌ Error al actualizar estructura BD: {str(e)}")
        return False

# ============================================================================
# SISTEMA DE BACKUP AUTOMÁTICO
# ============================================================================
def crear_backup_automatico():
    """Crear backup automático de la base de datos"""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = Path("backups")
        backup_dir.mkdir(exist_ok=True)
        
        backup_file = backup_dir / f"turnos_backup_{timestamp}.db"
        shutil.copy2(DB_NAME, backup_file)
        
        # Mantener solo los últimos 10 backups
        backups = sorted(backup_dir.glob("turnos_backup_*.db"))
        if len(backups) > 10:
            for old_backup in backups[:-10]:
                old_backup.unlink()
        
        print(f"✅ Backup automático creado: {backup_file.name}")
        return backup_file
        
    except Exception as e:
        print(f"❌ Error en backup automático: {str(e)}")
        return None

def restaurar_backup(backup_file):
    """Restaurar base de datos desde backup"""
    try:
        # Hacer backup de la base actual antes de restaurar
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_rescue")
        rescue_file = Path(f"backups/rescue_{timestamp}.db")
        shutil.copy2(DB_NAME, rescue_file)
        
        # Restaurar desde backup
        shutil.copy2(backup_file, DB_NAME)
        
        # Re-inicializar session state
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

def exportar_backup_json():
    """Exportar todos los datos a JSON"""
    try:
        datos = {
            'empleados': get_empleados().to_dict('records'),
            'codigos_turno': pd.read_sql("SELECT * FROM codigos_turno", get_connection()).to_dict('records'),
            'usuarios': get_usuarios().to_dict('records'),
            'malla_turnos': pd.read_sql("SELECT * FROM malla_turnos", get_connection()).to_dict('records'),
            'configuracion': get_configuracion(),
            'export_date': datetime.now().isoformat(),
            'version': '2.0'
        }
        
        return json.dumps(datos, indent=2, ensure_ascii=False)
        
    except Exception as e:
        print(f"❌ Error al exportar JSON: {str(e)}")
        return None

def importar_backup_json(json_str):
    """Importar datos desde JSON"""
    try:
        datos = json.loads(json_str)
        
        conn = get_connection()
        cursor = conn.cursor()
        
        # 1. Hacer backup de los datos actuales
        crear_backup_automatico()
        
        # 2. Limpiar tablas
        cursor.execute("DELETE FROM malla_turnos")
        cursor.execute("DELETE FROM empleados")
        cursor.execute("DELETE FROM codigos_turno")
        cursor.execute("DELETE FROM usuarios")
        cursor.execute("DELETE FROM configuracion")
        
        # 3. Restaurar empleados
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
        
        # 4. Restaurar códigos
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
        
        # 5. Restaurar usuarios (sin contraseñas, se pedirán nuevas)
        if 'usuarios' in datos:
            for user in datos['usuarios']:
                # Si no tiene password_hash, crear uno por defecto
                password_hash = user.get('password_hash')
                if not password_hash:
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
        
        # 6. Restaurar malla de turnos
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
        
        # 7. Restaurar configuración
        if 'configuracion' in datos:
            config = datos['configuracion']
            if isinstance(config, dict):
                for clave, valor in config.items():
                    if clave != 'departamentos':
                        cursor.execute('''
                            INSERT OR REPLACE INTO configuracion (clave, valor, tipo)
                            VALUES (?, ?, ?)
                        ''', (clave, str(valor), 'text'))
        
        conn.commit()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"❌ Error al importar JSON: {str(e)}")
        return False

# ============================================================================
# FUNCIONES DE BACKUP PARA GUARDADO
# ============================================================================
def guardar_malla_turnos_con_backup(df_malla, mes, ano):
    """Guardar malla de turnos con backup automático"""
    # Crear backup antes de guardar
    backup_file = crear_backup_automatico()
    
    # Guardar malla
    resultado = guardar_malla_turnos(df_malla, mes, ano)
    
    if resultado > 0 and backup_file:
        st.info(f"📦 Se creó backup automático: {backup_file.name}")
    
    return resultado

def guardar_empleados_con_backup(df_editado):
    """Guardar empleados con backup automático"""
    # Crear backup antes de guardar
    backup_file = crear_backup_automatico()
    
    # Guardar empleados
    cambios, nuevos = guardar_empleados(df_editado)
    
    if cambios > 0 and backup_file:
        st.info(f"📦 Backup creado: {backup_file.name}")
    
    return cambios, nuevos

def guardar_usuarios_con_backup(edited_df, original_df):
    """Guardar usuarios con backup automático"""
    # Crear backup antes de guardar
    backup_file = crear_backup_automatico()
    
    # Guardar usuarios
    cambios = guardar_usuarios(edited_df, original_df)
    
    if cambios > 0 and backup_file:
        st.info(f"📦 Backup creado: {backup_file.name}")
    
    return cambios

# ============================================================================
# FUNCIONES DE BASE DE DATOS (CORREGIDAS)
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
# FUNCIONES DE ACCESO A DATOS (CORREGIDAS)
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
    num_dias = calendar.monthrange(ano, mes)[1]
    
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
    """Guardar malla de turnos en la base de datos - VERSIÓN CORREGIDA"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Obtener empleados
        empleados_df = get_empleados()
        id_por_cedula = {}
        for _, emp in empleados_df.iterrows():
            id_por_cedula[str(emp['cedula'])] = emp['id']
        
        # Determinar número de días en el mes
        num_dias = calendar.monthrange(ano, mes)[1]
        
        # Contador de cambios
        cambios_guardados = 0
        
        # Para cada empleado y cada día, guardar el turno
        for idx, row in df_malla.iterrows():
            cedula = str(row.get('CC', ''))
            if not cedula or cedula not in id_por_cedula:
                st.error(f"❌ No se encontró empleado con cédula: {cedula}")
                continue
            
            emp_id = id_por_cedula[cedula]
            
            for dia in range(1, num_dias + 1):
                col_name = f'{dia}/{mes}/{ano}'
                if col_name in row:
                    codigo = row[col_name]
                    
                    # Manejar valores NaN, None o vacíos
                    if pd.isna(codigo) or codigo is None or str(codigo).strip() == '':
                        codigo_valor = None
                    else:
                        codigo_valor = str(codigo).strip()
                    
                    # Insertar o actualizar
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
    """Guardar empleados en la base de datos - CORREGIDA"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cambios_realizados = 0
        
        for _, row in df_editado.iterrows():
            # Verificar si es empleado existente (tiene ID_OCULTO)
            if 'ID_OCULTO' in row and pd.notna(row['ID_OCULTO']):
                # Es empleado existente - ACTUALIZAR
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
                    str(row['HORA_INICIO']) if pd.notna(row['HORA_INICIO']) else None,
                    str(row['HORA_FIN']) if pd.notna(row['HORA_FIN']) else None,
                    int(row['ID_OCULTO'])
                ))
                
                cambios_realizados += cursor.rowcount
                
            else:
                # Es empleado NUEVO - INSERTAR
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
        
        # Actualizar session state
        st.session_state.empleados_df = get_empleados()
        
        return cambios_realizados, []
        
    except Exception as e:
        print(f"❌ Error al guardar empleados: {str(e)}")
        return 0, []

def guardar_usuarios(edited_df, original_df):
    """Guardar cambios en usuarios - NUEVA FUNCIÓN"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cambios = 0
        
        for _, edited_row in edited_df.iterrows():
            username = edited_row['USUARIO']
            
            # Buscar usuario original
            original_row = original_df[original_df['username'] == username]
            
            if not original_row.empty:
                original_row = original_row.iloc[0]
                
                # Verificar si hay cambios
                cambios_detectados = False
                
                # Comparar campos
                if str(edited_row['NOMBRE_COMPLETO']) != str(original_row['nombre']):
                    cambios_detectados = True
                elif str(edited_row['ROL']) != str(original_row['role']):
                    cambios_detectados = True
                elif str(edited_row['DEPARTAMENTO']) != str(original_row.get('departamento', '')):
                    cambios_detectados = True
                
                if cambios_detectados:
                    # Actualizar usuario
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
# OTRAS FUNCIONES DE DATOS (MANTENIDAS)
# ============================================================================
def get_turnos_empleado_mes(empleado_id, mes, ano):
    """Obtener todos los turnos de un empleado para un mes específico"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Verificar parámetros
        if not empleado_id:
            return {}
        
        # Consultar turnos
        cursor.execute('''
            SELECT dia, codigo_turno 
            FROM malla_turnos 
            WHERE empleado_id = ? AND mes = ? AND ano = ?
            ORDER BY dia
        ''', (empleado_id, mes, ano))
        
        resultados = cursor.fetchall()
        conn.close()
        
        # Convertir a diccionario
        turnos = {}
        for dia, codigo in resultados:
            turnos[dia] = codigo if codigo else ""
        
        return turnos
        
    except Exception as e:
        print(f"Error en get_turnos_empleado_mes: {str(e)}")
        return {}

# ============================================================================
# ROLES Y PERMISOS (MANTENIDOS)
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
# INICIALIZACIÓN DE SESSION STATE (CORREGIDA)
# ============================================================================
def inicializar_session_state():
    """Inicializar todas las variables de session_state"""
    # Inicializar base de datos
    init_db()
    
    # Actualizar estructura si es necesario
    actualizar_estructura_bd()
    
    inicializar_datos_bd()
    
    # Crear backup inicial si no existe
    backup_dir = Path("backups")
    if not list(backup_dir.glob("*.db")):
        crear_backup_automatico()
    
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
# FUNCIONES DE AUTENTICACIÓN (MANTENIDAS)
# ============================================================================
def login(username, password):
    """Autenticar usuario desde base de datos - CORREGIDA"""
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
            
            # Buscar empleado correspondiente
            empleados_df = get_empleados()
            
            if empleados_df.empty:
                print("DEBUG: No hay empleados en la base de datos")
                st.session_state.empleado_actual = None
            else:
                # Convertir nombre a mayúsculas y quitar espacios
                nombre_buscado = nombre_usuario.strip().upper()
                
                # Primero buscar coincidencia exacta
                empleado_encontrado = empleados_df[
                    empleados_df['nombre_completo'].str.strip().str.upper() == nombre_buscado
                ]
                
                if empleado_encontrado.empty:
                    # Buscar coincidencia parcial usando apply
                    def buscar_coincidencia(nombre_empleado):
                        nombre_emp = str(nombre_empleado).upper()
                        return (nombre_buscado in nombre_emp or nombre_emp in nombre_buscado)
                    
                    mask = empleados_df['nombre_completo'].apply(buscar_coincidencia)
                    empleado_encontrado = empleados_df[mask]
                
                if not empleado_encontrado.empty:
                    st.session_state.empleado_actual = empleado_encontrado.iloc[0].to_dict()
                    print(f"DEBUG: Empleado encontrado: {st.session_state.empleado_actual.get('nombre_completo')}")
                else:
                    print(f"DEBUG: No se encontró empleado para: {nombre_usuario}")
                    print(f"DEBUG: Nombres disponibles: {empleados_df['nombre_completo'].tolist()[:5]}...")
                    st.session_state.empleado_actual = None
            
            # Configurar sesión
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
# FUNCIONES DE GESTIÓN DE DATOS (MANTENIDAS)
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
# PÁGINA DE LOGIN (MANTENIDA)
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
                
                if submit:
                    if login(username, password):
                        st.success(f"✅ Bienvenido, {username}!")
                        st.rerun()
                    else:
                        st.error("❌ Usuario o contraseña incorrectos")
    
    # Información de acceso
    #with st.expander("ℹ️ Información", expanded=False):
    #    st.markdown("""
    #    ### Sistema de Gestión de Turnos
    #    
    #    **Funcionalidades:**
    #    - 📅 Malla de turnos editable
    #    - 👥 Gestión de empleados
    #    - 👑 Gestión de usuarios
    #    - 📦 Sistema de backup automático
    #    - 🔄 Restauración de datos
    #    
    #    **Contacto:** soporte@empresa.com
    #    """)

# ============================================================================
# BARRA DE USUARIO (MANTENIDA)
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
# SIDEBAR SEGÚN ROL (MANTENIDO)
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
                ("👑 Gestión de Usuarios", "usuarios"),
                ("📦 Backup", "backup")
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

# ============================================================================
# PÁGINAS PRINCIPALES (CORREGIDAS)
# ============================================================================

# 1. PÁGINA DE MALLA DE TURNOS (CORREGIDA)
def pagina_malla():
    """Página principal - Malla de turnos (editable) - CORREGIDA"""
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
            
            # Opciones para los selectboxes (códigos de turno)
            opciones_codigos = list(st.session_state.codigos_turno.keys())
            # Filtrar opción vacía si existe
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
            
            # Mostrar editor de datos
            edited_df = st.data_editor(
                malla_editable,
                column_config=column_config,
                hide_index=True,
                use_container_width=True,
                height=600,
                num_rows="fixed",
                key=f"editor_malla_{mes_numero}_{ano}"
            )
            
            # BOTONES DE ACCIÓN - VERSIÓN SIMPLIFICADA Y FUNCIONAL
            st.markdown("---")
            st.markdown("### 💾 Acciones de Guardado")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.button("💾 Guardar Cambios Ahora", use_container_width=True, type="primary"):
                    with st.spinner("Guardando cambios..."):
                        try:
                            # Guardar en la base de datos
                            cambios = guardar_malla_turnos_con_backup(edited_df, mes_numero, ano)
                            
                            if cambios > 0:
                                # Actualizar session state
                                st.session_state.last_save = datetime.now()
                                st.session_state.malla_actual = get_malla_turnos(mes_numero, ano)
                                
                                st.success(f"✅ {cambios} cambios guardados exitosamente!")
                                registrar_log("guardar_malla", f"{mes_seleccionado} {ano} - {cambios} cambios")
                                
                                # Forzar recarga
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
                # Botón para limpiar todos los turnos
                if st.button("🗑️ Limpiar Todos", use_container_width=True, type="secondary"):
                    if st.checkbox("¿Confirmar que quieres limpiar TODOS los turnos de este mes?"):
                        # Crear copia vacía
                        malla_vacia = edited_df.copy()
                        for col in day_columns:
                            malla_vacia[col] = ""
                        
                        cambios = guardar_malla_turnos_con_backup(malla_vacia, mes_numero, ano)
                        st.session_state.malla_actual = get_malla_turnos(mes_numero, ano)
                        st.success(f"✅ {cambios} turnos limpiados")
                        st.rerun()
            
            # Detectar cambios automáticamente
            if 'ultima_version' not in st.session_state:
                st.session_state.ultima_version = st.session_state.malla_actual.to_dict()
            
            # Verificar si hay cambios
            cambios_detectados = False
            try:
                cambios_detectados = not edited_df.equals(st.session_state.malla_actual)
            except:
                pass
            
            if cambios_detectados:
                st.info("💡 **Hay cambios sin guardar.** Presiona 'Guardar Cambios Ahora' para guardarlos permanentemente.")
                
                # Mostrar vista previa de cambios
                with st.expander("👁️ Vista previa de cambios", expanded=False):
                    st.write("**Cambios detectados:**")
                    # Mostrar solo algunas filas con cambios
                    st.dataframe(edited_df.head(5))
            
            # Mostrar estadísticas
            mostrar_estadisticas_malla_preview(edited_df, mes_numero, ano)
            
        else:
            # Modo solo lectura
            st.info("👁️ Vista de solo lectura - No puedes editar")
            styled_df = aplicar_estilo_dataframe(st.session_state.malla_actual)
            st.dataframe(styled_df, use_container_width=True, height=600)
            
            # Mostrar estadísticas
            mostrar_estadisticas_malla()
def mostrar_estadisticas_malla_preview(df_malla, mes, ano):
    """Mostrar estadísticas de vista previa de la malla"""
    if df_malla.empty:
        return
    
    day_columns = [col for col in df_malla.columns if '/' in str(col)]
    
    if day_columns:
        # Calcular estadísticas
        total_empleados = len(df_malla)
        
        # Contar turnos por tipo
        turnos_por_tipo = {}
        for col in day_columns:
            for turno in df_malla[col]:
                if pd.notna(turno) and str(turno).strip() != '':
                    turno_str = str(turno).strip()
                    turnos_por_tipo[turno_str] = turnos_por_tipo.get(turno_str, 0) + 1
        
        total_turnos = sum(turnos_por_tipo.values())
        
        # Calcular horas totales
        horas_totales = 0
        for turno, cantidad in turnos_por_tipo.items():
            horas = st.session_state.codigos_turno.get(str(turno), {}).get("horas", 0)
            horas_totales += horas * cantidad
        
        # Mostrar métricas en tiempo real
        st.markdown("---")
        st.markdown("### 📊 Estadísticas (Vista Previa)")
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Empleados", total_empleados)
        with col2:
            st.metric("Turnos Asignados", total_turnos)
        with col3:
            st.metric("Horas Totales", horas_totales)
        with col4:
            empleados_activos = df_malla[
                df_malla['ESTADO'] == 'Activo'
            ].shape[0]
            st.metric("Empleados Activos", empleados_activos)

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

# 2. PÁGINA DE GESTIÓN DE EMPLEADOS (CORREGIDA)
def pagina_empleados():
    """Página de gestión de empleados - CORREGIDA CON GUARDADO FUNCIONAL"""
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
        # Crear DataFrame para edición MANTENIENDO el ID oculto
        df_editable = st.session_state.empleados_df.copy()
        
        # Renombrar columnas
        df_display = df_editable.rename(columns={
            'id': 'ID_OCULTO',  # Lo mantendremos oculto
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
        
        # Ocultar la columna ID_OCULTO en el display
        column_order = ['N°', 'CARGO', 'APELLIDOS Y NOMBRES', 'CC', 'DEPARTAMENTO', 
                       'ESTADO', 'HORA_INICIO', 'HORA_FIN', 'FECHA_REGISTRO']
        
        # Configurar columnas para el editor
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
        
        # Mostrar editor de datos
        edited_df = st.data_editor(
            df_display[column_order + ['ID_OCULTO']],  # Incluir ID al final
            column_config=column_config,
            hide_index=True,
            use_container_width=True,
            num_rows="fixed",
            key="editor_empleados"
        )
        
        # Botones de acción
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("💾 Guardar Cambios", use_container_width=True, key="btn_guardar_empleados"):
                try:
                    cambios, nuevos = guardar_empleados_con_backup(edited_df)
                    if cambios > 0:
                        st.success(f"✅ {cambios} cambios guardados correctamente")
                        if nuevos:
                            st.info(f"📝 {len(nuevos)} empleados nuevos agregados")
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
    """Agregar nuevo empleado a la base de datos - CORREGIDA"""
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
            # Obtener siguiente número
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(numero) FROM empleados")
            max_num = cursor.fetchone()[0]
            nuevo_numero = (max_num or 0) + 1
            conn.close()
            
            st.info(f"**Número asignado:** {nuevo_numero}")
        
        # Horarios opcionales
        st.markdown("**Horarios (Opcional)**")
        col3, col4 = st.columns(2)
        with col3:
            hora_inicio = st.text_input("Hora Inicio", placeholder="Ej: 06:00", key="hora_ini_nuevo")
        with col4:
            hora_fin = st.text_input("Hora Fin", placeholder="Ej: 14:00", key="hora_fin_nuevo")
        
        submitted = st.form_submit_button("💾 Guardar Empleado", use_container_width=True)
        
        if submitted:
            # Validar campos obligatorios
            if not all([cargo.strip(), nombre.strip(), cc.strip(), departamento]):
                st.error("❌ Por favor complete todos los campos obligatorios (*)")
                return False
            
            # Validar formato de cédula (solo números)
            if not cc.strip().isdigit():
                st.error("❌ La cédula debe contener solo números")
                return False
            
            conn = get_connection()
            cursor = conn.cursor()
            
            # Verificar si la cédula ya existe
            cursor.execute("SELECT COUNT(*) FROM empleados WHERE cedula = ?", (cc.strip(),))
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
            
            # Actualizar datos en session state
            st.session_state.empleados_df = get_empleados()
            
            return True
    
    return False

# 3. PÁGINA DE GESTIÓN DE USUARIOS (CORREGIDA)
def pagina_usuarios():
    """Página de gestión de usuarios para administradores - CORREGIDA"""
    if not check_permission("manage_users"):
        st.error("⛔ No tienes permisos para gestionar usuarios")
        return
    
    st.markdown("<h1 class='main-header'>👑 Gestión de Usuarios</h1>", unsafe_allow_html=True)
    
    # Mostrar usuarios existentes en editor de datos
    st.markdown("### 📋 Usuarios del Sistema")
    
    usuarios_df = get_usuarios()
    
    if usuarios_df.empty:
        st.warning("No hay usuarios registrados en el sistema.")
    else:
        # Preparar DataFrame para edición
        df_editable = usuarios_df.copy()
        
        # Eliminar columna password_hash del display
        if 'password_hash' in df_editable.columns:
            df_display = df_editable.drop(columns=['password_hash'])
        else:
            df_display = df_editable
        
        # Renombrar columnas
        df_display = df_display.rename(columns={
            'username': 'USUARIO',
            'nombre': 'NOMBRE_COMPLETO',
            'role': 'ROL',
            'departamento': 'DEPARTAMENTO',
            'created_at': 'FECHA_CREACION'
        })
        
        # Configurar columnas para el editor
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
        
        # Mostrar editor de datos
        edited_df = st.data_editor(
            df_display,
            column_config=column_config,
            hide_index=True,
            use_container_width=True,
            num_rows="fixed",
            key="editor_usuarios"
        )
        
        # Botón para guardar cambios en usuarios
        if st.button("💾 Guardar Cambios de Usuarios", use_container_width=True, key="btn_guardar_usuarios"):
            try:
                cambios = guardar_usuarios_con_backup(edited_df, usuarios_df)
                if cambios > 0:
                    st.success(f"✅ {cambios} usuarios actualizados correctamente")
                    st.rerun()
                else:
                    st.warning("⚠️ No se realizaron cambios")
            except Exception as e:
                st.error(f"❌ Error al guardar usuarios: {str(e)}")
    
    # Sección para crear nuevo usuario
    st.markdown("---")
    st.markdown("### ➕ Crear Nuevo Usuario")
    
    with st.form("form_nuevo_usuario", clear_on_submit=True):
        col1, col2 = st.columns(2)
        
        with col1:
            nuevo_username = st.text_input("Usuario*", placeholder="Ej: juan.perez", key="username_nuevo")
            nuevo_nombre = st.text_input("Nombre Completo*", placeholder="Ej: Juan Pérez García", key="nombre_usuario_nuevo")
        
        with col2:
            nuevo_rol = st.selectbox("Rol*", list(ROLES.keys()), key="rol_nuevo")
            nuevo_depto = st.selectbox("Departamento", st.session_state.configuracion['departamentos'], key="depto_usuario_nuevo")
        
        st.markdown("**Contraseña**")
        col3, col4 = st.columns(2)
        with col3:
            nueva_password = st.text_input("Contraseña*", type="password", placeholder="Mínimo 6 caracteres", key="pass_nuevo")
        with col4:
            confirm_password = st.text_input("Confirmar Contraseña*", type="password", key="confirm_pass_nuevo")
        
        submitted = st.form_submit_button("👑 Crear Nuevo Usuario", use_container_width=True)
        
        if submitted:
            if crear_nuevo_usuario(nuevo_username, nueva_password, confirm_password, nuevo_nombre, nuevo_rol, nuevo_depto):
                st.rerun()

def crear_nuevo_usuario(username, password, confirm_password, nombre, rol, departamento):
    """Crear nuevo usuario - NUEVA FUNCIÓN"""
    # Validaciones
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
    
    # Verificar si el usuario ya existe
    cursor.execute("SELECT COUNT(*) FROM usuarios WHERE username = ?", (username.strip(),))
    if cursor.fetchone()[0] > 0:
        st.error("❌ Ya existe un usuario con ese nombre")
        conn.close()
        return False
    
    # Crear hash de la contraseña
    password_hash = hashlib.sha256(password.encode()).hexdigest()
    
    # Insertar nuevo usuario
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
    
    return True

# 4. PÁGINA DE CONFIGURACIÓN (ACTUALIZADA)
def pagina_configuracion():
    """Página de configuración - CORREGIDA"""
    if not check_permission("configure"):
        st.error("⛔ No tienes permisos para acceder a la configuración")
        return
    
    st.markdown("<h1 class='main-header'>⚙️ Configuración</h1>", unsafe_allow_html=True)
    
    tab1, tab2, tab3 = st.tabs(["Códigos de Turno", "General", "Sistema"])
    
    with tab1:
        st.markdown("### Configurar Códigos de Turno")
        
        # Obtener códigos actuales
        conn = get_connection()
        codigos_df = pd.read_sql("SELECT * FROM codigos_turno ORDER BY codigo", conn)
        conn.close()
        
        # Configurar columnas para el editor - CORREGIDO
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
        
        # Mostrar editor de datos con posibilidad de agregar filas
        edited_codigos = st.data_editor(
            codigos_df,
            column_config=column_config,
            num_rows="dynamic",  # Permite agregar nuevas filas
            use_container_width=True,
            key="editor_codigos"
        )
        
        # Mostrar estadísticas rápidas
        if not edited_codigos.empty:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Códigos", len(edited_codigos))
            with col2:
                codigos_con_horas = len(edited_codigos[edited_codigos['horas'] > 0])
                st.metric("Con Horas", codigos_con_horas)
            with col3:
                st.metric("Sin Horas", len(edited_codigos[edited_codigos['horas'] == 0]))
        
        # Sección para agregar código rápido
        st.markdown("---")
        st.markdown("### ➕ Agregar Código Rápido")
        
        with st.form("form_codigo_rapido"):
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                nuevo_codigo = st.text_input("Código*", placeholder="Ej: 25")
            with col2:
                nuevo_nombre = st.text_input("Descripción*", placeholder="Ej: 9 AM - 6 PM")
            with col3:
                nuevo_color = st.color_picker("Color", "#FF6B6B")
            with col4:
                nuevo_horas = st.number_input("Horas*", min_value=0, max_value=24, value=8)
            
            submitted = st.form_submit_button("➕ Agregar Código", use_container_width=True)
            
            if submitted:
                if nuevo_codigo and nuevo_nombre:
                    # Verificar si el código ya existe
                    if nuevo_codigo in edited_codigos['codigo'].values:
                        st.error(f"❌ El código '{nuevo_codigo}' ya existe")
                    else:
                        # Convertir color HEX a string
                        color_hex = nuevo_color
                        
                        # Agregar nueva fila al DataFrame
                        nueva_fila = pd.DataFrame({
                            'codigo': [nuevo_codigo],
                            'nombre': [nuevo_nombre],
                            'color': [color_hex],
                            'horas': [nuevo_horas]
                        })
                        edited_codigos = pd.concat([edited_codigos, nueva_fila], ignore_index=True)
                        st.success(f"✅ Código '{nuevo_codigo}' agregado a la lista")
                        st.rerun()
                else:
                    st.error("❌ Por favor complete los campos obligatorios (*)")
        
        # Botones de acción
        st.markdown("---")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("💾 Guardar Todos los Cambios", use_container_width=True, type="primary", key="btn_guardar_codigos"):
                try:
                    # Validar datos antes de guardar
                    errores = []
                    
                    for idx, row in edited_codigos.iterrows():
                        # Validar código no vacío
                        if pd.isna(row['codigo']) or str(row['codigo']).strip() == '':
                            errores.append(f"Fila {idx+1}: Código vacío")
                            continue
                        
                        # Validar nombre no vacío
                        if pd.isna(row['nombre']) or str(row['nombre']).strip() == '':
                            errores.append(f"Fila {idx+1}: Descripción vacía")
                            continue
                        
                        # Validar formato color HEX
                        color_str = str(row['color']).strip()
                        if not color_str.startswith('#') or len(color_str) != 7:
                            errores.append(f"Fila {idx+1}: Color inválido (debe ser #RRGGBB)")
                            continue
                    
                    if errores:
                        st.error("❌ Errores encontrados:")
                        for error in errores:
                            st.write(f"- {error}")
                        return
                    
                    conn = get_connection()
                    cursor = conn.cursor()
                    
                    # Eliminar todos los códigos existentes
                    cursor.execute("DELETE FROM codigos_turno")
                    
                    # Insertar los códigos editados
                    for _, row in edited_codigos.iterrows():
                        cursor.execute(
                            "INSERT INTO codigos_turno (codigo, nombre, color, horas) VALUES (?, ?, ?, ?)",
                            (
                                str(row['codigo']).strip(),
                                str(row['nombre']).strip(),
                                str(row['color']).strip(),
                                int(row['horas'])
                            )
                        )
                    
                    conn.commit()
                    conn.close()
                    
                    # Actualizar session state
                    st.session_state.codigos_turno = get_codigos_turno()
                    st.success(f"✅ {len(edited_codigos)} códigos guardados correctamente")
                    registrar_log("actualizar_codigos", f"{len(edited_codigos)} códigos")
                    
                    # Crear backup automático
                    crear_backup_automatico()
                    
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"❌ Error al guardar: {str(e)}")
        
        with col2:
            if st.button("🔄 Restaurar Valores por Defecto", use_container_width=True, key="btn_reset_codigos"):
                with st.expander("⚠️ Confirmar Restauración", expanded=True):
                    st.warning("""
                    **ADVERTENCIA:** Esto eliminará todos los códigos actuales y 
                    restaurará los valores por defecto.
                    
                    Se recomienda exportar primero los códigos actuales.
                    """)
                    
                    col_res1, col_res2 = st.columns(2)
                    with col_res1:
                        if st.button("✅ Sí, Restaurar", type="primary"):
                            try:
                                conn = get_connection()
                                cursor = conn.cursor()
                                
                                # Limpiar tabla
                                cursor.execute("DELETE FROM codigos_turno")
                                
                                # Insertar códigos por defecto
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
                                st.success("✅ Valores por defecto restaurados")
                                st.rerun()
                                
                            except Exception as e:
                                st.error(f"❌ Error al restaurar: {str(e)}")
                    
                    with col_res2:
                        if st.button("❌ Cancelar"):
                            st.info("Operación cancelada")
        
        with col3:
            # Exportar códigos
            if not edited_codigos.empty:
                csv = edited_codigos.to_csv(index=False)
                st.download_button(
                    label="📥 Exportar CSV",
                    data=csv,
                    file_name="codigos_turno.csv",
                    mime="text/csv",
                    use_container_width=True,
                    key="btn_export_codigos"
                )
        
        # Mostrar vista previa de cambios
        if not edited_codigos.empty:
            with st.expander("👁️ Vista Previa de Códigos", expanded=False):
                st.dataframe(edited_codigos, use_container_width=True)
                
                # Mostrar colores
                st.markdown("**🎨 Vista previa de colores:**")
                cols = st.columns(min(5, len(edited_codigos)))
                for idx, (_, row) in enumerate(edited_codigos.iterrows()):
                    if idx < 5:  # Mostrar solo primeros 5
                        with cols[idx % 5]:
                            st.markdown(f"""
                            <div style="background-color: {row['color']}; padding: 10px; 
                                      border-radius: 5px; text-align: center; margin: 5px;">
                                <strong>{row['codigo']}</strong><br>
                                <small>{row['nombre']}</small>
                            </div>
                            """, unsafe_allow_html=True)
    
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
                value=",".join(st.session_state.configuracion['departamentos']),
                height=100
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
            st.rerun()
    
    with tab3:
        st.markdown("### ⚙️ Configuración del Sistema")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 🔒 Seguridad")
            auto_backup = st.checkbox("Backup automático al guardar", value=True)
            max_backups = st.number_input("Máximo de backups", min_value=5, max_value=50, value=10)
            logs_dias = st.number_input("Retener logs (días)", min_value=7, max_value=365, value=30)
        
        with col2:
            st.markdown("#### 📊 Rendimiento")
            auto_refresh = st.checkbox("Auto-refresh cada 5 minutos", value=False)
            cache_time = st.number_input("Cache (minutos)", min_value=1, max_value=60, value=10)
        
        if st.button("💾 Guardar Configuración Sistema", use_container_width=True):
            conn = get_connection()
            cursor = conn.cursor()
            
            config_sistema = [
                ("auto_backup", "1" if auto_backup else "0", "boolean"),
                ("max_backups", str(max_backups), "number"),
                ("logs_dias", str(logs_dias), "number"),
                ("auto_refresh", "1" if auto_refresh else "0", "boolean"),
                ("cache_time", str(cache_time), "number")
            ]
            
            for clave, valor, tipo in config_sistema:
                cursor.execute('''
                    INSERT OR REPLACE INTO configuracion (clave, valor, tipo)
                    VALUES (?, ?, ?)
                ''', (clave, valor, tipo))
            
            conn.commit()
            conn.close()
            
            st.success("✅ Configuración del sistema guardada")
            registrar_log("config_sistema", "Configuración actualizada")
            st.rerun()

# 5. PÁGINA DE BACKUP (NUEVA)
def pagina_backup():
    """Página completa de backup y restauración"""
    st.markdown("<h1 class='main-header'>📦 Sistema de Backup y Restauración</h1>", unsafe_allow_html=True)
    
    tab1, tab2, tab3, tab4 = st.tabs(["🗄️ Backups DB", "📄 Exportar JSON", "📥 Importar JSON", "🔄 Rollback"])
    
    with tab1:
        st.markdown("### 🗄️ Backups de Base de Datos")
        
        # Listar backups existentes
        backup_dir = Path("backups")
        backup_dir.mkdir(exist_ok=True)
        
        backups = sorted(backup_dir.glob("turnos_backup_*.db"), key=os.path.getmtime, reverse=True)
        
        if backups:
            st.markdown(f"**📊 Total de backups:** {len(backups)}")
            
            # Mostrar tabla de backups
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
            
            # Seleccionar backup para restaurar
            st.markdown("### 🔄 Restaurar desde Backup")
            backup_opciones = [b.name for b in backups]
            selected_backup = st.selectbox("Seleccionar backup para restaurar", backup_opciones)
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("🔄 Restaurar este Backup", use_container_width=True):
                    if selected_backup:
                        backup_path = backup_dir / selected_backup
                        if restaurar_backup(backup_path):
                            st.success("✅ Base de datos restaurada correctamente")
                            st.rerun()
            
            with col2:
                if st.button("🗑️ Eliminar Backups Antiguos", use_container_width=True):
                    # Mantener solo los últimos 10
                    if len(backups) > 10:
                        for old_backup in backups[10:]:
                            old_backup.unlink()
                        st.success(f"✅ Eliminados {len(backups)-10} backups antiguos")
                        st.rerun()
                    else:
                        st.info("✅ Ya solo hay 10 backups o menos")
        
        else:
            st.warning("No hay backups disponibles.")
        
        # Crear backup manual
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
            # Descargar backup actual
            if os.path.exists(DB_NAME):
                with open(DB_NAME, "rb") as f:
                    db_bytes = f.read()
                
                st.download_button(
                    label="📥 Descargar DB Actual",
                    data=db_bytes,
                    file_name=f"turnos_db_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db",
                    mime="application/octet-stream",
                    use_container_width=True
                )
    
    with tab2:
        st.markdown("### 📄 Exportar a JSON")
        st.markdown("Exporta todos los datos a un archivo JSON portable.")
        
        # Exportar datos
        json_data = exportar_backup_json()
        
        if json_data:
            # Mostrar vista previa
            with st.expander("👁️ Vista previa (primeros 1000 caracteres)", expanded=False):
                st.code(json_data[:1000] + "..." if len(json_data) > 1000 else json_data)
            
            # Botón de descarga
            st.download_button(
                label="📥 Descargar JSON Completo",
                data=json_data,
                file_name=f"turnos_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json",
                use_container_width=True
            )
            
            # Estadísticas
            datos = json.loads(json_data)
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Empleados", len(datos.get('empleados', [])))
            with col2:
                st.metric("Turnos", len(datos.get('malla_turnos', [])))
            with col3:
                st.metric("Usuarios", len(datos.get('usuarios', [])))
            with col4:
                st.metric("Códigos", len(datos.get('codigos_turno', [])))
        else:
            st.error("Error al exportar datos")
    
    with tab3:
        st.markdown("### 📥 Importar desde JSON")
        st.markdown("Importa datos desde un archivo JSON de backup.")
        
        # Advertencia
        st.warning("""
        ⚠️ **ADVERTENCIA:** 
        - Esta acción SOBREESCRIBIRÁ todos los datos actuales
        - Se creará un backup automático antes de importar
        - Se recomienda exportar primero los datos actuales
        """)
        
        uploaded_file = st.file_uploader("Seleccionar archivo JSON de backup", type=['json'])
        
        if uploaded_file is not None:
            try:
                # Leer archivo
                json_str = uploaded_file.getvalue().decode('utf-8')
                
                # Validar estructura básica
                datos = json.loads(json_str)
                
                if 'empleados' in datos and 'version' in datos:
                    st.success("✅ Archivo válido detectado")
                    
                    # Mostrar resumen
                    with st.expander("📊 Resumen del archivo", expanded=True):
                        col1, col2 = st.columns(2)
                        with col1:
                            st.write(f"**Empleados:** {len(datos.get('empleados', []))}")
                            st.write(f"**Turnos:** {len(datos.get('malla_turnos', []))}")
                            st.write(f"**Versión:** {datos.get('version', 'Desconocida')}")
                        with col2:
                            st.write(f"**Usuarios:** {len(datos.get('usuarios', []))}")
                            st.write(f"**Códigos:** {len(datos.get('codigos_turno', []))}")
                            st.write(f"**Fecha exportación:** {datos.get('export_date', 'Desconocida')}")
                    
                    # Confirmación
                    st.markdown("---")
                    confirm = st.checkbox("✅ Confirmo que quiero importar estos datos (sobreescribirá los actuales)")
                    
                    if confirm and st.button("🚀 Importar Datos", use_container_width=True):
                        with st.spinner("Importando datos..."):
                            if importar_backup_json(json_str):
                                st.success("✅ Datos importados correctamente")
                                st.info("🔄 La página se recargará en 3 segundos...")
                                time.sleep(3)
                                st.rerun()
                            else:
                                st.error("❌ Error al importar datos")
                
                else:
                    st.error("❌ El archivo no tiene la estructura correcta")
                    
            except Exception as e:
                st.error(f"❌ Error al leer el archivo: {str(e)}")
    
    with tab4:
        st.markdown("### 🔄 Sistema de Rollback")
        st.markdown("Revertir cambios recientes en la base de datos.")
        
        # Obtener logs recientes
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM logs 
            WHERE accion IN ('guardar_malla', 'actualizar_empleados', 'crear_usuario', 'actualizar_configuracion')
            ORDER BY timestamp DESC 
            LIMIT 20
        ''')
        
        logs_recientes = cursor.fetchall()
        conn.close()
        
        if logs_recientes:
            st.markdown("#### 📝 Últimos cambios registrados:")
            
            for log in logs_recientes:
                log_id, timestamp, accion, detalles, usuario = log
                col1, col2, col3 = st.columns([2, 2, 1])
                with col1:
                    st.write(f"**{accion}**")
                with col2:
                    st.write(f"{detalles[:50]}..." if len(detalles) > 50 else detalles)
                with col3:
                    st.write(f"`{timestamp}`")
            
            st.markdown("---")
            st.markdown("#### 🔙 Revertir último cambio")
            
            # Buscar último backup disponible
            backups = sorted(backup_dir.glob("turnos_backup_*.db"), key=os.path.getmtime, reverse=True)
            
            if len(backups) >= 2:  # Necesitamos al menos 2 backups
                ultimo_backup = backups[0]
                backup_anterior = backups[1]
                
                col1, col2 = st.columns(2)
                with col1:
                    st.info(f"**Último backup:**\n{ultimo_backup.name}")
                with col2:
                    st.info(f"**Backup anterior:**\n{backup_anterior.name}")
                
                if st.button("↩️ Revertir al Backup Anterior", use_container_width=True):
                    if restaurar_backup(backup_anterior):
                        st.success("✅ Rollback completado")
                        st.rerun()
            else:
                st.warning("Se necesitan al menos 2 backups para realizar rollback")
        
        else:
            st.info("No hay logs de cambios recientes")

# 6. PÁGINAS PARA EMPLEADOS (MANTENIDAS)
def pagina_mis_turnos():
    """Página para que los empleados vean SUS turnos (solo lectura) - SIMPLIFICADA"""
    st.markdown("<h1 class='main-header'>📅 Mis Turnos</h1>", unsafe_allow_html=True)
    
    if not st.session_state.empleado_actual:
        st.warning("⚠️ No se encontró tu registro como empleado.")
        
        # Solución simple
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Ir a Mi Información", use_container_width=True):
                st.session_state.current_page = "mi_info"
                st.rerun()
        with col2:
            if st.button("Cerrar Sesión y Reintentar", use_container_width=True):
                logout()
        
        return
    
    empleado_info = st.session_state.empleado_actual
    
    # Mostrar información básica
    st.info(f"""
    **👤 {empleado_info.get('nombre_completo', 'N/A')}**  
    **💼 {empleado_info.get('cargo', 'N/A')}** - {empleado_info.get('departamento', 'N/A')}
    """)
    
    # Seleccionar mes y año
    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        meses = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", 
                "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
        mes_seleccionado = st.selectbox("Mes:", meses, index=datetime.now().month - 1)
        mes_numero = meses.index(mes_seleccionado) + 1
    
    with col2:
        ano = st.number_input("Año:", min_value=2023, max_value=2030, value=datetime.now().year)
    
    with col3:
        if st.button("🔍 Buscar Turnos", use_container_width=True, type="primary"):
            st.session_state.buscar_turnos = True
            st.session_state.mes_busqueda = mes_numero
            st.session_state.ano_busqueda = ano
    
    # Si se presionó buscar, mostrar resultados
    if hasattr(st.session_state, 'buscar_turnos') and st.session_state.buscar_turnos:
        try:
            empleado_id = empleado_info.get('id')
            
            if not empleado_id:
                st.error("❌ No se pudo identificar tu ID de empleado")
                return
            
            # Obtener turnos
            turnos_dict = get_turnos_empleado_mes(empleado_id, mes_numero, ano)
            
            if not turnos_dict:
                st.info(f"📭 No tienes turnos asignados para {mes_seleccionado} {ano}.")
                
                # Opción para ver otros meses
                st.markdown("---")
                st.markdown("**¿Quieres ver otro mes?**")
                if st.button("🔄 Buscar otro mes"):
                    st.session_state.buscar_turnos = False
                    st.rerun()
                
                return
            
            # Mostrar turnos encontrados
            st.markdown(f"### 📋 Turnos de {mes_seleccionado} {ano}")
            
            # Crear tabla simple
            turnos_data = []
            for dia in sorted(turnos_dict.keys()):
                codigo = turnos_dict[dia]
                if codigo and str(codigo).strip() != '':
                    turno_info = st.session_state.codigos_turno.get(str(codigo), {})
                    turnos_data.append({
                        'Fecha': f"{dia:02d}/{mes_numero:02d}/{ano}",
                        'Día': dia,
                        'Turno': turno_info.get('nombre', 'Desconocido'),
                        'Horas': turno_info.get('horas', 0),
                        'Código': codigo
                    })
            
            if turnos_data:
                df = pd.DataFrame(turnos_data)
                
                # Mostrar tabla
                st.dataframe(
                    df[['Fecha', 'Turno', 'Horas']],
                    hide_index=True,
                    use_container_width=True
                )
                
                # Mostrar estadísticas
                total_turnos = len(turnos_data)
                total_horas = sum(t['Horas'] for t in turnos_data)
                dias_mes = calendar.monthrange(ano, mes_numero)[1]
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Turnos", total_turnos)
                with col2:
                    st.metric("Horas Totales", total_horas)
                with col3:
                    if total_turnos > 0:
                        promedio = total_horas / total_turnos
                        st.metric("Promedio H/Turno", f"{promedio:.1f}")
                    else:
                        st.metric("Promedio H/Turno", 0)
                
                # Mostrar días con/sin turno
                st.markdown(f"**📅 Días del mes:** {dias_mes} días")
                st.markdown(f"**✅ Días con turno:** {total_turnos} días ({total_turnos/dias_mes*100:.1f}%)")
                
            else:
                st.info(f"📭 No tienes turnos asignados para {mes_seleccionado} {ano}.")
                
        except Exception as e:
            st.error(f"❌ Error al cargar turnos: {e}")
            
            # Botón para reintentar
            if st.button("🔄 Reintentar"):
                st.rerun()
    
    # Si no se ha buscado aún, mostrar instrucciones
    elif not hasattr(st.session_state, 'buscar_turnos'):
        st.markdown("---")
        st.markdown("""
        ### ℹ️ Instrucciones
        
        1. Selecciona el mes y año que deseas consultar
        2. Presiona el botón **"🔍 Buscar Turnos"**
        3. Si no aparecen turnos, es porque no tienes asignaciones para ese periodo
        
        **Nota:** Los turnos deben ser asignados por un supervisor o administrador.
        """)

def pagina_calendario():
    """Página de calendario visual simplificada - MEJORADA"""
    st.markdown("<h1 class='main-header'>📆 Mi Calendario</h1>", unsafe_allow_html=True)
    
    # Lista de meses para usar en selectbox
    nombres_meses = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", 
                    "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
    
    if not st.session_state.empleado_actual:
        st.warning("⚠️ No se encontró tu registro como empleado.")
        
        # Solución rápida
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Ir a Mi Información", use_container_width=True):
                st.session_state.current_page = "mi_info"
                st.rerun()
        
        return
    
    empleado_info = st.session_state.empleado_actual
    
    # Selector de mes y año
    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        mes_seleccionado = st.selectbox("Mes:", nombres_meses, 
                                       index=st.session_state.get('calendario_mes', datetime.now().month) - 1)
        mes_numero = nombres_meses.index(mes_seleccionado) + 1
    
    with col2:
        ano = st.number_input("Año:", min_value=2023, max_value=2030, 
                             value=st.session_state.get('calendario_ano', datetime.now().year))
    
    with col3:
        if st.button("📅 Generar Calendario", use_container_width=True, type="primary"):
            # Guardar selección
            st.session_state.calendario_mes = mes_numero
            st.session_state.calendario_ano = ano
            st.rerun()
    
    # Si ya hay una selección, mostrar calendario
    if 'calendario_mes' in st.session_state and 'calendario_ano' in st.session_state:
        # Obtener turnos del empleado para el mes seleccionado
        turnos_dict = get_turnos_empleado_mes(empleado_info['id'], mes_numero, ano)
        
        # Mostrar información resumida
        st.markdown("---")
        
        # Información del empleado
        col_info1, col_info2 = st.columns(2)
        with col_info1:
            st.markdown(f"""
            <div style="background-color: #f8f9fa; padding: 15px; border-radius: 10px; 
                       border: 1px solid #dee2e6; margin-bottom: 20px;">
                <h4 style="margin-top: 0;">👤 Mi Información</h4>
                <p><strong>Nombre:</strong> {empleado_info.get('nombre_completo', 'N/A')}</p>
                <p><strong>Cargo:</strong> {empleado_info.get('cargo', 'N/A')}</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col_info2:
            st.markdown(f"""
            <div style="background-color: #f8f9fa; padding: 15px; border-radius: 10px; 
                       border: 1px solid #dee2e6; margin-bottom: 20px;">
                <h4 style="margin-top: 0;">💼 Datos Laborales</h4>
                <p><strong>Departamento:</strong> {empleado_info.get('departamento', 'N/A')}</p>
                <p><strong>Estado:</strong> {empleado_info.get('estado', 'N/A')}</p>
            </div>
            """, unsafe_allow_html=True)
        
        # Estadísticas del mes
        if turnos_dict:
            dias_con_turno = sum(1 for codigo in turnos_dict.values() if codigo and str(codigo).strip() != '')
            total_dias = len(turnos_dict)
            
            # Calcular horas totales
            horas_totales = 0
            for dia, codigo in turnos_dict.items():
                if codigo and str(codigo).strip() != '':
                    horas = st.session_state.codigos_turno.get(str(codigo), {}).get("horas", 0)
                    horas_totales += horas
            
            col_stat1, col_stat2, col_stat3 = st.columns(3)
            with col_stat1:
                st.metric("Días del Mes", total_dias)
            with col_stat2:
                st.metric("Días con Turno", dias_con_turno)
            with col_stat3:
                st.metric("Horas Totales", horas_totales)
        
        # Generar y mostrar calendario
        generar_calendario_simple(mes_numero, ano, turnos_dict)
        
        # Botones de navegación
        st.markdown("---")
        col_nav1, col_nav2, col_nav3 = st.columns(3)
        
        with col_nav1:
            if st.button("⬅️ Mes Anterior", use_container_width=True):
                nuevo_mes = mes_numero - 1
                nuevo_ano = ano
                if nuevo_mes < 1:
                    nuevo_mes = 12
                    nuevo_ano -= 1
                st.session_state.calendario_mes = nuevo_mes
                st.session_state.calendario_ano = nuevo_ano
                st.rerun()
        
        with col_nav2:
            if st.button("🔄 Mes Actual", use_container_width=True):
                st.session_state.calendario_mes = datetime.now().month
                st.session_state.calendario_ano = datetime.now().year
                st.rerun()
        
        with col_nav3:
            if st.button("➡️ Mes Siguiente", use_container_width=True):
                nuevo_mes = mes_numero + 1
                nuevo_ano = ano
                if nuevo_mes > 12:
                    nuevo_mes = 1
                    nuevo_ano += 1
                st.session_state.calendario_mes = nuevo_mes
                st.session_state.calendario_ano = nuevo_ano
                st.rerun()
    
    else:
        # Instrucciones si no se ha generado calendario
        st.markdown("---")
        st.markdown("""
        ### 📅 Cómo usar el calendario
        
        1. **Selecciona** el mes y año que deseas ver
        2. **Presiona** el botón "📅 Generar Calendario"
        3. **Navega** entre meses usando los botones de navegación
        
        **Colores:**
        - Cada color representa un tipo de turno diferente
        - Los días sin color son días libres o sin turno asignado
        - Los domingos aparecen en **rojo**
        - Los sábados aparecen en **azul**
        
        **Leyenda:** Abajo del calendario verás la leyenda con los códigos de turno asignados.
        """)
        
        # Ejemplo de cómo se verá
        with st.expander("👁️ Ver ejemplo de calendario", expanded=False):
            st.info("Así se verá tu calendario una vez generado:")
            
            # Crear un ejemplo con datos de prueba
            ejemplo_turnos = {
                1: "20", 2: "15", 3: "", 4: "VC", 5: "20",
                15: "CP", 20: "155", 25: "70", 28: "26"
            }
            
            # Mostrar ejemplo para el mes actual
            generar_calendario_simple(datetime.now().month, datetime.now().year, ejemplo_turnos)

def generar_calendario_simple(mes, ano, turnos_dict):
    """Generar calendario simple - VERSIÓN SIMPLIFICADA Y CORREGIDA"""
    # Lista de meses
    nombres_meses = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", 
                    "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
    
    # Determinar número de días en el mes
    num_dias = calendar.monthrange(ano, mes)[1]
    
    # Días de la semana (DOMINGO primero)
    dias_semana = ["Dom", "Lun", "Mar", "Mié", "Jue", "Vie", "Sáb"]
    
    # Primer día del mes
    primer_dia = date(ano, mes, 1)
    
    # Calcular día de inicio (0=Domingo, 1=Lunes, ..., 6=Sábado)
    # weekday() devuelve: Lunes=0, Martes=1, ..., Domingo=6
    dia_semana_python = primer_dia.weekday()  # Lunes=0, Domingo=6
    dia_inicio_semana = (dia_semana_python + 1) % 7  # Convertir a Dom=0, Lun=1, ..., Sáb=6
    
    # Mostrar título
    st.markdown(f"### 📅 {nombres_meses[mes-1]} {ano}")
    
    # Crear encabezados de días
    cols = st.columns(7)
    for idx, dia in enumerate(dias_semana):
        with cols[idx]:
            if idx == 0:  # Domingo
                st.markdown(f"<div style='color: #d32f2f; text-align: center; font-weight: bold; padding: 5px;'>DOM</div>", 
                           unsafe_allow_html=True)
            elif idx == 6:  # Sábado
                st.markdown(f"<div style='color: #1976d2; text-align: center; font-weight: bold; padding: 5px;'>SÁB</div>", 
                           unsafe_allow_html=True)
            else:
                st.markdown(f"<div style='text-align: center; font-weight: bold; padding: 5px;'>{dia.upper()}</div>", 
                           unsafe_allow_html=True)
    
    # Crear calendario usando Streamlit nativo (sin HTML complejo)
    dia_actual = 0
    filas_necesarias = (dia_inicio_semana + num_dias + 6) // 7
    
    for fila in range(filas_necesarias):
        cols = st.columns(7)
        
        for columna in range(7):
            with cols[columna]:
                # Calcular número de día
                num_dia_celda = dia_actual - dia_inicio_semana + 1
                
                if dia_actual < dia_inicio_semana or num_dia_celda > num_dias:
                    # Celda vacía (antes del primer día o después del último)
                    st.write(" ")
                else:
                    # Día válido del mes
                    dia_num = num_dia_celda
                    
                    # Obtener información del turno
                    codigo = turnos_dict.get(dia_num, "")
                    turno_info = st.session_state.codigos_turno.get(str(codigo), 
                                                                   {"color": "#FFFFFF", "nombre": "Sin turno"})
                    color = turno_info["color"]
                    nombre_turno = turno_info.get("nombre", "")
                    
                    # Determinar día de la semana
                    dia_semana_actual = (dia_inicio_semana + dia_num - 1) % 7
                    
                    # Determinar si es hoy
                    hoy = datetime.now()
                    es_hoy = (dia_num == hoy.day and mes == hoy.month and ano == hoy.year)
                    
                    # Estilos según día de la semana
                    estilo_texto = ""
                    if dia_semana_actual == 0:  # Domingo
                        estilo_texto = "color: #d32f2f;"
                    elif dia_semana_actual == 6:  # Sábado
                        estilo_texto = "color: #1976d2;"
                    
                    # Crear contenido de la celda
                    contenido = f"""
                    <div style="background-color: {color}; 
                               padding: 8px; 
                               border-radius: 5px; 
                               border: 2px solid {'#FF5722' if es_hoy else '#e0e0e0'};
                               text-align: center;
                               min-height: 60px;
                               display: flex;
                               flex-direction: column;
                               justify-content: center;">
                        <div style="font-weight: bold; font-size: 1.1em; {estilo_texto}">
                            {dia_num}
                        </div>
                        {f'<div style="font-size: 0.8em; margin-top: 3px;">{codigo}</div>' if codigo else ''}
                    </div>
                    """
                    
                    st.markdown(contenido, unsafe_allow_html=True)
            
            dia_actual += 1
    
    # Indicador de "Hoy"
    hoy = datetime.now()
    if mes == hoy.month and ano == hoy.year:
        st.markdown(f'<div style="text-align: center; color: #FF5722; margin-top: 10px;">📅 <strong>Hoy: {hoy.day} de {nombres_meses[hoy.month-1]}</strong></div>', 
                   unsafe_allow_html=True)
    
    # Leyenda de códigos
    codigos_presentes = set(str(codigo) for codigo in turnos_dict.values() if codigo)
    
    if codigos_presentes:
        st.markdown("---")
        st.markdown("#### 🎨 Turnos Asignados")
        
        # Mostrar en columnas
        codigos_lista = sorted(list(codigos_presentes))
        num_columnas = min(4, len(codigos_lista))
        
        if num_columnas > 0:
            cols_leyenda = st.columns(num_columnas)
            
            for idx, codigo in enumerate(codigos_lista):
                col_idx = idx % num_columnas
                with cols_leyenda[col_idx]:
                    info = st.session_state.codigos_turno.get(codigo, {})
                    color = info.get('color', '#FFFFFF')
                    nombre = info.get('nombre', 'Desconocido')
                    horas = info.get('horas', 0)
                    
                    st.markdown(f"""
                    <div style="display: flex; align-items: center; margin-bottom: 10px; padding: 5px;">
                        <div style="width: 20px; height: 20px; background-color: {color}; 
                                 margin-right: 8px; border-radius: 3px; border: 1px solid #ccc;"></div>
                        <div>
                            <strong>{codigo}</strong><br>
                            <small style="color: #666;">{nombre[:12]}{'...' if len(nombre) > 12 else ''}</small>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)

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
# MONITOREO DEL SISTEMA
# ============================================================================
def monitoreo_sistema():
    """Mostrar estado del sistema"""
    with st.sidebar.expander("📊 Estado del Sistema", expanded=False):
        # Tamaño de la base de datos
        if os.path.exists(DB_NAME):
            size_mb = os.path.getsize(DB_NAME) / (1024 * 1024)
            st.metric("Tamaño BD", f"{size_mb:.1f} MB")
        
        # Número de backups
        backup_dir = Path("backups")
        if backup_dir.exists():
            num_backups = len(list(backup_dir.glob("*.db")))
            st.metric("Backups", num_backups)
        
        # Última actividad
        if st.session_state.last_save:
            tiempo = datetime.now() - st.session_state.last_save
            minutos = int(tiempo.total_seconds() / 60)
            st.metric("Último guardado", f"Hace {minutos} min")
        
        # Verificar integridad
        conn = get_connection()
        cursor = conn.cursor()
        
        # Verificar tablas
        cursor.execute("SELECT COUNT(*) FROM empleados")
        num_emp = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM malla_turnos")
        num_turnos = cursor.fetchone()[0]
        
        conn.close()
        
        st.progress(min(100, num_emp * 2), text=f"Empleados: {num_emp}")
        st.progress(min(100, num_turnos // 10), text=f"Turnos: {num_turnos}")

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
    
    # Mostrar monitoreo del sistema (solo admin)
    if st.session_state.auth['role'] == 'admin':
        monitoreo_sistema()
    
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
    elif pagina_actual == "backup":
        pagina_backup()
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
        "📊 Creado por Edwin Merchán | © 2026 | Versión 2.0 | Sistema de Backup Activo"
        "</div>",
        unsafe_allow_html=True
    )

# ============================================================================
# EJECUCIÓN PRINCIPAL
# ============================================================================
if __name__ == "__main__":
    main()