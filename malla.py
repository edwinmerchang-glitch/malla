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