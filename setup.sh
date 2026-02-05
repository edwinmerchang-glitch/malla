#!/bin/bash

# Crear directorio para configuraciones de Streamlit
mkdir -p ~/.streamlit/

# Crear archivo de configuración
echo "\
[server]\n\
headless = true\n\
port = \$PORT\n\
enableCORS = false\n\
enableXsrfProtection = false\n\
\n\
[browser]\n\
serverAddress = \"0.0.0.0\"\n\
gatherUsageStats = false\n\
" > ~/.streamlit/config.toml

# Crear directorio para backups (importante para tu app)
mkdir -p turnos_backups
chmod 755 turnos_backups

echo "✅ Configuración completada"