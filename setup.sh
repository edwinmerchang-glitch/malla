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

echo "✅ Configuración de Streamlit creada"