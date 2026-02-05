#!/bin/bash

# Crear directorio para configuraciones de Streamlit
mkdir -p ~/.streamlit/

# Crear archivo de configuración
cat > ~/.streamlit/config.toml << EOF
[server]
headless = true
port = \$PORT
enableCORS = false
enableXsrfProtection = false

[browser]
serverAddress = "0.0.0.0"
gatherUsageStats = false
EOF

echo "✅ Configuración de Streamlit completada"