@echo off
echo ============================================
echo   Cliente WhatsApp Distribuido
echo ============================================
echo.
echo Conectando al sistema distribuido...
echo (Por defecto se conecta a localhost:5001)
echo.
java -cp "bin;lib\mysql-connector-j.jar" client.Cliente
