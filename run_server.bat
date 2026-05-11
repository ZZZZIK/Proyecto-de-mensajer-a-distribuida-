@echo off
echo ============================================
echo   Servidor WhatsApp Distribuido
echo ============================================
echo Requiere: XAMPP con MySQL encendido
echo Puerto: 5000
echo Presione Ctrl+C para detener el servidor.
echo.
java -cp "bin;lib\mysql-connector-j.jar" server.Servidor 5000
pause
