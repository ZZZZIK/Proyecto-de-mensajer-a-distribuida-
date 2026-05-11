@echo off
echo ============================================
echo   Compilando WhatsApp Distribuido...
echo ============================================
if not exist bin mkdir bin
javac -encoding UTF-8 -cp "lib\mysql-connector-j.jar" -d bin src\common\Mensaje.java src\server\Seguridad.java src\server\GestorBD.java src\server\Servidor.java src\server\ManejadorCliente.java src\client\Cliente.java
if %ERRORLEVEL% EQU 0 (
    echo.
    echo [OK] Compilacion exitosa.
    echo.
    echo Para ejecutar:
    echo   1. Servidor: run_server.bat
    echo   2. Cliente:  run_client.bat
    echo.
    echo IMPORTANTE: Asegurese de tener XAMPP con MySQL encendido
    echo y haber ejecutado el script SQL en phpMyAdmin.
) else (
    echo.
    echo [ERROR] Hubo errores de compilacion.
)
pause
