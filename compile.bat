@echo off
echo ============================================
echo   Compilando WhatsApp Distribuido...
echo ============================================
if not exist bin mkdir bin
javac -d bin src\common\Mensaje.java src\server\Servidor.java src\server\ManejadorCliente.java src\client\Cliente.java
if %ERRORLEVEL% EQU 0 (
    echo.
    echo [OK] Compilacion exitosa.
    echo.
    echo Para ejecutar:
    echo   1. Servidor: run_server.bat
    echo   2. Cliente:  run_client.bat
) else (
    echo.
    echo [ERROR] Hubo errores de compilacion.
)
pause
