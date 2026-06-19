@echo off
echo ============================================
echo   Generador de Carga — WhatsApp Distribuido
echo ============================================
echo.
echo Uso: Este script lanza 50 clientes simultaneos
echo      durante 60 segundos contra el Nodo 1.
echo.
echo IMPORTANTE: Los 3 nodos deben estar corriendo
echo             antes de ejecutar esta prueba.
echo.
echo Conectando a localhost:5001 (Nodo 1)...
echo.
java -cp "bin;lib\mysql-connector-j.jar" loadtest.GeneradorCarga localhost 5001 50 60
pause
