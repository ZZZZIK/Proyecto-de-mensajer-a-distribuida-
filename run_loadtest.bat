@echo off
echo ============================================
echo   Generador de Carga — WhatsApp Distribuido
echo ============================================
echo.
echo Uso: Este script lanza 50 clientes simultaneos
echo      durante 60 segundos distribuidos entre los nodos.
echo.
echo IMPORTANTE: Los 3 nodos deben estar corriendo
echo             antes de ejecutar esta prueba.
echo.
echo Conectando a localhost en puertos 5001, 5002, 5003...
echo.
java -cp "bin;lib\mysql-connector-j.jar" loadtest.GeneradorCarga localhost 5001,5002,5003 50 60
pause

