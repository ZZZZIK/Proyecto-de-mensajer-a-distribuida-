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
echo Conectando a nodos distribuidos (2 locales, 1 remoto)...
echo.

:: Ejecucion Tailscale (2 locales en tu PC, 1 remoto en PC B):
java -cp "bin;lib\mysql-connector-j.jar" loadtest.GeneradorCarga localhost localhost:5001,localhost:5002,100.110.116.123:5003 50 60

:: Ejecucion local 100% anterior:
:: java -cp "bin;lib\mysql-connector-j.jar" loadtest.GeneradorCarga localhost 5001,5002,5003 50 60

pause


