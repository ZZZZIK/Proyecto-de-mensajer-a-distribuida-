@echo off
echo ============================================
echo   Compilando WhatsApp Distribuido Multinodo
echo ============================================
if not exist bin mkdir bin
if not exist logs mkdir logs
if not exist resultados mkdir resultados

echo Compilando paquete common...
javac -encoding UTF-8 -cp "lib\mysql-connector-j.jar" --release 21 -d bin src\common\Mensaje.java
if %ERRORLEVEL% NEQ 0 goto ERROR

echo Compilando paquete server...
javac -encoding UTF-8 -cp "lib\mysql-connector-j.jar;bin" --release 21 -d bin src\server\Seguridad.java src\server\GestorBD.java
if %ERRORLEVEL% NEQ 0 goto ERROR

echo Compilando paquete node...
javac -encoding UTF-8 -cp "lib\mysql-connector-j.jar;bin" --release 21 -d bin src\node\InfoNodo.java src\node\RelojVectorial.java src\node\LogDistribuido.java
if %ERRORLEVEL% NEQ 0 goto ERROR

javac -encoding UTF-8 -cp "lib\mysql-connector-j.jar;bin" --release 21 -d bin src\node\ConectorNodos.java src\node\GestorHeartbeat.java src\node\AlgoritmoBully.java src\node\MutexRicartAgrawala.java src\node\ConsensoRaft.java
if %ERRORLEVEL% NEQ 0 goto ERROR

echo Compilando ManejadorCliente y Servidor...
javac -encoding UTF-8 -cp "lib\mysql-connector-j.jar;bin" --release 21 -d bin src\server\ManejadorCliente.java src\server\Servidor.java
if %ERRORLEVEL% NEQ 0 goto ERROR

echo Compilando NodoServidor...
javac -encoding UTF-8 -cp "lib\mysql-connector-j.jar;bin" --release 21 -d bin src\node\NodoServidor.java
if %ERRORLEVEL% NEQ 0 goto ERROR

echo Compilando cliente...
javac -encoding UTF-8 -cp "lib\mysql-connector-j.jar;bin" --release 21 -d bin src\client\Cliente.java
if %ERRORLEVEL% NEQ 0 goto ERROR

echo Compilando generador de carga...
javac -encoding UTF-8 -cp "lib\mysql-connector-j.jar;bin" --release 21 -d bin src\loadtest\GeneradorCarga.java
if %ERRORLEVEL% NEQ 0 goto ERROR

echo.
echo [OK] Compilacion exitosa de todos los modulos.
echo.
echo Para ejecutar el sistema distribuido (3 nodos):
echo   1. Nodo 1: run_nodo1.bat
echo   2. Nodo 2: run_nodo2.bat
echo   3. Nodo 3: run_nodo3.bat
echo   4. Cliente: run_client.bat
echo   5. Prueba de carga: run_loadtest.bat
echo.
echo Para ejecutar en modo standalone (servidor unico):
echo   1. Servidor: run_server.bat
echo   2. Cliente:  run_client.bat
echo.
echo IMPORTANTE: Asegurese de tener XAMPP con MySQL encendido
echo y haber ejecutado el script SQL en phpMyAdmin.
goto END

:ERROR
echo.
echo [ERROR] Hubo errores de compilacion.
:END
pause

