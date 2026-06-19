@echo off
echo ============================================
echo   Iniciando NODO 2 (ID=1, Puerto=5002)
echo ============================================
java -cp "bin;lib\mysql-connector-j.jar" node.NodoServidor config\nodo2.properties
pause
