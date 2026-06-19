@echo off
echo ============================================
echo   Iniciando NODO 3 (ID=2, Puerto=5003)
echo ============================================
java -cp "bin;lib\mysql-connector-j.jar" node.NodoServidor config\nodo3.properties
pause
