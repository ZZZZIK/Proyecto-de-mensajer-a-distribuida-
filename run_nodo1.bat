@echo off
echo ============================================
echo   Iniciando NODO 1 (ID=0, Puerto=5001)
echo ============================================
java -cp "bin;lib\mysql-connector-j.jar" node.NodoServidor config\nodo1.properties
pause
