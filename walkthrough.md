# Walkthrough — Entrega 2: WhatsApp Distribuido Multinodo

Se ha completado e integrado con éxito el sistema distribuido multinodo para el chat de WhatsApp, cumpliendo con todos los requisitos obligatorios y el puntaje extra de Consenso Raft.

## Cambios Realizados

Se han implementado y refactorizado los siguientes componentes:

### 1. Protocolo y Mensajes
* [Mensaje.java](file:///c:/Users/Alonso/Desktop/Proyecto%20paralela/src/common/Mensaje.java): Extendido para incluir nuevos tipos de mensajes inter-nodo (`HEARTBEAT`, `ELECCION`, `MUTEX_REQUEST`, `RAFT_APPEND`, etc.) y campos para el reloj vectorial, término de Raft y secuencias globales.

### 2. Infraestructura de Nodo
* [NodoServidor.java](file:///c:/Users/Alonso/Desktop/Proyecto%20paralela/src/node/NodoServidor.java): El proceso central que corre en cada nodo y coordina el resto de submódulos TCP.
* [InfoNodo.java](file:///c:/Users/Alonso/Desktop/Proyecto%20paralela/src/node/InfoNodo.java): Estructura serializable que almacena el estado (activo/inactivo), IP, puerto y tiempo del último heartbeat de cada nodo.
* [ConectorNodos.java](file:///c:/Users/Alonso/Desktop/Proyecto%20paralela/src/node/ConectorNodos.java): Administra la comunicación de sockets TCP persistentes en malla (mesh completo) entre nodos remotos.

### 3. Algoritmos Distribuidos y Coordinación
* [RelojVectorial.java](file:///c:/Users/Alonso/Desktop/Proyecto%20paralela/src/node/RelojVectorial.java): Reloj vectorial de contadores para establecer causalidad y concurrencia.
* [LogDistribuido.java](file:///c:/Users/Alonso/Desktop/Proyecto%20paralela/src/node/LogDistribuido.java): Sistema de logs profesional con marcas vectoriales y salida formateada con colores ANSI en la terminal y persistencia en disco (`logs/`).
* [GestorHeartbeat.java](file:///c:/Users/Alonso/Desktop/Proyecto%20paralela/src/node/GestorHeartbeat.java): Monitoreo mutuo periódico de salud. Si un nodo no responde a 3 pings seguidos (6 segundos), se marca como inactivo y se reporta.
* [AlgoritmoBully.java](file:///c:/Users/Alonso/Desktop/Proyecto%20paralela/src/node/AlgoritmoBully.java): Re-elección automática del nodo con mayor ID disponible cuando cae el coordinador actual.
* [MutexRicartAgrawala.java](file:///c:/Users/Alonso/Desktop/Proyecto%20paralela/src/node/MutexRicartAgrawala.java): Exclusión mutua distribuida que protege la escritura concurrente en la base de datos de historial permanente.
* [ConsensoRaft.java](file:///c:/Users/Alonso/Desktop/Proyecto%20paralela/src/node/ConsensoRaft.java) (¡PUNTOS EXTRA!): Protocolo Raft simplificado para replicar de manera consistente la lista de usuarios online en todos los nodos.

### 4. Cliente y Generador de Carga
* [Cliente.java](file:///c:/Users/Alonso/Desktop/Proyecto%20paralela/src/client/Cliente.java): Modificado para mostrar el nodo al que se conecta y permitir cambiar el puerto de conexión dinámicamente si hay caídas.
* [GeneradorCarga.java](file:///c:/Users/Alonso/Desktop/Proyecto%20paralela/src/loadtest/GeneradorCarga.java): Generador con 50 hilos concurrentes que simulan clientes y envían tráfico sostenido.

---

## Mejoras y Corrección de Bugs Realizadas

1. **Corrección de ConsensoRaft**: Corregido un bug crítico donde el Líder descartaba o ignoraba las propuestas enviadas por los seguidores (debido a la verificación estricta de la presencia de dos puntos en el contenido del mensaje). Ahora detecta correctamente propuestas de 3 partes de forma dinámica.
2. **Medición Automatizada del Tiempo de Recuperación**: Se integraron notificaciones estructuradas del sistema (`[FALLO DETECTADO]` y `[SISTEMA RECUPERADO]`) para que el `GeneradorCarga` calcule y muestre automáticamente el tiempo de recuperación exacto en segundos (con precisión de milisegundos) en el reporte de métricas en tiempo real.
3. **Instrucciones Interactivas**: A los 30 segundos de iniciada la prueba de carga, el panel muestra una instrucción clara incitando al usuario a derribar al coordinador actual, lo cual facilita la demostración y video del proyecto.
4. **Registro de Logs en la BD**: Se habilitó la persistencia de los logs de eventos distribuidos directamente en la tabla `log_eventos` de MySQL de forma asíncrona (usando hilos en segundo plano para no demorar la entrega de mensajes), logrando que la tabla se autocomplete en tiempo real con marcas de reloj vectorial y detalles de exclusión mutua y elecciones.

---

## Plan de Verificación y Demostración

Para ejecutar la demostración completa del sistema:

1. **Base de Datos**: Iniciar MySQL en XAMPP y asegurarse de tener la base de datos `chat_distribuido` creada con el script [chat_distribuido.sql](file:///c:/Users/Alonso/Desktop/Proyecto%20paralela/database/chat_distribuido.sql).
2. **Compilar**: Ejecutar [compile.bat](file:///c:/Users/Alonso/Desktop/Proyecto%20paralela/compile.bat).
3. **Iniciar Nodos**: Ejecutar en tres consolas distintas:
   * [run_nodo1.bat](file:///c:/Users/Alonso/Desktop/Proyecto%20paralela/run_nodo1.bat) (Puerto 5001 / Nodos 5101)
   * [run_nodo2.bat](file:///c:/Users/Alonso/Desktop/Proyecto%20paralela/run_nodo2.bat) (Puerto 5002 / Nodos 5102)
   * [run_nodo3.bat](file:///c:/Users/Alonso/Desktop/Proyecto%20paralela/run_nodo3.bat) (Puerto 5003 / Nodos 5103)
4. **Verificación de Elección**: Los logs mostrarán que el **Nodo 2** (ID=2, el mayor) se autodeclara como Coordinador Inicial.
5. **Prueba de Carga y Falla**: Ejecutar [run_loadtest.bat](file:///c:/Users/Alonso/Desktop/Proyecto%20paralela/run_loadtest.bat).
   * La terminal mostrará el dashboard interactivo de métricas.
   * A los 30 segundos, aparecerá la advertencia pidiendo cerrar el Nodo 2 (coordinador).
   * Al cerrar la terminal del Nodo 2, el Gestor de Heartbeats del Nodo 0 y Nodo 1 lo detectará y el Nodo 1 (mayor ID restante) ganará la elección Bully.
   * El dashboard de carga medirá la latencia promedio, percentil p95, errores y reportará el tiempo de recuperación (aprox. 10s: 6s de detección + 4s de elección).
6. **Gráficos y Logs**: Al terminar la prueba de carga, se generará un reporte interactivo en HTML con Chart.js en la carpeta `resultados/` y logs detallados en `logs/`.
