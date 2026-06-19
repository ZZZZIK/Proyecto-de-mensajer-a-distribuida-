# Plan de Implementación — Entrega 2: WhatsApp Distribuido Multinodo

## Diagnóstico del Estado Actual

El proyecto actual es un **chat cliente-servidor centralizado** con:
- 1 servidor único (`Servidor.java`) que centraliza toda la lógica
- N clientes Swing (`Cliente.java`) que se conectan al servidor
- Base de datos MySQL para persistencia (usuarios, historial, buzón offline)
- Serialización de objetos Java sobre TCP
- Seguridad básica (AES para mensajes, SHA-256 para passwords)

> [!CAUTION]
> **Gap crítico:** La arquitectura actual depende 100% de un único servidor. Si cae, todo el sistema muere. Esto viola directamente el requisito fundamental de la Entrega 2.

---

## Decisiones Técnicas Clave

### 1. Relojes Vectoriales (no Lamport)

**Recomendación: Relojes Vectoriales** por las siguientes razones:

| Criterio | Lamport | Vectorial |
|---|---|---|
| Detecta causalidad real | ❌ Solo orden parcial | ✅ Causalidad completa |
| "Si a→b entonces C(a) < C(b)" | ✅ | ✅ |
| "Si C(a) < C(b) entonces a→b" | ❌ (falso positivo) | ✅ |
| Concurrencia detectable | ❌ | ✅ (si V(a) ∥ V(b)) |
| Complejidad | O(1) por mensaje | O(n) por mensaje (n=nodos) |
| Nota en rúbrica | Correcto | Más profesional y completo |

Con solo 3 nodos, el overhead de O(n) es insignificante. Los relojes vectoriales permiten **demostrar concurrencia real** en el informe, lo cual impresiona más.

### 2. Coordinación: Exclusión Mutua + Elección de Coordinador (ambos)

La rúbrica dice "al menos uno", pero para **nota máxima (7 en código)** necesitamos que todo funcione impecable. Implementaremos:

- **Exclusión mutua: Ricart-Agrawala** — más elegante que anillo, demuestra multicasting y timestamps vectoriales.
- **Elección de coordinador: Algoritmo Bully** — se dispara automáticamente al detectar caída del coordinador.

El recurso protegido por exclusión mutua será la **escritura en el historial de la base de datos**, que es el recurso compartido real del sistema.

### 3. Consenso Simplificado (Puntaje Extra §2.3)

Implementaremos un **consenso tipo Raft simplificado** para replicar el estado de la lista de usuarios conectados entre todos los nodos. Esto otorga el puntaje extra y complementa la tolerancia a fallos.

> [!IMPORTANT]
> **Puntaje extra confirmado:** El punto 2.3 dice textualmente "Consenso simplificado (opcional, con puntaje extra)". Implementaremos un esquema Raft reducido (líder, seguidores, log replicado) para mantener consistente la lista de membresía entre nodos.

### 4. Topología: 2 LAN + 1 WAN

Para el video demostrativo y la prueba de carga:

```
┌─────────────────────────────────────┐     ┌─────────────────────────┐
│       RED LAN (192.168.1.0/24)      │     │   RED REMOTA (WAN)      │
│                                     │     │                         │
│  ┌─────────┐      ┌─────────┐      │     │  ┌─────────┐           │
│  │ Nodo 1  │◄────►│ Nodo 2  │      │     │  │ Nodo 3  │           │
│  │ :5001   │      │ :5002   │      │◄───►│  │ :5003   │           │
│  │ PC #1   │      │ PC #2   │      │     │  │ PC #3   │           │
│  └─────────┘      └─────────┘      │     │  └─────────┘           │
│                                     │     │  (otra red / Ngrok)    │
└─────────────────────────────────────┘     └─────────────────────────┘
```

- **Nodo 1 y 2:** Dos PCs en la misma LAN (o misma PC con puertos distintos para desarrollo)
- **Nodo 3:** PC en otra red (o simulado con Ngrok/port-forwarding para demostrar WAN)
- Cada nodo es un **servidor completo** que puede aceptar clientes y comunicarse peer-to-peer con los otros nodos

> [!NOTE]
> Para desarrollo y pruebas, los 3 nodos pueden correr en `localhost` con puertos diferentes (5001, 5002, 5003). Para el video, idealmente 2 en LAN real + 1 remoto.

---

## Arquitectura Propuesta

### Transformación de Cliente-Servidor → Peer-to-Peer Multiservidor

```
ANTES (Parcial):                    DESPUÉS (Final):
                                    
  Cliente ──► Servidor ◄── Cliente    Cliente ──► Nodo1 ◄──► Nodo2 ◄── Cliente
                                                   ▲          ▲
                                                   │          │
                                                   ▼          ▼
                                       Cliente ──► Nodo3 ◄──────┘  ◄── Cliente
```

Cada **Nodo** es un proceso Java independiente que:
1. Acepta conexiones de clientes (como el servidor actual)
2. Se comunica peer-to-peer con otros nodos via TCP
3. Participa en protocolos distribuidos (relojes, exclusión mutua, elección, consenso)
4. Tiene su propia instancia de base de datos (o comparte una con replicación lógica)

---

## Proposed Changes

### Componente 1: Protocolo de Mensajes Inter-Nodo

Extender `Mensaje.java` para soportar comunicación entre nodos, no solo cliente-servidor.

#### [MODIFY] [Mensaje.java](file:///c:/Users/Alonso/Desktop/Proyecto%20paralela/src/common/Mensaje.java)

Agregar nuevos campos y tipos de mensaje:

```java
// Nuevos tipos de mensaje para comunicación inter-nodo
public static final String HEARTBEAT        = "HEARTBEAT";        // Latido entre nodos
public static final String HEARTBEAT_ACK    = "HEARTBEAT_ACK";    // ACK del heartbeat
public static final String ELECCION         = "ELECCION";         // Algoritmo Bully
public static final String ELECCION_OK      = "ELECCION_OK";      // Respuesta Bully
public static final String COORDINADOR      = "COORDINADOR";      // Anuncio de nuevo coordinador
public static final String MUTEX_REQUEST    = "MUTEX_REQUEST";    // Ricart-Agrawala: solicitud
public static final String MUTEX_REPLY      = "MUTEX_REPLY";      // Ricart-Agrawala: permiso
public static final String MUTEX_RELEASE    = "MUTEX_RELEASE";    // Ricart-Agrawala: liberación
public static final String CONSENSUS_APPEND = "CONSENSUS_APPEND"; // Raft: append entries
public static final String CONSENSUS_VOTE   = "CONSENSUS_VOTE";   // Raft: vote request
public static final String NODO_JOIN        = "NODO_JOIN";        // Descubrimiento de nodo
public static final String NODO_LEAVE       = "NODO_LEAVE";       // Nodo se va
public static final String SYNC_ESTADO      = "SYNC_ESTADO";      // Sincronizar estado

// Nuevos campos
private int[] relojVectorial;     // Reloj vectorial [nodo0, nodo1, nodo2, ...]
private int nodoOrigen;           // ID del nodo que origina el mensaje
private int nodoDestino;          // ID del nodo destino (-1 = broadcast)
private long secuenciaGlobal;     // Número de secuencia para consenso
```

---

### Componente 2: Infraestructura de Nodo (Nuevo paquete `node`)

#### [NEW] `src/node/NodoServidor.java`

Clase principal de cada nodo. Reemplaza conceptualmente a `Servidor.java` en la nueva arquitectura.

**Responsabilidades:**
- Escuchar conexiones de clientes en su puerto asignado
- Mantener conexiones TCP persistentes con otros nodos (mesh completo)
- Gestionar el reloj vectorial local
- Participar en protocolos de coordinación
- Detectar fallos via heartbeats

```java
public class NodoServidor {
    private final int nodoId;              // 0, 1, 2
    private final int puerto;              // 5001, 5002, 5003
    private final int[] relojVectorial;    // [3] para 3 nodos
    private final Map<Integer, Socket> conexionesNodos;     // Conexiones P2P
    private final Map<String, ManejadorCliente> clientes;   // Clientes locales
    private final List<InfoNodo> miembros;                  // Lista de membresía
    private volatile int coordinadorActual;                  // ID del coordinador
    private volatile boolean activo;
    
    // Sub-componentes (composición)
    private RelojVectorial reloj;
    private GestorHeartbeat gestorHeartbeat;
    private AlgoritmoBully algoritmoBully;
    private MutexRicartAgrawala mutex;
    private ConsensoRaft consenso;
    private MetricasCollector metricas;
}
```

#### [NEW] `src/node/InfoNodo.java`

Información de un nodo en la lista de membresía:

```java
public class InfoNodo implements Serializable {
    private int id;
    private String host;
    private int puerto;
    private boolean activo;
    private long ultimoHeartbeat;
}
```

#### [NEW] `src/node/ConectorNodos.java`

Maneja las conexiones TCP peer-to-peer entre nodos:
- Establece conexiones con todos los nodos conocidos al iniciar
- Reconecta automáticamente si se pierde una conexión
- Envía/recibe mensajes inter-nodo en hilos dedicados

---

### Componente 3: Reloj Vectorial

#### [NEW] `src/node/RelojVectorial.java`

```java
public class RelojVectorial {
    private final int[] vector;    // Un slot por nodo
    private final int miId;        // ID de este nodo
    private final Object lock = new Object();
    
    // Evento local: vector[miId]++
    public int[] eventoLocal();
    
    // Enviar: vector[miId]++, retorna copia
    public int[] prepararEnvio();
    
    // Recibir: vector[i] = max(vector[i], recibido[i]) ∀i, luego vector[miId]++
    public int[] recibirMensaje(int[] relojRecibido);
    
    // Comparar causalidad
    public static boolean esCausal(int[] a, int[] b);  // a → b
    public static boolean esConcurrente(int[] a, int[] b);  // a ∥ b
    
    // Para log
    public String toString();  // "[3, 1, 5]"
}
```

Cada mensaje que pase por el sistema (tanto inter-nodo como cliente→nodo→nodo→cliente) llevará su marca vectorial. Se registrará en el log con formato:

```
[2026-06-18 23:45:12] [V=[3,1,5]] TEXTO: Alice → Bob "Hola"
[2026-06-18 23:45:12] [V=[3,2,5]] TEXTO: Carlos → Alice "Hey"  ← Concurrente con el anterior
```

---

### Componente 4: Exclusión Mutua — Ricart-Agrawala

#### [NEW] `src/node/MutexRicartAgrawala.java`

Protege el recurso compartido: **escritura en la base de datos del historial** (el recurso crítico real del sistema).

```java
public class MutexRicartAgrawala {
    private final NodoServidor nodo;
    private volatile boolean solicitando;       // ¿Estoy pidiendo acceso?
    private volatile boolean enSeccionCritica;   // ¿Estoy dentro?
    private int[] timestampSolicitud;            // Mi reloj al solicitar
    private int respuestasRecibidas;             // Contador de OKs
    private final Queue<Integer> diferidos;      // Nodos cuya respuesta diferí
    
    // Solicitar acceso: enviar REQUEST a todos, esperar N-1 REPLYs
    public void solicitarAcceso();
    
    // Liberar: enviar REPLY a todos los diferidos
    public void liberarAcceso();
    
    // Al recibir REQUEST de otro nodo
    public void procesarSolicitud(int nodoSolicitante, int[] timestampSolicitante);
    
    // Al recibir REPLY
    public void procesarRespuesta(int nodoRespondedor);
}
```

**¿Qué se protege?** La escritura del historial de mensajes en la BD. Cuando un mensaje llega a un nodo, el nodo debe adquirir el mutex distribuido antes de escribir en `historial_mensajes`. Esto garantiza que no haya escrituras concurrentes corruptas entre nodos.

---

### Componente 5: Elección de Coordinador — Algoritmo Bully

#### [NEW] `src/node/AlgoritmoBully.java`

```java
public class AlgoritmoBully {
    private final NodoServidor nodo;
    private volatile int coordinadorActual;
    private volatile boolean eleccionEnProceso;
    
    // Iniciar elección: enviar ELECCION a todos los nodos con ID mayor
    public void iniciarEleccion();
    
    // Al recibir ELECCION de un nodo con ID menor: responder OK e iniciar mi propia elección
    public void procesarEleccion(int nodoSolicitante);
    
    // Al recibir OK: alguien con ID mayor está vivo, me retiro
    public void procesarOk();
    
    // Si no recibo OK en timeout: soy el coordinador, enviar COORDINADOR a todos
    public void declararseCoordinador();
    
    // Al recibir COORDINADOR: actualizar coordinadorActual
    public void procesarCoordinador(int nuevoCoordinador);
}
```

**¿Cuándo se dispara?** Automáticamente cuando el `GestorHeartbeat` detecta que el nodo coordinador no responde (3 heartbeats fallidos consecutivos).

**¿Qué hace el coordinador?** Es responsable de:
1. Asignar el orden global de mensajes (secuenciador)
2. Gestionar la replicación de la lista de membresía
3. Ser el líder en el consenso Raft

---

### Componente 6: Consenso Simplificado (Raft Reducido) — Puntaje Extra

#### [NEW] `src/node/ConsensoRaft.java`

Implementa un Raft simplificado para replicar la **lista de usuarios conectados globalmente** entre todos los nodos.

```java
public class ConsensoRaft {
    // Estados del nodo en Raft
    enum EstadoRaft { SEGUIDOR, CANDIDATO, LIDER }
    
    private EstadoRaft estado;
    private int termActual;              // Término actual (época)
    private int votadoPor;               // A quién voté en este término
    private List<EntradaLog> log;        // Log replicado
    private int commitIndex;             // Última entrada confirmada
    
    // El líder (coordinador) propone entradas:
    //   "USUARIO_CONECTADO:Alice:Nodo1"
    //   "USUARIO_DESCONECTADO:Bob:Nodo2"
    
    // Append Entries: el líder envía entradas a los seguidores
    public void enviarAppendEntries();
    
    // Los seguidores confirman, cuando hay quórum (2/3) se commitea
    public void procesarAppendEntries(EntradaLog entrada, int termLider);
    
    // Request Vote: si el líder cae, se elige nuevo
    public void solicitarVotos();
    public void procesarSolicitudVoto(int candidato, int termCandidato);
}
```

**¿Qué se replica?** La lista global de membresía (qué usuarios están conectados en qué nodo). Cuando un usuario se conecta al Nodo 1, el líder propone la entrada al log, espera quórum (mayoría: 2 de 3), y luego todos los nodos tienen la misma vista consistente.

---

### Componente 7: Tolerancia a Fallos

#### [NEW] `src/node/GestorHeartbeat.java`

```java
public class GestorHeartbeat {
    private static final long INTERVALO_HEARTBEAT = 2000;  // 2 segundos
    private static final int MAX_FALLOS = 3;                // 3 fallos = nodo caído
    
    private final Map<Integer, Integer> contadorFallos;     // nodoId → fallos consecutivos
    private final Map<Integer, Long> ultimoHeartbeat;       // nodoId → timestamp último HB
    
    // Hilo que envía heartbeats cada 2 segundos
    public void iniciar();
    
    // Al recibir heartbeat: resetear contador de fallos
    public void procesarHeartbeat(int nodoOrigen);
    
    // Al detectar nodo caído (3 fallos seguidos):
    //   1. Marcar nodo como inactivo en membresía
    //   2. Si era coordinador → disparar Bully
    //   3. Redistribuir clientes del nodo caído (si es posible)
    //   4. Registrar evento en log con marca vectorial
    private void nodoDetectadoCaido(int nodoId);
    
    // Reintegración: cuando un nodo vuelve
    public void procesarReconexion(int nodoId);
}
```

**Flujo de recuperación:**
1. Nodo 2 deja de enviar heartbeats
2. Nodos 1 y 3 detectan 3 fallos consecutivos (6 segundos)
3. Si Nodo 2 era coordinador → Bully elige nuevo (el de mayor ID)
4. Los clientes del Nodo 2 reciben error de conexión
5. Los clientes pueden reconectarse a Nodo 1 o Nodo 3
6. Cuando Nodo 2 vuelve, envía `NODO_JOIN` y se reintegra

---

### Componente 8: Generador de Carga y Métricas

#### [NEW] `src/loadtest/GeneradorCarga.java`

```java
public class GeneradorCarga {
    private static final int NUM_CLIENTES = 50;          // Mínimo requerido
    private static final int DURACION_SEGUNDOS = 60;     // Mínimo requerido
    
    // Fase 1: Conectar 50 clientes simulados (hilos)
    // Fase 2: Cada cliente envía mensajes de texto y archivos pequeños
    // Fase 3: A los 30 segundos, derribar un nodo (falla inducida)
    // Fase 4: Continuar midiendo durante la recuperación
    
    // Métricas que recolecta:
    private AtomicLong peticionesTotales;
    private AtomicLong peticionesExitosas;
    private AtomicLong peticionesFallidas;
    private List<Long> latencias;  // Para calcular promedio y p95
    private AtomicLong mensajesCoordinacion;  // Mensajes del algoritmo
}
```

#### [NEW] `src/loadtest/MetricasCollector.java`

Recolector profesional de métricas con dashboard en consola:

```java
public class MetricasCollector {
    // Métricas en tiempo real
    private final HdrHistogram histogramaLatencia;   // Para p95 preciso
    private final AtomicLong throughputActual;
    private final AtomicLong erroresActuales;
    
    // Imprime en consola cada segundo:
    // ┌─────────────────────────────────────────────────────┐
    // │  MÉTRICAS EN TIEMPO REAL — WhatsApp Distribuido     │
    // ├─────────────┬──────────────┬──────────────┬─────────┤
    // │ Throughput   │ Latencia Avg │ Latencia p95 │ Errores │
    // │ 342 req/s    │ 12.3 ms      │ 45.7 ms      │ 0.2%    │
    // ├─────────────┴──────────────┴──────────────┴─────────┤
    // │ Mensajes coordinación: 1,234  │ Nodos activos: 3/3   │
    // │ Mutex requests: 89            │ Elecciones: 0         │
    // └─────────────────────────────────────────────────────┘
    
    // Al final genera:
    // - Archivo CSV con todas las muestras
    // - Resumen en texto para el informe
    // - Datos para gráficos (throughput vs tiempo, latencia vs tiempo)
}
```

#### [NEW] `src/loadtest/ReporteResultados.java`

Genera un reporte HTML con:
- Tabla resumen de métricas
- Gráfico de throughput vs tiempo (usando Chart.js embebido o ASCII art)
- Gráfico de latencia vs tiempo
- Marcador del momento de la falla inducida
- Tiempo de recuperación medido

---

### Componente 9: Sistema de Logging Profesional

#### [NEW] `src/node/LogDistribuido.java`

```java
public class LogDistribuido {
    // Formato de cada entrada:
    // [2026-06-18T23:45:12.345] [NODO-1] [V=[3,1,5]] [EVENTO] descripción
    
    // Categorías de eventos:
    // MENSAJE    — Mensaje de texto/archivo procesado
    // MUTEX      — Solicitud/respuesta/liberación de exclusión mutua  
    // ELECCION   — Eventos del algoritmo Bully
    // HEARTBEAT  — Detección de latidos
    // FALLO      — Nodo detectado como caído
    // CONSENSO   — Entradas del log Raft
    // METRICAS   — Punto de medición
    
    // Escribe a:
    // 1. Consola (con colores ANSI)
    // 2. Archivo de log rotativo (logs/nodo1_2026-06-18.log)
    // 3. Cola en memoria para que MetricasCollector pueda contar eventos
}
```

---

### Componente 10: Modificaciones a Clases Existentes

#### [MODIFY] [Servidor.java](file:///c:/Users/Alonso/Desktop/Proyecto%20paralela/src/server/Servidor.java)

Se refactoriza para convertirse en la capa de servicio que usa `NodoServidor`. El `main()` ahora:
1. Lee configuración del nodo (ID, puerto, lista de peers) desde argumentos o archivo `.properties`
2. Crea e inicia un `NodoServidor`
3. Inicia el hilo aceptador de clientes
4. Inicia el hilo conector de peers

#### [MODIFY] [ManejadorCliente.java](file:///c:/Users/Alonso/Desktop/Proyecto%20paralela/src/server/ManejadorCliente.java)

- Conecta con `NodoServidor`
- Integra el reloj vectorial al reenviar mensajes
- Protege la escritura en historial llamando al mutex distribuido

#### [MODIFY] [Cliente.java](file:///c:/Users/Alonso/Desktop/Proyecto%20paralela/src/client/Cliente.java)

- Permite reconexiones a otros nodos
- Muestra el ID del nodo conectado en la UI

#### [MODIFY] [GestorBD.java](file:///c:/Users/Alonso/Desktop/Proyecto%20paralela/src/server/GestorBD.java)

- Protegido por mutex Ricart-Agrawala
- Integra la tabla `log_eventos` para los logs de eventos del sistema
