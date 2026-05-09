# Informe Completo de Modelado — WhatsApp Distribuido

Documento técnico para la construcción de los tres modelos requeridos: **Modelo Físico**, **Modelo Arquitectónico** y **Modelado de Funciones (Diagramas de Secuencia UML)**.

---

## 1. MODELO FÍSICO

### 1.1 Identificación de Nodos

El sistema se compone de dos tipos de nodos que se ejecutan como procesos Java independientes:

| Nodo | Archivo de Entrada | Descripción | Cantidad | Puerto |
|---|---|---|---|---|
| **Servidor Central** | `server.Servidor.main()` | Proceso que escucha conexiones TCP entrantes en un `ServerSocket`. Mantiene en memoria un `HashMap` con todos los clientes registrados. Actúa como enrutador central de mensajes. | **1 (único)** | TCP `5000` (configurable) |
| **Cliente N** | `client.Cliente.main()` | Proceso con interfaz gráfica Swing (`JFrame`). Se conecta al servidor mediante un `Socket(ip, puerto)`. No conoce la IP de otros clientes. | **N (ilimitados)** | Puerto efímero asignado por el SO |

**Nodos internos del Servidor (hilos, no procesos separados):**

| Componente Interno | Clase | Relación |
|---|---|---|
| Hilo Principal del Servidor | `Servidor.main()` | Bucle infinito en `serverSocket.accept()`, acepta conexiones |
| Hilo Manejador (1 por cliente) | `ManejadorCliente` (implementa `Runnable`) | Creado con `new Thread(manejador).start()` por cada conexión aceptada |

**Nodos internos del Cliente (hilos):**

| Componente Interno | Clase | Relación |
|---|---|---|
| Hilo Principal (EDT de Swing) | `Cliente` (extiende `JFrame`) | Maneja la interfaz gráfica y los eventos del usuario (botones, texto) |
| Hilo Receptor | `HiloReceptor` (clase interna, implementa `Runnable`) | Bucle infinito leyendo objetos `Mensaje` desde el socket con `entrada.readObject()` |

### 1.2 Entorno de Red

| Característica | Valor en el Proyecto |
|---|---|
| **Tipo de Red** | **LAN** (Red de Área Local). Los clientes se conectan al servidor usando su IPv4 privada (ej: `192.168.1.X`). También funciona en `localhost` (misma máquina). |
| **Protocolo de Transporte** | **TCP** (Transmission Control Protocol). Garantiza entrega ordenada y sin pérdida de paquetes. Implementado con `java.net.Socket` y `java.net.ServerSocket`. |
| **Protocolo de Aplicación** | **Serialización de objetos Java** (Marshalling). Se envían objetos `Mensaje` completos usando `ObjectOutputStream.writeObject()` y se reciben con `ObjectInputStream.readObject()`. |
| **Puerto del Servidor** | `5000` por defecto. Configurable como argumento de línea de comandos. |
| **Dirección de Binding** | `0.0.0.0` (implícito al usar `new ServerSocket(puerto)` sin especificar dirección). Acepta conexiones desde cualquier interfaz de red. |
| **Límite de transferencia** | Archivos de hasta **10 MB** (validado en `Cliente.enviarArchivo()`, línea 350). |

### 1.3 Representación Gráfica de la Interconexión (Topología de Estrella)

```mermaid
graph TB
    subgraph RED_LAN["Red LAN (192.168.1.0/24)"]
        S["Servidor Central<br/>ServerSocket TCP:5000<br/>Servidor.java<br/>IP: 192.168.1.10"]
        
        C1["Cliente 1<br/>Socket TCP<br/>Cliente.java<br/>IP: 192.168.1.20"]
        C2["Cliente 2<br/>Socket TCP<br/>Cliente.java<br/>IP: 192.168.1.30"]
        C3["Cliente N<br/>Socket TCP<br/>Cliente.java<br/>IP: 192.168.1.X"]
        
        C1 -->|"Conexión TCP persistente<br/>ObjectStream bidireccional"| S
        C2 -->|"Conexión TCP persistente<br/>ObjectStream bidireccional"| S
        C3 -->|"Conexión TCP persistente<br/>ObjectStream bidireccional"| S
    end
```

> [!IMPORTANT]
> **No existe comunicación directa entre clientes.** Todo el tráfico pasa obligatoriamente por el servidor central (topología de estrella). Si el Cliente 1 quiere enviar un mensaje al Cliente 2, el mensaje viaja: `Cliente 1 → Servidor → Cliente 2`.

### 1.4 Estructura de Archivos del Proyecto (Despliegue Físico)

```
Proyecto paralela/
├── compile.bat              ← Compila todo con javac hacia /bin
├── run_server.bat           ← Ejecuta: java -cp bin server.Servidor 5000
├── run_client.bat           ← Ejecuta: java -cp bin client.Cliente
├── src/
│   ├── common/
│   │   └── Mensaje.java     ← Objeto serializable (protocolo)
│   ├── server/
│   │   ├── Servidor.java    ← Punto de entrada del servidor
│   │   └── ManejadorCliente.java ← Runnable: un hilo por cliente
│   └── client/
│       └── Cliente.java     ← GUI Swing + HiloReceptor interno
├── bin/                     ← Archivos .class compilados
└── archivos_recibidos/      ← Carpeta donde se guardan archivos recibidos
```

---

## 2. MODELO ARQUITECTÓNICO

### 2.1 Tipo de Arquitectura

**Cliente-Servidor Centralizado** con las siguientes características:

| Propiedad | Implementación |
|---|---|
| **Patrón** | Cliente-Servidor de 2 capas (2-Tier) |
| **Comunicación** | Sockets TCP con serialización de objetos Java |
| **Concurrencia** | Thread-per-Connection (un hilo por cada cliente conectado) |
| **Sincronización** | Bloques `synchronized` sobre el `HashMap` compartido y los `ObjectOutputStream` |
| **Transparencia de Ubicación** | El cliente usa nombres de usuario, no direcciones IP, para identificar destinatarios |
| **Transparencia de Acceso** | La interfaz de envío es idéntica sin importar la ubicación física del destinatario |

### 2.2 Definición de Capas

```mermaid
graph TB
    subgraph CAPA_PRESENTACION["CAPA 1: Presentación (Cliente)"]
        GUI["Interfaz Gráfica Swing<br/>JFrame, JTextArea, JList, JButton<br/>Tema visual estilo WhatsApp"]
        EVENTOS["Eventos de Usuario<br/>ActionListener en botones<br/>WindowListener para cierre"]
    end
    
    subgraph CAPA_COMUNICACION["CAPA 2: Comunicación (Compartida)"]
        MSG["Objeto Mensaje<br/>Serializable (Marshalling)<br/>Tipos: TEXTO, ARCHIVO,<br/>CONECTAR, DESCONECTAR,<br/>LISTA_USUARIOS, NOTIFICACION"]
        STREAMS["ObjectOutputStream / ObjectInputStream<br/>Serialización automática de objetos Java<br/>sobre Socket TCP"]
    end
    
    subgraph CAPA_LOGICA["CAPA 3: Lógica de Negocio (Servidor)"]
        REGISTRO["Registro de Usuarios<br/>HashMap con synchronized"]
        RUTEO["Ruteo de Mensajes<br/>Resolución de nombre → ManejadorCliente"]
        BROADCAST["Broadcast<br/>Notificaciones y listas<br/>a todos los conectados"]
    end
    
    subgraph CAPA_CONCURRENCIA["CAPA 4: Concurrencia (Servidor)"]
        HILOS["Thread-per-Connection<br/>ManejadorCliente implements Runnable<br/>Un hilo independiente por cliente"]
        SYNC["Sincronización<br/>synchronized en HashMap<br/>synchronized en ObjectOutputStream"]
    end
    
    CAPA_PRESENTACION --> CAPA_COMUNICACION
    CAPA_COMUNICACION --> CAPA_LOGICA
    CAPA_LOGICA --> CAPA_CONCURRENCIA
```

### 2.3 Componentes Principales y sus Relaciones

```mermaid
classDiagram
    class Mensaje {
        -String emisor
        -String receptor
        -String tipo
        -String contenido
        -byte[] datosAdjuntos
        -String nombreArchivo
        -long timestamp
        +Mensaje(emisor, receptor, tipo, contenido)
        +Mensaje(emisor, receptor, nombreArchivo, datosAdjuntos)
        +getEmisor() String
        +getReceptor() String
        +getTipo() String
        +getContenido() String
        +getDatosAdjuntos() byte[]
        +getNombreArchivo() String
        +getHoraFormateada() String
    }
    Mensaje ..|> Serializable : implements

    class Servidor {
        -HashMap~String,ManejadorCliente~ clientesConectados$
        -int PUERTO_DEFECTO$
        +main(args)$
        +registrarCliente(nombre, manejador)$ boolean
        +removerCliente(nombre)$
        +reenviarMensaje(mensaje)$
        +enviarListaUsuarios()$
        +enviarNotificacion(texto)$
    }

    class ManejadorCliente {
        -Socket socket
        -ObjectOutputStream salida
        -ObjectInputStream entrada
        -String nombreUsuario
        +ManejadorCliente(socket)
        +run()
        +enviarMensaje(mensaje)
        +getNombreUsuario() String
    }
    ManejadorCliente ..|> Runnable : implements

    class Cliente {
        -Socket socket
        -ObjectOutputStream salida
        -ObjectInputStream entrada
        -String nombreUsuario
        -boolean conectado
        -JTextArea areaMensajes
        -JList~String~ listaUsuarios
        +Cliente()
        -construirInterfaz()
        -mostrarDialogoConexion()
        -conectar(nombre, ip, puerto)
        -enviarMensajeTexto()
        -enviarArchivo()
        -desconectar()
    }
    Cliente --|> JFrame : extends

    class HiloReceptor {
        +run()
        -recibirArchivo(mensaje)
        -actualizarListaUsuarios(listaCSV)
    }
    HiloReceptor ..|> Runnable : implements
    HiloReceptor --* Cliente : clase interna

    Servidor "1" o-- "N" ManejadorCliente : HashMap clientesConectados
    ManejadorCliente --> Mensaje : lee y escribe
    Cliente --> Mensaje : crea y envía
    HiloReceptor --> Mensaje : recibe y procesa
    ManejadorCliente --> Servidor : invoca métodos estáticos
```

### 2.4 Flujo General de Comunicación

```mermaid
graph LR
    A["Cliente A<br/>(GUI Swing)"] -->|"1. writeObject(Mensaje)"| SA["Socket A"]
    SA -->|"2. TCP/IP por la LAN"| SS["ServerSocket<br/>puerto 5000"]
    SS -->|"3. readObject()"| MA["ManejadorCliente A<br/>(Hilo dedicado)"]
    MA -->|"4. Servidor.reenviarMensaje()"| MAPA["HashMap<br/>clientesConectados<br/>(synchronized)"]
    MAPA -->|"5. Busca ManejadorCliente B"| MB["ManejadorCliente B<br/>(Hilo dedicado)"]
    MB -->|"6. writeObject(Mensaje)"| SB["Socket B"]
    SB -->|"7. TCP/IP por la LAN"| B["Cliente B<br/>(HiloReceptor)"]
    B -->|"8. Muestra en GUI<br/>SwingUtilities.invokeLater()"| GUI_B["JTextArea<br/>de Cliente B"]
```

### 2.5 Tipos de Mensaje (Protocolo de Aplicación)

| Tipo (constante) | Dirección | Propósito | Campos Utilizados |
|---|---|---|---|
| `CONECTAR` | Cliente → Servidor | Solicitud de login. Primer mensaje obligatorio tras abrir socket. | `emisor` = nombre del usuario |
| `DESCONECTAR` | Cliente → Servidor | Notificación de cierre voluntario. Dispara `removerCliente()`. | `emisor` = nombre del usuario |
| `TEXTO` | Cliente → Servidor → Cliente | **Función 1:** Envío de mensaje de texto privado. | `emisor`, `receptor`, `contenido` |
| `ARCHIVO` | Cliente → Servidor → Cliente | **Función 2:** Envío de archivo multimedia (hasta 10 MB). | `emisor`, `receptor`, `nombreArchivo`, `datosAdjuntos` (byte[]) |
| `LISTA_USUARIOS` | Servidor → Todos los Clientes | Broadcast con los nombres de todos los usuarios conectados separados por coma. | `contenido` = "usuario1,usuario2,usuario3" |
| `NOTIFICACION` | Servidor → Cliente(s) | Mensajes del sistema: bienvenida, errores, avisos de conexión/desconexión. | `contenido` = texto de la notificación |

---

## 3. MODELADO DE FUNCIONES (Diagramas de Secuencia UML)

### 3.1 FUNCIÓN 1: Envío y Recepción de Mensaje de Texto

**Escenario:** El Usuario A escribe "Hola" y lo envía al Usuario B. Ambos ya están conectados al servidor.

**Código involucrado:**
- Emisión: `Cliente.enviarMensajeTexto()` (líneas 294-324)
- Recepción en servidor: `ManejadorCliente.run()` → `case Mensaje.TEXTO` (línea 129)
- Ruteo: `Servidor.reenviarMensaje()` (líneas 206-232)
- Envío al destino: `ManejadorCliente.enviarMensaje()` (líneas 224-245)
- Recepción en cliente: `HiloReceptor.run()` → `case Mensaje.TEXTO` (línea 446)

```mermaid
sequenceDiagram
    participant UA as Cliente A<br/>(Hilo EDT Swing)
    participant SOA as ObjectOutputStream A<br/>(Socket de A)
    participant MA as ManejadorCliente A<br/>(Hilo del servidor)
    participant SRV as Servidor<br/>(HashMap synchronized)
    participant MB as ManejadorCliente B<br/>(Hilo del servidor)
    participant SOB as ObjectOutputStream B<br/>(Socket de B)
    participant HRB as HiloReceptor B<br/>(Hilo del Cliente B)
    participant UB as Cliente B<br/>(Hilo EDT Swing)

    UA->>UA: 1. Usuario A selecciona "Usuario B"<br/>en JList y escribe "Hola"
    UA->>UA: 2. Crea objeto Mensaje(<br/>emisor="A", receptor="B",<br/>tipo=TEXTO, contenido="Hola")
    UA->>SOA: 3. synchronized(salida)<br/>salida.writeObject(msg)<br/>salida.flush() + reset()
    Note over SOA: Marshalling:<br/>Objeto Mensaje → bytes
    SOA-->>MA: 4. Bytes viajan por TCP/LAN
    MA->>MA: 5. entrada.readObject()<br/>(deserialización)
    Note over MA: Unmarshalling:<br/>bytes → Objeto Mensaje
    MA->>MA: 6. switch(mensaje.getTipo())<br/>→ case TEXTO
    MA->>SRV: 7. Servidor.reenviarMensaje(msg)
    SRV->>SRV: 8. synchronized(clientesConectados)<br/>Busca "B" en HashMap
    SRV->>MB: 9. destino = clientesConectados.get("B")<br/>destino.enviarMensaje(msg)
    MB->>SOB: 10. synchronized(salida)<br/>salida.writeObject(msg)<br/>salida.flush() + reset()
    Note over SOB: Marshalling:<br/>Objeto Mensaje → bytes
    SOB-->>HRB: 11. Bytes viajan por TCP/LAN
    HRB->>HRB: 12. entrada.readObject()<br/>(deserialización)
    HRB->>HRB: 13. switch → case TEXTO
    HRB->>UB: 14. SwingUtilities.invokeLater()<br/>areaMensajes.append(<br/>"A: Hola")
    Note over UB: Se muestra en pantalla<br/>de forma thread-safe
```

**Detalles técnicos críticos del flujo:**
- **Paso 3 — `synchronized(salida)` en el Cliente:** Protege el `ObjectOutputStream` porque el hilo EDT (que maneja los clics) podría intentar enviar un mensaje mientras otro evento de envío aún no termina.
- **Paso 3 — `salida.reset()`:** Sin este llamado, `ObjectOutputStream` mantiene un caché interno de referencias. Si se envía un segundo mensaje, Java detectaría que "ya envió un objeto Mensaje antes" y enviaría solo una referencia al anterior, haciendo que el receptor lea datos obsoletos.
- **Paso 8 — `synchronized(clientesConectados)`:** Garantiza exclusión mutua. Si Cliente C se está conectando al mismo tiempo que A envía un mensaje, ambas operaciones no pueden modificar/leer el HashMap simultáneamente.
- **Paso 14 — `SwingUtilities.invokeLater()`:** El `HiloReceptor` NO puede modificar componentes Swing directamente (no es thread-safe). Este método encola la actualización visual para que la ejecute el hilo EDT de Swing.

### 3.2 FUNCIÓN 2: Envío y Recepción de Archivo Multimedia

**Escenario:** El Usuario A selecciona un archivo PDF de 2 MB desde su disco y lo envía al Usuario B.

**Código involucrado:**
- Emisión: `Cliente.enviarArchivo()` (líneas 333-381)
- Recepción en servidor: `ManejadorCliente.run()` → `case Mensaje.ARCHIVO` (líneas 132-143)
- Ruteo: `Servidor.reenviarMensaje()` (líneas 206-232) — mismo método que texto
- Recepción en cliente: `HiloReceptor.run()` → `case Mensaje.ARCHIVO` → `recibirArchivo()` (líneas 486-503)

```mermaid
sequenceDiagram
    participant UA as Cliente A<br/>(Hilo EDT Swing)
    participant FS as FileSystem Local<br/>(Disco de A)
    participant SOA as ObjectOutputStream A
    participant MA as ManejadorCliente A<br/>(Hilo servidor)
    participant SRV as Servidor<br/>(HashMap synchronized)
    participant MB as ManejadorCliente B<br/>(Hilo servidor)
    participant SOB as ObjectOutputStream B
    participant HRB as HiloReceptor B<br/>(Hilo Cliente B)
    participant FSB as FileSystem Local<br/>(Disco de B)
    participant UB as Cliente B<br/>(Hilo EDT Swing)

    UA->>UA: 1. Usuario A selecciona "Usuario B"<br/>en JList y presiona "Archivo"
    UA->>UA: 2. JFileChooser.showOpenDialog()<br/>Selecciona "informe.pdf"
    UA->>UA: 3. Validación: archivo.length()<br/>debe ser menor a 10 MB
    UA->>FS: 4. FileInputStream.read(datosArchivo)<br/>Lee archivo completo como byte[]
    FS-->>UA: 5. byte[2097152] (2 MB en memoria)
    UA->>UA: 6. Crea Mensaje(<br/>emisor="A", receptor="B",<br/>nombreArchivo="informe.pdf",<br/>datosAdjuntos=byte[2MB])<br/>tipo se asigna automáticamente<br/>como ARCHIVO en el constructor
    UA->>SOA: 7. synchronized(salida)<br/>writeObject(msg) + flush() + reset()
    Note over SOA: Marshalling pesado:<br/>Objeto + 2MB de bytes<br/>se serializan juntos
    SOA-->>MA: 8. Bytes viajan por TCP/LAN<br/>(Java fragmenta en paquetes TCP)
    MA->>MA: 9. readObject() deserializa<br/>el Mensaje completo con<br/>los 2 MB de datosAdjuntos
    MA->>MA: 10. switch → case ARCHIVO<br/>Log en consola del servidor
    MA->>SRV: 11. Servidor.reenviarMensaje(msg)
    SRV->>SRV: 12. synchronized(clientesConectados)<br/>Busca "B" en HashMap
    SRV->>MB: 13. destino.enviarMensaje(msg)
    MB->>SOB: 14. synchronized(salida)<br/>writeObject(msg) + flush() + reset()
    SOB-->>HRB: 15. Bytes viajan por TCP/LAN
    HRB->>HRB: 16. readObject() deserializa Mensaje
    HRB->>HRB: 17. switch → case ARCHIVO<br/>Llama a recibirArchivo(msg)
    HRB->>FSB: 18. Crea carpeta "archivos_recibidos/"<br/>si no existe (mkdirs())
    HRB->>FSB: 19. FileOutputStream.write(<br/>msg.getDatosAdjuntos())<br/>Escribe "informe.pdf" en disco
    FSB-->>HRB: 20. Archivo guardado exitosamente
    HRB->>UB: 21. SwingUtilities.invokeLater()<br/>areaMensajes.append(<br/>"A: Archivo recibido: informe.pdf<br/>(2.0 MB) → Guardado en:<br/>archivos_recibidos/informe.pdf")
```

**Detalles técnicos críticos del flujo:**
- **Paso 4 — Lectura completa en memoria:** El archivo se carga entero en un `byte[]`. Esto permite que Java serialice los datos binarios junto con el resto del objeto `Mensaje` de forma atómica (todo o nada).
- **Paso 6 — Constructor especial de Mensaje:** Cuando se usa el constructor `Mensaje(emisor, receptor, nombreArchivo, datosAdjuntos)`, el `tipo` se asigna automáticamente como `ARCHIVO` internamente (línea 91 de Mensaje.java).
- **Paso 8 — Fragmentación TCP:** Aunque el objeto serializado puede pesar megabytes, TCP se encarga automáticamente de fragmentarlo en paquetes de red de tamaño apropiado y reensamblarlos en orden al otro lado. Esto es transparente para el programador.
- **Paso 18 — Creación de directorio:** El cliente receptor crea automáticamente la carpeta `archivos_recibidos/` en el directorio de trabajo si no existe, usando `File.mkdirs()`.

---

## RESUMEN DE CONCEPTOS DE SISTEMAS DISTRIBUIDOS IMPLEMENTADOS

| Concepto | Dónde se Implementa | Cómo |
|---|---|---|
| **Comunicación TCP** | `ServerSocket`, `Socket` | Conexiones persistentes bidireccionales entre cliente y servidor |
| **Marshalling / Serialización** | `ObjectOutputStream.writeObject()`, `ObjectInputStream.readObject()` | Objetos `Mensaje` (que implementan `Serializable`) se convierten automáticamente a bytes para viajar por la red |
| **Transparencia de Ubicación** | `Servidor.reenviarMensaje()` | El emisor indica solo el nombre del destinatario. El servidor resuelve la ubicación buscando en su `HashMap` |
| **Transparencia de Acceso** | `Cliente.enviarMensajeTexto()`, `Cliente.enviarArchivo()` | La interfaz de envío es idéntica sin importar dónde esté el destinatario (localhost, LAN, WAN) |
| **Concurrencia** | `ManejadorCliente implements Runnable`, `HiloReceptor implements Runnable` | Un hilo por conexión en el servidor; un hilo receptor separado en el cliente |
| **Sincronización** | `synchronized(clientesConectados)`, `synchronized(salida)` | Exclusión mutua sobre el HashMap compartido y sobre los streams de escritura |
| **Resiliencia** | Bloques `try-catch-finally` en `ManejadorCliente.run()` | Si un cliente falla, solo su hilo muere. El servidor limpia recursos y notifica a los demás |
| **Prevención de Deadlock** | Orden de inicialización: `ObjectOutputStream` antes de `ObjectInputStream` | Ambos lados envían la cabecera de serialización antes de intentar leerla |
