package node;

import common.Mensaje;
import server.ManejadorCliente;
import server.GestorBD;
import server.Servidor;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Proceso principal de cada nodo en el sistema distribuido.
 * 
 * Cada NodoServidor es un proceso Java independiente que:
 * 1. Acepta conexiones de clientes (como el servidor original)
 * 2. Se comunica peer-to-peer con otros nodos via TCP (mesh completo)
 * 3. Participa en protocolos distribuidos:
 *    - Relojes vectoriales (§2.2)
 *    - Exclusión mutua Ricart-Agrawala (§2.3)
 *    - Elección de coordinador Bully (§2.3)
 *    - Consenso Raft simplificado (§2.3, puntaje extra)
 *    - Heartbeats para detección de fallos (§2.4)
 * 4. Tiene su propia capa de servicio de chat
 * 
 * Configuración via archivo .properties:
 *   nodo.id=0
 *   nodo.puerto=5001
 *   nodo.puerto.nodos=5101
 *   nodo.peers=localhost:5002:5102,localhost:5003:5103
 *   nodo.total=3
 */
public class NodoServidor {

    // Configuración del nodo
    private final int nodoId;
    private final int puertoClientes;    // Puerto para clientes
    private final int puertoNodos;       // Puerto para comunicación inter-nodo
    private final int totalNodos;

    // Sub-componentes distribuidos
    private final RelojVectorial reloj;
    private final LogDistribuido log;
    private final ConectorNodos conector;
    private final GestorHeartbeat heartbeat;
    private final AlgoritmoBully bully;
    private final MutexRicartAgrawala mutex;
    private final ConsensoRaft consenso;

    // Estado del nodo
    private final List<InfoNodo> peers;
    private volatile int coordinadorActual = -1;
    private volatile boolean activo = true;

    // Clientes conectados a ESTE nodo
    private final Map<String, ManejadorCliente> clientesLocales = new ConcurrentHashMap<>();

    /**
     * Crea un NodoServidor a partir de un archivo de configuración.
     */
    public NodoServidor(Properties config) {
        this.nodoId = Integer.parseInt(config.getProperty("nodo.id", "0"));
        this.puertoClientes = Integer.parseInt(config.getProperty("nodo.puerto", "5001"));
        this.puertoNodos = Integer.parseInt(config.getProperty("nodo.puerto.nodos",
                String.valueOf(puertoClientes + 100)));
        this.totalNodos = Integer.parseInt(config.getProperty("nodo.total", "3"));

        // Configurar base de datos si se especifica en propiedades
        String dbHost = config.getProperty("db.host");
        String dbUser = config.getProperty("db.user");
        String dbPass = config.getProperty("db.password");
        if (dbHost != null) {
            GestorBD.configurar(dbHost, dbUser != null ? dbUser : "root", dbPass != null ? dbPass : "");
        }

        // Parsear lista de peers
        this.peers = parsearPeers(config.getProperty("nodo.peers", ""));

        // Inicializar sub-componentes
        this.reloj = new RelojVectorial(nodoId, totalNodos);
        this.log = new LogDistribuido(nodoId, "logs");
        this.conector = new ConectorNodos(this, nodoId, puertoNodos, peers);
        this.heartbeat = new GestorHeartbeat(this, nodoId, peers);
        this.bully = new AlgoritmoBully(this, nodoId, peers);
        this.mutex = new MutexRicartAgrawala(this, nodoId, peers);
        this.consenso = new ConsensoRaft(this, nodoId, peers, totalNodos);
    }

    /**
     * Parsea la lista de peers desde la configuración.
     * Formato: "host1:puerto1:puertoNodos1,host2:puerto2:puertoNodos2"
     */
    private List<InfoNodo> parsearPeers(String peersStr) {
        List<InfoNodo> resultado = new ArrayList<>();
        if (peersStr == null || peersStr.isEmpty()) return resultado;

        String[] peerArray = peersStr.split(",");
        int peerId = 0;
        for (String peerDef : peerArray) {
            String[] partes = peerDef.trim().split(":");
            if (partes.length >= 2) {
                String host = partes[0];
                int puerto = Integer.parseInt(partes[1]);
                // Calcular ID basado en la posición, saltando nuestro propio ID
                if (peerId == nodoId) peerId++;
                resultado.add(new InfoNodo(peerId, host, puerto));
                peerId++;
            }
        }
        return resultado;
    }

    /**
     * Inicia el nodo: conecta con peers, inicia heartbeats, elige coordinador,
     * y comienza a aceptar clientes.
     */
    public void iniciar() {
        log.registrar(LogDistribuido.SISTEMA, null,
                "════════════════════════════════════════════════");
        log.registrar(LogDistribuido.SISTEMA, null,
                "  NODO-" + nodoId + " — WhatsApp Distribuido Multinodo");
        log.registrar(LogDistribuido.SISTEMA, null,
                "  Puerto clientes: " + puertoClientes);
        log.registrar(LogDistribuido.SISTEMA, null,
                "  Puerto inter-nodo: " + puertoNodos);
        log.registrar(LogDistribuido.SISTEMA, null,
                "  Peers configurados: " + peers.size());
        log.registrar(LogDistribuido.SISTEMA, null,
                "════════════════════════════════════════════════");

        // 1. Verificar BD
        if (!GestorBD.verificarConexion()) {
            log.registrar(LogDistribuido.FALLO, null,
                    "No se pudo conectar a MySQL. Abortando.");
            return;
        }

        // 2. Iniciar conector P2P
        conector.iniciar();

        // 3. Esperar a que al menos un peer se conecte (o timeout de 10 seg)
        log.registrar(LogDistribuido.NODO, null,
                "Esperando conexión con peers (máximo 10 segundos)...");
        long inicio = System.currentTimeMillis();
        while (conector.getNumPeersConectados() == 0 &&
               System.currentTimeMillis() - inicio < 10000) {
            try { Thread.sleep(500); } catch (InterruptedException e) { break; }
        }

        log.registrar(LogDistribuido.NODO, null,
                "Peers conectados: " + conector.getNumPeersConectados() + "/" + peers.size());

        // 4. Iniciar heartbeats
        heartbeat.iniciar();

        // 5. Determinar coordinador inicial
        try { Thread.sleep(2000); } catch (InterruptedException e) { /* ignore */ }
        bully.determinarCoordinadorInicial();

        // 6. Broadcast periódico de métricas de coordinación
        iniciarBroadcastMetricas();

        // 7. Aceptar clientes
        iniciarServidorClientes();
    }

    /**
     * Inicia el ServerSocket para aceptar conexiones de clientes.
     */
    private void iniciarServidorClientes() {
        try {
            ServerSocket serverSocket = new ServerSocket(puertoClientes);
            log.registrar(LogDistribuido.SISTEMA, null,
                    "Aceptando clientes en puerto " + puertoClientes);

            while (activo) {
                Socket socketCliente = serverSocket.accept();
                log.registrar(LogDistribuido.NODO, reloj.eventoLocal(),
                        "Nueva conexión de cliente desde " +
                        socketCliente.getInetAddress().getHostAddress());

                ManejadorCliente manejador = new ManejadorCliente(socketCliente, this);
                Thread hiloCliente = new Thread(manejador);
                hiloCliente.setDaemon(true);
                hiloCliente.start();
            }

        } catch (IOException e) {
            if (activo) {
                log.registrar(LogDistribuido.FALLO, null,
                        "Error en servidor de clientes: " + e.getMessage());
            }
        }
    }

    /**
     * Inicia un broadcast periódico de métricas de coordinación a los clientes locales.
     * Permite al GeneradorCarga mostrar conteos de mensajes de mutex, elección y consenso.
     */
    private void iniciarBroadcastMetricas() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "Metricas-Nodo-" + nodoId);
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(() -> {
            if (!activo || clientesLocales.isEmpty()) return;
            long msgsMutex = mutex.getTotalMensajesMutex();
            long msgsConsenso = consenso.getTotalMensajesConsenso();
            long msgsCoord = log.getTotalMensajesCoordinacion();
            long elecciones = log.getContadorElecciones();
            String metricas = "[METRICAS_COORD] mutex=" + msgsMutex +
                ",consenso=" + msgsConsenso + ",elecciones=" + elecciones + ",total=" + msgsCoord;
            Mensaje notif = new Mensaje("SERVIDOR", "TODOS", Mensaje.NOTIFICACION, metricas);
            for (ManejadorCliente cliente : clientesLocales.values()) {
                cliente.enviarMensaje(notif);
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    // ═══════════════════════════════════════════════════
    //  PROCESAMIENTO DE MENSAJES INTER-NODO
    // ═══════════════════════════════════════════════════

    /**
     * Despacha un mensaje recibido de otro nodo al componente apropiado.
     */
    public void procesarMensajeInterNodo(Mensaje mensaje, int peerId) {
        switch (mensaje.getTipo()) {
            // === HEARTBEATS ===
            case Mensaje.HEARTBEAT:
                heartbeat.procesarHeartbeat(peerId, mensaje.getRelojVectorial());
                break;
            case Mensaje.HEARTBEAT_ACK:
                heartbeat.procesarHeartbeatAck(peerId, mensaje.getRelojVectorial());
                break;

            // === ALGORITMO BULLY ===
            case Mensaje.ELECCION:
                bully.procesarEleccion(peerId, mensaje.getRelojVectorial());
                break;
            case Mensaje.ELECCION_OK:
                bully.procesarOk(peerId, mensaje.getRelojVectorial());
                break;
            case Mensaje.COORDINADOR:
                bully.procesarCoordinador(
                        Integer.parseInt(mensaje.getContenido()),
                        mensaje.getRelojVectorial());
                break;

            // === RICART-AGRAWALA ===
            case Mensaje.MUTEX_REQUEST:
                mutex.procesarSolicitud(peerId, mensaje.getRelojVectorial());
                break;
            case Mensaje.MUTEX_REPLY:
                mutex.procesarRespuesta(peerId, mensaje.getRelojVectorial());
                break;

            // === RAFT ===
            case Mensaje.RAFT_APPEND:
                consenso.procesarAppendEntries(peerId, mensaje.getContenido(),
                        mensaje.getRelojVectorial(), mensaje.getTermRaft(),
                        mensaje.getSecuenciaGlobal());
                break;
            case Mensaje.RAFT_APPEND_ACK:
                consenso.procesarAppendAck(peerId,
                        mensaje.getSecuenciaGlobal(),
                        mensaje.getRelojVectorial());
                break;

            // === DESCUBRIMIENTO ===
            case Mensaje.NODO_JOIN:
                log.registrar(LogDistribuido.NODO, reloj.recibirMensaje(mensaje.getRelojVectorial()),
                        "Nodo-" + peerId + " se unió al sistema");
                break;
            case Mensaje.NODO_LEAVE:
                log.registrar(LogDistribuido.NODO, reloj.recibirMensaje(mensaje.getRelojVectorial()),
                        "Nodo-" + peerId + " abandonó el sistema");
                notificarPeerDesconectado(peerId);
                break;

            // === REENVÍO DE MENSAJES DE USUARIO ===
            case Mensaje.REENVIO_TEXTO:
            case Mensaje.REENVIO_ARCHIVO:
                procesarReenvioInterNodo(mensaje);
                break;

            default:
                log.registrar(LogDistribuido.SISTEMA, null,
                        "Mensaje inter-nodo desconocido: " + mensaje.getTipo());
                break;
        }
    }

    // ═══════════════════════════════════════════════════
    //  GESTIÓN DE CLIENTES Y MENSAJES
    // ═══════════════════════════════════════════════════

    /**
     * Registra un cliente que se conectó a este nodo.
     */
    public boolean registrarClienteLocal(String nombre, ManejadorCliente manejador) {
        clientesLocales.put(nombre, manejador);

        int[] relojEvento = reloj.eventoLocal();
        log.registrarMensaje(relojEvento, nombre, "SISTEMA", "CONECTADO",
                "Usuario conectado en Nodo-" + nodoId);

        // Proponer al consenso Raft
        consenso.proponer("CONECTADO", nombre, nodoId);

        return true;
    }

    /**
     * Remueve un cliente de este nodo.
     */
    public void removerClienteLocal(String nombre) {
        clientesLocales.remove(nombre);

        int[] relojEvento = reloj.eventoLocal();
        log.registrarMensaje(relojEvento, nombre, "SISTEMA", "DESCONECTADO",
                "Usuario desconectado de Nodo-" + nodoId);

        // Proponer al consenso Raft
        consenso.proponer("DESCONECTADO", nombre, nodoId);
    }

    /**
     * Procesa un mensaje de usuario (texto o archivo) que necesita ser enrutado.
     * 
     * Si el destinatario está en este nodo → entrega directa.
     * Si está en otro nodo → reenvía al nodo correspondiente.
     * Si no está conectado → guarda en buzón offline.
     */
    public void procesarMensajeUsuario(Mensaje mensaje) {
        // Stampar con reloj vectorial
        int[] relojMsg = reloj.prepararEnvio();
        mensaje.setRelojVectorial(relojMsg);
        mensaje.setNodoOrigen(nodoId);

        log.registrarMensaje(relojMsg, mensaje.getEmisor(), mensaje.getReceptor(),
                mensaje.getTipo(), mensaje.getContenido());

        String receptor = mensaje.getReceptor();

        // 1. Verificar si el destinatario está en ESTE nodo
        ManejadorCliente destinoLocal = clientesLocales.get(receptor);
        if (destinoLocal != null) {
            destinoLocal.enviarMensaje(mensaje);

            // Guardar en historial con exclusión mutua
            guardarConMutex(mensaje);
            return;
        }

        // 2. Verificar en qué nodo está el usuario (via consenso Raft)
        Map<String, Integer> usuariosGlobales = consenso.getUsuariosGlobales();
        Integer nodoDestino = usuariosGlobales.get(receptor);

        if (nodoDestino != null && nodoDestino != nodoId) {
            // Reenviar al nodo correspondiente
            String tipoReenvio = mensaje.getTipo().equals(Mensaje.TEXTO) ?
                    Mensaje.REENVIO_TEXTO : Mensaje.REENVIO_ARCHIVO;
            mensaje.setTipo(tipoReenvio);
            mensaje.setNodoDestino(nodoDestino);

            conector.enviarAPeer(nodoDestino, mensaje);
            log.registrar(LogDistribuido.MENSAJE, relojMsg,
                    "Mensaje reenviado a Nodo-" + nodoDestino + " para " + receptor);

            // Guardar en historial
            guardarConMutex(mensaje);
            return;
        }

        // 3. Usuario no conectado → guardar offline
        if (GestorBD.usuarioExiste(receptor)) {
            GestorBD.guardarMensajeOffline(mensaje);
            log.registrar(LogDistribuido.MENSAJE, relojMsg,
                    "Mensaje guardado offline para " + receptor);

            // Notificar al emisor
            ManejadorCliente emisor = clientesLocales.get(mensaje.getEmisor());
            if (emisor != null) {
                Mensaje aviso = new Mensaje("SERVIDOR", mensaje.getEmisor(),
                        Mensaje.NOTIFICACION,
                        "📩 '" + receptor + "' está desconectado. Tu mensaje se ha guardado.");
                emisor.enviarMensaje(aviso);
            }
        } else {
            // Usuario no existe
            ManejadorCliente emisor = clientesLocales.get(mensaje.getEmisor());
            if (emisor != null) {
                Mensaje error = new Mensaje("SERVIDOR", mensaje.getEmisor(),
                        Mensaje.NOTIFICACION,
                        "❌ El usuario '" + receptor + "' no existe en el chat.");
                emisor.enviarMensaje(error);
            }
        }
    }

    /**
     * Procesa un mensaje reenviado desde otro nodo.
     * Busca el destinatario entre los clientes locales y se lo entrega.
     */
    private void procesarReenvioInterNodo(Mensaje mensaje) {
        reloj.recibirMensaje(mensaje.getRelojVectorial());

        // Restaurar tipo original
        if (mensaje.getTipo().equals(Mensaje.REENVIO_TEXTO)) {
            mensaje.setTipo(Mensaje.TEXTO);
        } else {
            mensaje.setTipo(Mensaje.ARCHIVO);
        }

        String receptor = mensaje.getReceptor();
        ManejadorCliente destino = clientesLocales.get(receptor);

        if (destino != null) {
            destino.enviarMensaje(mensaje);
            log.registrar(LogDistribuido.MENSAJE, reloj.obtenerActual(),
                    "Mensaje inter-nodo entregado a " + receptor);
        } else {
            // El usuario se desconectó mientras el mensaje viajaba → guardar offline
            GestorBD.guardarMensajeOffline(mensaje);
            log.registrar(LogDistribuido.MENSAJE, reloj.obtenerActual(),
                    "Mensaje inter-nodo guardado offline para " + receptor);
        }
    }

    /**
     * Guarda un mensaje en el historial usando exclusión mutua distribuida.
     */
    private void guardarConMutex(Mensaje mensaje) {
        // Ejecutar en hilo separado para no bloquear el procesamiento
        Thread hiloMutex = new Thread(() -> {
            try {
                mutex.solicitarAcceso();
                GestorBD.guardarMensajeHistorial(mensaje);
            } finally {
                mutex.liberarAcceso();
            }
        }, "Mutex-Escritura");
        hiloMutex.setDaemon(true);
        hiloMutex.start();
    }

    // ═══════════════════════════════════════════════════
    //  NOTIFICACIONES DE ESTADO
    // ═══════════════════════════════════════════════════

    /**
     * Notificado cuando un peer se desconecta.
     */
    public void notificarPeerDesconectado(int peerId) {
        log.registrarFallo(reloj.eventoLocal(),
                "Peer desconectado: Nodo-" + peerId);
        
        // Limpiar usuarios que estaban conectados al nodo caído
        consenso.removerUsuariosDeNodo(peerId);
        
        // Actualizar listas de usuarios de los clientes locales
        enviarListasActualizadas();
    }

    /**
     * Notificado cuando un peer se reconecta.
     */
    public void notificarPeerReconectado(int peerId) {
        log.registrar(LogDistribuido.NODO, reloj.eventoLocal(),
                "Peer reconectado: Nodo-" + peerId);
        enviarListasActualizadas();
    }

    /**
     * Envía las listas actualizadas de usuarios a todos los clientes locales.
     */
    public void enviarListaUsuarios() {
        enviarListasActualizadas();
    }

    private void enviarListasActualizadas() {
        // Construir lista de usuarios online (locales + globales del consenso)
        Set<String> todosOnline = new HashSet<>(clientesLocales.keySet());
        todosOnline.addAll(consenso.getUsuariosGlobales().keySet());

        String listaOnline = String.join(",", todosOnline);
        Mensaje msgLista = new Mensaje("SERVIDOR", "TODOS",
                Mensaje.LISTA_USUARIOS, listaOnline);

        for (ManejadorCliente cliente : clientesLocales.values()) {
            cliente.enviarMensaje(msgLista);
        }

        // Lista offline
        List<String> todosRegistrados = GestorBD.obtenerUsuariosRegistrados();
        List<String> offline = new ArrayList<>();
        for (String reg : todosRegistrados) {
            if (!todosOnline.contains(reg)) {
                offline.add(reg);
            }
        }

        String listaOffline = String.join(",", offline);
        Mensaje msgOffline = new Mensaje("SERVIDOR", "TODOS",
                Mensaje.LISTA_OFFLINE, listaOffline);

        for (ManejadorCliente cliente : clientesLocales.values()) {
            cliente.enviarMensaje(msgOffline);
        }
    }

    /**
     * Envía una notificación a todos los clientes locales.
     */
    public void enviarNotificacion(String texto) {
        Mensaje notif = new Mensaje("SERVIDOR", "TODOS",
                Mensaje.NOTIFICACION, texto);
        for (ManejadorCliente cliente : clientesLocales.values()) {
            cliente.enviarMensaje(notif);
        }
    }

    // ═══════════════════════════════════════════════════
    //  GETTERS
    // ═══════════════════════════════════════════════════

    public int getNodoId()                      { return nodoId; }
    public RelojVectorial getReloj()            { return reloj; }
    public LogDistribuido getLog()              { return log; }
    public ConectorNodos getConector()          { return conector; }
    public GestorHeartbeat getHeartbeat()       { return heartbeat; }
    public AlgoritmoBully getBully()            { return bully; }
    public MutexRicartAgrawala getMutex()       { return mutex; }
    public ConsensoRaft getConsenso()           { return consenso; }
    public int getCoordinadorActual()           { return coordinadorActual; }
    public Map<String, ManejadorCliente> getClientesLocales() { return clientesLocales; }
    public int getTotalNodos()                  { return totalNodos; }
    public List<InfoNodo> getPeers()            { return peers; }

    public void setCoordinadorActual(int id) {
        this.coordinadorActual = id;
    }

    // ═══════════════════════════════════════════════════
    //  PUNTO DE ENTRADA
    // ═══════════════════════════════════════════════════

    /**
     * Punto de entrada principal para ejecutar un nodo.
     * Uso: java node.NodoServidor config/nodo1.properties
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Uso: java node.NodoServidor <archivo_config>");
            System.out.println("Ejemplo: java node.NodoServidor config/nodo1.properties");
            return;
        }

        Properties config = new Properties();
        try (FileInputStream fis = new FileInputStream(args[0])) {
            config.load(fis);
        } catch (IOException e) {
            System.err.println("Error leyendo configuración: " + e.getMessage());
            return;
        }

        NodoServidor nodo = new NodoServidor(config);

        // Shutdown hook para cierre limpio
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            nodo.log.registrar(LogDistribuido.SISTEMA, null, "Cerrando nodo...");
            nodo.activo = false;
            nodo.conector.detener();
            nodo.heartbeat.detener();
            nodo.log.cerrar();
        }));

        nodo.iniciar();
    }
}
