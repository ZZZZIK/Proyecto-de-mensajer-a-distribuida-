package node;

import common.Mensaje;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Gestiona las conexiones TCP peer-to-peer entre nodos del sistema distribuido.
 * 
 * Cada nodo mantiene un socket TCP persistente con cada otro nodo (mesh completo).
 * Si una conexión se pierde, intenta reconectar automáticamente.
 * Los mensajes inter-nodo se envían y reciben a través de ObjectStreams.
 */
public class ConectorNodos {

    private final NodoServidor nodo;
    private final int nodoId;
    private final List<InfoNodo> peers;  // Nodos remotos conocidos

    // Conexiones activas: nodoId → streams
    private final Map<Integer, ObjectOutputStream> salidas = new ConcurrentHashMap<>();
    private final Map<Integer, Socket> sockets = new ConcurrentHashMap<>();

    // ServerSocket para aceptar conexiones entrantes de otros nodos
    private ServerSocket serverSocketNodos;
    private final int puertoNodos;  // Puerto para comunicación inter-nodo

    private volatile boolean activo = true;
    private final ScheduledExecutorService reconectador = Executors.newSingleThreadScheduledExecutor();

    /**
     * @param nodo        Referencia al nodo servidor principal
     * @param nodoId      ID de este nodo
     * @param puertoNodos Puerto para aceptar conexiones de otros nodos
     * @param peers       Lista de nodos remotos conocidos
     */
    public ConectorNodos(NodoServidor nodo, int nodoId, int puertoNodos, List<InfoNodo> peers) {
        this.nodo = nodo;
        this.nodoId = nodoId;
        this.puertoNodos = puertoNodos;
        this.peers = peers;
    }

    /**
     * Inicia la infraestructura de conexión P2P:
     * 1. Servidor para aceptar conexiones entrantes
     * 2. Conecta activamente a peers con ID menor (evita conexiones duplicadas)
     * 3. Programa reconexiones periódicas
     */
    public void iniciar() {
        // Hilo que acepta conexiones entrantes de otros nodos
        Thread hiloAceptar = new Thread(this::aceptarConexiones, "Aceptador-Nodos-" + nodoId);
        hiloAceptar.setDaemon(true);
        hiloAceptar.start();

        // Esperar un momento para que el servidor inicie
        try { Thread.sleep(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        // Conectar activamente a peers (solo a los de ID menor para evitar duplicados)
        for (InfoNodo peer : peers) {
            if (peer.getId() < nodoId) {
                conectarAPeer(peer);
            }
        }

        // Reconexión periódica cada 5 segundos para peers desconectados
        reconectador.scheduleAtFixedRate(this::intentarReconexiones, 5, 5, TimeUnit.SECONDS);
    }

    /**
     * Acepta conexiones TCP entrantes de otros nodos.
     */
    private void aceptarConexiones() {
        try {
            serverSocketNodos = new ServerSocket(puertoNodos);
            nodo.getLog().registrar(LogDistribuido.NODO, null,
                    "Escuchando conexiones inter-nodo en puerto " + puertoNodos);

            while (activo) {
                Socket socketPeer = serverSocketNodos.accept();
                Thread hiloLector = new Thread(() -> manejarConexionEntrante(socketPeer),
                        "Lector-Peer-" + nodoId);
                hiloLector.setDaemon(true);
                hiloLector.start();
            }
        } catch (IOException e) {
            if (activo) {
                nodo.getLog().registrar(LogDistribuido.FALLO, null,
                        "Error en servidor inter-nodo: " + e.getMessage());
            }
        }
    }

    /**
     * Maneja una conexión entrante de otro nodo.
     * El primer mensaje recibido debe ser NODO_JOIN con el ID del nodo remoto.
     */
    private void manejarConexionEntrante(Socket socket) {
        try {
            ObjectOutputStream salida = new ObjectOutputStream(socket.getOutputStream());
            salida.flush();
            ObjectInputStream entrada = new ObjectInputStream(socket.getInputStream());

            // Leer mensaje de identificación
            Mensaje msgJoin = (Mensaje) entrada.readObject();
            if (!msgJoin.getTipo().equals(Mensaje.NODO_JOIN)) {
                socket.close();
                return;
            }

            int peerId = msgJoin.getNodoOrigen();
            nodo.getLog().registrar(LogDistribuido.NODO, null,
                    "Conexión entrante aceptada de Nodo-" + peerId);

            // Registrar la conexión
            salidas.put(peerId, salida);
            sockets.put(peerId, socket);

            // Marcar peer como activo
            for (InfoNodo peer : peers) {
                if (peer.getId() == peerId) {
                    peer.registrarHeartbeat();
                    break;
                }
            }

            // Enviar nuestra identificación de vuelta
            Mensaje respuesta = new Mensaje(Mensaje.NODO_JOIN, nodoId, peerId, "ACK");
            enviarAPeer(peerId, respuesta);

            // Bucle de lectura de mensajes del peer
            leerMensajesDePeer(peerId, entrada, socket);

        } catch (IOException | ClassNotFoundException e) {
            if (activo) {
                nodo.getLog().registrar(LogDistribuido.FALLO, null,
                        "Error en conexión entrante: " + e.getMessage());
            }
        }
    }

    /**
     * Conecta activamente a un nodo peer.
     */
    private void conectarAPeer(InfoNodo peer) {
        if (salidas.containsKey(peer.getId())) return; // Ya conectado

        try {
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress(peer.getHost(), peer.getPuerto() + 100), 3000);

            ObjectOutputStream salida = new ObjectOutputStream(socket.getOutputStream());
            salida.flush();
            ObjectInputStream entrada = new ObjectInputStream(socket.getInputStream());

            // Enviar identificación
            Mensaje msgJoin = new Mensaje(Mensaje.NODO_JOIN, nodoId, peer.getId(),
                    "CONNECT");
            salida.writeObject(msgJoin);
            salida.flush();
            salida.reset();

            // Registrar conexión
            salidas.put(peer.getId(), salida);
            sockets.put(peer.getId(), socket);
            peer.registrarHeartbeat();

            nodo.getLog().registrar(LogDistribuido.NODO, null,
                    "Conectado exitosamente a Nodo-" + peer.getId() +
                    " (" + peer.getDireccion() + ")");

            // Iniciar hilo de lectura
            Thread hiloLector = new Thread(() -> leerMensajesDePeer(peer.getId(), entrada, socket),
                    "Lector-Peer-" + peer.getId());
            hiloLector.setDaemon(true);
            hiloLector.start();

        } catch (IOException e) {
            nodo.getLog().registrar(LogDistribuido.NODO, null,
                    "No se pudo conectar a Nodo-" + peer.getId() +
                    " (" + peer.getDireccion() + "): " + e.getMessage());
        }
    }

    /**
     * Lee mensajes continuamente de un peer y los despacha al NodoServidor.
     */
    private void leerMensajesDePeer(int peerId, ObjectInputStream entrada, Socket socket) {
        try {
            while (activo) {
                Mensaje mensaje = (Mensaje) entrada.readObject();
                nodo.procesarMensajeInterNodo(mensaje, peerId);
            }
        } catch (IOException | ClassNotFoundException e) {
            if (activo) {
                nodo.getLog().registrar(LogDistribuido.FALLO, null,
                        "Conexión perdida con Nodo-" + peerId + ": " + e.getMessage());
                desconectarPeer(peerId);
                nodo.notificarPeerDesconectado(peerId);
            }
        }
    }

    /**
     * Envía un mensaje a un peer específico.
     * 
     * @param peerId  ID del nodo destino
     * @param mensaje Mensaje a enviar
     * @return true si se envió exitosamente
     */
    public boolean enviarAPeer(int peerId, Mensaje mensaje) {
        ObjectOutputStream salida = salidas.get(peerId);
        if (salida == null) return false;

        try {
            synchronized (salida) {
                salida.writeObject(mensaje);
                salida.flush();
                salida.reset();
            }
            return true;
        } catch (IOException e) {
            nodo.getLog().registrar(LogDistribuido.FALLO, null,
                    "Error enviando a Nodo-" + peerId + ": " + e.getMessage());
            desconectarPeer(peerId);
            return false;
        }
    }

    /**
     * Envía un mensaje a TODOS los peers conectados (broadcast).
     * 
     * @param mensaje Mensaje a enviar
     * @return Número de peers a los que se envió exitosamente
     */
    public int broadcast(Mensaje mensaje) {
        int exitosos = 0;
        for (InfoNodo peer : peers) {
            if (enviarAPeer(peer.getId(), mensaje)) {
                exitosos++;
            }
        }
        return exitosos;
    }

    /**
     * Envía un mensaje a todos los peers con ID mayor que el dado.
     * Usado por el algoritmo Bully.
     * 
     * @param mensaje Mensaje a enviar
     * @param idMinimo Solo enviar a nodos con ID > idMinimo
     * @return Número de envíos exitosos
     */
    public int enviarAMayores(Mensaje mensaje, int idMinimo) {
        int exitosos = 0;
        for (InfoNodo peer : peers) {
            if (peer.getId() > idMinimo && enviarAPeer(peer.getId(), mensaje)) {
                exitosos++;
            }
        }
        return exitosos;
    }

    /**
     * Desconecta un peer (limpia sockets y streams).
     */
    private void desconectarPeer(int peerId) {
        salidas.remove(peerId);
        Socket socket = sockets.remove(peerId);
        if (socket != null) {
            try { socket.close(); } catch (IOException e) { /* ignorar */ }
        }
        for (InfoNodo peer : peers) {
            if (peer.getId() == peerId) {
                peer.setActivo(false);
                break;
            }
        }
    }

    /**
     * Intenta reconectar con peers que se han desconectado.
     */
    private void intentarReconexiones() {
        if (!activo) return;
        for (InfoNodo peer : peers) {
            if (!salidas.containsKey(peer.getId())) {
                conectarAPeer(peer);
            }
        }
    }

    /**
     * Verifica si un peer está conectado.
     */
    public boolean estaConectado(int peerId) {
        return salidas.containsKey(peerId);
    }

    /**
     * Obtiene la lista de IDs de peers actualmente conectados.
     */
    public List<Integer> getPeersConectados() {
        return new ArrayList<>(salidas.keySet());
    }

    /**
     * Obtiene el número de peers conectados.
     */
    public int getNumPeersConectados() {
        return salidas.size();
    }

    /**
     * Detiene todas las conexiones y libera recursos.
     */
    public void detener() {
        activo = false;
        reconectador.shutdownNow();

        // Enviar NODO_LEAVE a todos los peers
        Mensaje msgLeave = new Mensaje(Mensaje.NODO_LEAVE, nodoId, -1, "SHUTDOWN");
        broadcast(msgLeave);

        // Cerrar todas las conexiones
        for (Map.Entry<Integer, Socket> entry : sockets.entrySet()) {
            try { entry.getValue().close(); } catch (IOException e) { /* ignorar */ }
        }
        salidas.clear();
        sockets.clear();

        // Cerrar server socket
        if (serverSocketNodos != null && !serverSocketNodos.isClosed()) {
            try { serverSocketNodos.close(); } catch (IOException e) { /* ignorar */ }
        }
    }
}
