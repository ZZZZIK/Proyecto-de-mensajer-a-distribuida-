package node;

import common.Mensaje;
import java.util.*;
import java.util.concurrent.*;

/**
 * Gestor de Heartbeats para detección de fallos en el sistema distribuido.
 * 
 * Cada nodo envía un heartbeat (latido) periódico a todos los demás nodos.
 * Si un nodo no responde después de MAX_FALLOS latidos consecutivos,
 * se considera caído y se notifica al NodoServidor para la recuperación.
 * 
 * Requisito §2.4: Detección mediante heartbeats o timeouts.
 */
public class GestorHeartbeat {

    // Configuración
    private static final long INTERVALO_HEARTBEAT_MS = 2000;  // Enviar cada 2 segundos
    private static final int MAX_FALLOS_CONSECUTIVOS = 3;      // 3 fallos = nodo caído (6 seg)

    private final NodoServidor nodo;
    private final int nodoId;
    private final List<InfoNodo> peers;

    // Contadores de fallos consecutivos por peer
    private final Map<Integer, Integer> contadorFallos = new ConcurrentHashMap<>();

    // Scheduler para envío periódico
    private ScheduledExecutorService scheduler;
    private volatile boolean activo = false;

    public GestorHeartbeat(NodoServidor nodo, int nodoId, List<InfoNodo> peers) {
        this.nodo = nodo;
        this.nodoId = nodoId;
        this.peers = peers;
    }

    /**
     * Inicia el envío periódico de heartbeats a todos los peers.
     */
    public void iniciar() {
        activo = true;
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "Heartbeat-Nodo-" + nodoId);
            t.setDaemon(true);
            return t;
        });

        // Inicializar contadores
        for (InfoNodo peer : peers) {
            contadorFallos.put(peer.getId(), 0);
        }

        // Programar envío periódico de heartbeats
        scheduler.scheduleAtFixedRate(this::enviarHeartbeats,
                INTERVALO_HEARTBEAT_MS, INTERVALO_HEARTBEAT_MS, TimeUnit.MILLISECONDS);

        nodo.getLog().registrar(LogDistribuido.HEARTBEAT, null,
                "Gestor de Heartbeats iniciado (intervalo=" + INTERVALO_HEARTBEAT_MS +
                "ms, maxFallos=" + MAX_FALLOS_CONSECUTIVOS + ")");
    }

    /**
     * Envía un heartbeat a todos los peers y verifica sus respuestas.
     */
    private void enviarHeartbeats() {
        if (!activo) return;

        int[] reloj = nodo.getReloj().prepararEnvio();
        Mensaje heartbeat = new Mensaje(Mensaje.HEARTBEAT, nodoId, -1, "PING");
        heartbeat.setRelojVectorial(reloj);

        for (InfoNodo peer : peers) {
            boolean enviado = nodo.getConector().enviarAPeer(peer.getId(), heartbeat);

            if (!enviado) {
                // No se pudo enviar = incrementar contador de fallos
                int fallos = contadorFallos.merge(peer.getId(), 1, Integer::sum);

                if (fallos == MAX_FALLOS_CONSECUTIVOS && peer.isActivo()) {
                    // Nodo detectado como caído
                    nodoDetectadoCaido(peer.getId());
                }
            }
        }
    }

    /**
     * Procesa un heartbeat recibido de otro nodo.
     * Resetea el contador de fallos y responde con ACK.
     */
    public void procesarHeartbeat(int peerId, int[] relojRecibido) {
        // Actualizar reloj vectorial
        nodo.getReloj().recibirMensaje(relojRecibido);

        // Resetear contador de fallos
        contadorFallos.put(peerId, 0);

        // Marcar peer como activo
        for (InfoNodo peer : peers) {
            if (peer.getId() == peerId) {
                boolean estabaInactivo = !peer.isActivo();
                peer.registrarHeartbeat();

                // Si estaba inactivo y ahora responde → reintegración
                if (estabaInactivo) {
                    nodo.getLog().registrarHeartbeat(nodo.getReloj().obtenerActual(),
                            "Nodo-" + peerId + " reintegrado al sistema (respondió heartbeat)");
                    nodo.notificarPeerReconectado(peerId);
                }
                break;
            }
        }

        // Enviar ACK
        int[] relojAck = nodo.getReloj().prepararEnvio();
        Mensaje ack = new Mensaje(Mensaje.HEARTBEAT_ACK, nodoId, peerId, "PONG");
        ack.setRelojVectorial(relojAck);
        nodo.getConector().enviarAPeer(peerId, ack);
    }

    /**
     * Procesa un ACK de heartbeat recibido.
     */
    public void procesarHeartbeatAck(int peerId, int[] relojRecibido) {
        nodo.getReloj().recibirMensaje(relojRecibido);
        contadorFallos.put(peerId, 0);

        for (InfoNodo peer : peers) {
            if (peer.getId() == peerId) {
                peer.registrarHeartbeat();
                break;
            }
        }
    }

    /**
     * Acción cuando se detecta que un nodo cayó.
     * 
     * 1. Marca el nodo como inactivo
     * 2. Registra el evento en el log
     * 3. Si era el coordinador → dispara algoritmo Bully
     * 4. Notifica al NodoServidor para redistribuir carga
     */
    private void nodoDetectadoCaido(int peerId) {
        // Marcar como inactivo
        for (InfoNodo peer : peers) {
            if (peer.getId() == peerId) {
                peer.setActivo(false);
                break;
            }
        }

        int[] reloj = nodo.getReloj().eventoLocal();
        nodo.getLog().registrarFallo(reloj,
                "NODO CAÍDO DETECTADO: Nodo-" + peerId +
                " (" + MAX_FALLOS_CONSECUTIVOS + " heartbeats sin respuesta)");

        // Verificar si el nodo caído era el coordinador
        if (peerId == nodo.getCoordinadorActual()) {
            nodo.getLog().registrarFallo(reloj,
                    "El coordinador (Nodo-" + peerId + ") cayó → Iniciando elección Bully");
            nodo.getBully().iniciarEleccion();
        }

        // Notificar al NodoServidor
        nodo.notificarPeerDesconectado(peerId);
        nodo.enviarNotificacion("[FALLO DETECTADO] El Nodo-" + peerId + " se ha caído.");
    }

    /**
     * Obtiene el número de fallos consecutivos de un peer.
     */
    public int getFallosConsecutivos(int peerId) {
        return contadorFallos.getOrDefault(peerId, 0);
    }

    /**
     * Obtiene la lista de nodos activos (respondiendo a heartbeats).
     */
    public List<Integer> getNodosActivos() {
        List<Integer> activos = new ArrayList<>();
        activos.add(nodoId); // Este nodo siempre está activo
        for (InfoNodo peer : peers) {
            if (peer.isActivo()) {
                activos.add(peer.getId());
            }
        }
        Collections.sort(activos);
        return activos;
    }

    /**
     * Detiene el gestor de heartbeats.
     */
    public void detener() {
        activo = false;
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        nodo.getLog().registrar(LogDistribuido.HEARTBEAT, null,
                "Gestor de Heartbeats detenido");
    }
}
