package node;

import common.Mensaje;
import java.util.*;
import java.util.concurrent.*;

/**
 * Implementación simplificada del protocolo de consenso Raft.
 * 
 * Replica la lista de usuarios conectados globalmente entre todos los nodos,
 * garantizando que todos tengan la misma vista consistente del sistema.
 * 
 * Simplificaciones respecto a Raft completo:
 * - El líder es elegido por el algoritmo Bully (no por el propio Raft)
 * - No se implementa log compaction
 * - El log solo contiene entradas de membresía (USUARIO_CONECTADO/DESCONECTADO)
 * 
 * Flujo:
 * 1. Cuando un usuario se conecta/desconecta, el nodo lo propone al líder
 * 2. El líder crea una entrada en el log y envía RAFT_APPEND a seguidores
 * 3. Cuando hay quórum (mayoría: 2/3), la entrada se commitea
 * 4. La entrada commiteada se aplica al estado (lista de usuarios global)
 * 
 * Requisito §2.3: Consenso simplificado (opcional, con puntaje extra).
 */
public class ConsensoRaft {

    /**
     * Entrada en el log replicado de Raft.
     */
    public static class EntradaLog implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public final int term;           // Término en que se creó
        public final long indice;        // Índice en el log
        public final String operacion;   // "CONECTADO" o "DESCONECTADO"
        public final String usuario;     // Nombre del usuario
        public final int nodoUsuario;    // Nodo donde está el usuario
        public final long timestamp;     // Cuándo ocurrió

        public EntradaLog(int term, long indice, String operacion,
                          String usuario, int nodoUsuario) {
            this.term = term;
            this.indice = indice;
            this.operacion = operacion;
            this.usuario = usuario;
            this.nodoUsuario = nodoUsuario;
            this.timestamp = System.currentTimeMillis();
        }

        @Override
        public String toString() {
            return "[T" + term + ":I" + indice + "] " + operacion + " " +
                   usuario + "@Nodo-" + nodoUsuario;
        }
    }

    private final NodoServidor nodo;
    private final int nodoId;
    private final List<InfoNodo> peers;
    private final int totalNodos;

    // Estado de Raft
    private volatile int termActual = 0;          // Término actual (época)
    private final List<EntradaLog> log = new CopyOnWriteArrayList<>();
    private volatile long commitIndex = -1;        // Último índice commiteado
    private volatile long lastApplied = -1;        // Último índice aplicado al estado

    // Estado replicado: usuario → nodoId donde está conectado
    private final Map<String, Integer> usuariosGlobales = new ConcurrentHashMap<>();

    // Conteo de ACKs para entradas pendientes: indice → set de nodos que confirmaron
    private final Map<Long, Set<Integer>> acksRecibidos = new ConcurrentHashMap<>();

    // Métricas
    private volatile long totalEntradas = 0;
    private volatile long totalMensajesConsenso = 0;

    public ConsensoRaft(NodoServidor nodo, int nodoId, List<InfoNodo> peers, int totalNodos) {
        this.nodo = nodo;
        this.nodoId = nodoId;
        this.peers = peers;
        this.totalNodos = totalNodos;
    }

    /**
     * Propone una nueva entrada al log (solo el líder puede proponer).
     * Si este nodo no es el líder, reenvía la propuesta al líder.
     * 
     * @param operacion "CONECTADO" o "DESCONECTADO"
     * @param usuario   Nombre del usuario
     * @param nodoUsuario Nodo donde está el usuario
     */
    public void proponer(String operacion, String usuario, int nodoUsuario) {
        if (nodoId == nodo.getCoordinadorActual()) {
            // Soy el líder → crear entrada y replicar
            long indice = log.size();
            EntradaLog entrada = new EntradaLog(termActual, indice, operacion,
                    usuario, nodoUsuario);
            log.add(entrada);
            totalEntradas++;

            nodo.getLog().registrarConsenso(nodo.getReloj().obtenerActual(),
                    "PROPUESTA_LIDER",
                    "Nueva entrada: " + entrada);

            // Registrar mi propio ACK
            Set<Integer> acks = ConcurrentHashMap.newKeySet();
            acks.add(nodoId);
            acksRecibidos.put(indice, acks);

            // Enviar RAFT_APPEND a todos los seguidores
            replicarEntrada(entrada);

        } else {
            // No soy líder → reenviar al líder
            int liderId = nodo.getCoordinadorActual();
            if (liderId >= 0) {
                Mensaje msgPropuesta = new Mensaje(Mensaje.RAFT_APPEND, nodoId, liderId,
                        operacion + ":" + usuario + ":" + nodoUsuario);
                msgPropuesta.setRelojVectorial(nodo.getReloj().prepararEnvio());
                msgPropuesta.setTermRaft(termActual);
                nodo.getConector().enviarAPeer(liderId, msgPropuesta);
                totalMensajesConsenso++;

                nodo.getLog().registrarConsenso(nodo.getReloj().obtenerActual(),
                        "PROPUESTA_REENVIADA",
                        "Reenviada a líder Nodo-" + liderId + ": " + operacion + " " + usuario);
            }
        }
    }

    /**
     * Replica una entrada del log a todos los seguidores.
     * Solo llamado por el líder.
     */
    private void replicarEntrada(EntradaLog entrada) {
        int[] reloj = nodo.getReloj().prepararEnvio();

        // Serializar entrada como contenido del mensaje
        String contenido = entrada.term + ":" + entrada.indice + ":" +
                entrada.operacion + ":" + entrada.usuario + ":" + entrada.nodoUsuario;

        Mensaje msgAppend = new Mensaje(Mensaje.RAFT_APPEND, nodoId, -1, contenido);
        msgAppend.setRelojVectorial(reloj);
        msgAppend.setTermRaft(termActual);
        msgAppend.setSecuenciaGlobal(entrada.indice);

        for (InfoNodo peer : peers) {
            if (peer.isActivo()) {
                nodo.getConector().enviarAPeer(peer.getId(), msgAppend);
                totalMensajesConsenso++;
            }
        }

        nodo.getLog().registrarConsenso(reloj,
                "APPEND_ENVIADO",
                "Entrada " + entrada + " enviada a seguidores");

        // Verificar quórum inmediato (si solo somos 1 nodo activo)
        verificarQuorum(entrada.indice);
    }

    /**
     * Procesa un RAFT_APPEND recibido de otro nodo.
     * Si soy seguidor, agrego la entrada a mi log y respondo ACK.
     * Si soy líder y recibo una propuesta, la proceso.
     */
    public void procesarAppendEntries(int nodoEmisor, String contenido, 
                                       int[] relojRecibido, int term, long secuencia) {
        nodo.getReloj().recibirMensaje(relojRecibido);
        totalMensajesConsenso++;

        String[] partes = contenido.split(":");
        if (nodoId == nodo.getCoordinadorActual() && partes.length == 3) {
            // Soy líder y recibí una propuesta reenviada
            proponer(partes[0], partes[1], Integer.parseInt(partes[2]));
            return;
        }

        // Soy seguidor → procesar la entrada
        partes = contenido.split(":");
        if (partes.length >= 5) {
            int entryTerm = Integer.parseInt(partes[0]);
            long entryIndex = Long.parseLong(partes[1]);
            String operacion = partes[2];
            String usuario = partes[3];
            int nodoUsuario = Integer.parseInt(partes[4]);

            // Actualizar término si es mayor
            if (entryTerm > termActual) {
                termActual = entryTerm;
            }

            // Agregar entrada al log local si no existe
            EntradaLog entrada = new EntradaLog(entryTerm, entryIndex, operacion,
                    usuario, nodoUsuario);

            while (log.size() <= entryIndex) {
                log.add(null); // Rellenar huecos
            }
            log.set((int) entryIndex, entrada);

            nodo.getLog().registrarConsenso(nodo.getReloj().obtenerActual(),
                    "APPEND_RECIBIDO",
                    "Entrada recibida del líder: " + entrada);

            // Enviar ACK al líder
            Mensaje ack = new Mensaje(Mensaje.RAFT_APPEND_ACK, nodoId, nodoEmisor,
                    String.valueOf(entryIndex));
            ack.setRelojVectorial(nodo.getReloj().prepararEnvio());
            ack.setTermRaft(termActual);
            ack.setSecuenciaGlobal(entryIndex);
            nodo.getConector().enviarAPeer(nodoEmisor, ack);
            totalMensajesConsenso++;

            // Aplicar la entrada al estado local
            aplicarEntrada(entrada);
        }
    }

    /**
     * Procesa un ACK de RAFT_APPEND.
     * Verifica si se alcanzó quórum para commitear la entrada.
     */
    public void procesarAppendAck(int nodoEmisor, long indice, int[] relojRecibido) {
        nodo.getReloj().recibirMensaje(relojRecibido);
        totalMensajesConsenso++;

        Set<Integer> acks = acksRecibidos.computeIfAbsent(indice,
                k -> ConcurrentHashMap.newKeySet());
        acks.add(nodoEmisor);

        nodo.getLog().registrarConsenso(nodo.getReloj().obtenerActual(),
                "ACK_RECIBIDO",
                "ACK de Nodo-" + nodoEmisor + " para entrada " + indice +
                " (total ACKs: " + acks.size() + "/" + totalNodos + ")");

        verificarQuorum(indice);
    }

    /**
     * Verifica si una entrada tiene quórum (mayoría) de ACKs.
     * Si lo tiene, commitea la entrada y aplica al estado.
     */
    private void verificarQuorum(long indice) {
        Set<Integer> acks = acksRecibidos.get(indice);
        if (acks == null) return;

        int quorum = (totalNodos / 2) + 1; // Mayoría

        if (acks.size() >= quorum && indice > commitIndex) {
            commitIndex = indice;

            // Aplicar todas las entradas pendientes
            while (lastApplied < commitIndex) {
                lastApplied++;
                if (lastApplied < log.size() && log.get((int) lastApplied) != null) {
                    EntradaLog entrada = log.get((int) lastApplied);
                    aplicarEntrada(entrada);

                    nodo.getLog().registrarConsenso(nodo.getReloj().obtenerActual(),
                            "COMMIT",
                            "★ Entrada commiteada por quórum (" + acks.size() + "/" +
                            totalNodos + "): " + entrada);
                }
            }
        }
    }

    /**
     * Aplica una entrada del log al estado local (lista de usuarios globales).
     */
    private void aplicarEntrada(EntradaLog entrada) {
        if (entrada == null) return;

        switch (entrada.operacion) {
            case "CONECTADO":
                usuariosGlobales.put(entrada.usuario, entrada.nodoUsuario);
                break;
            case "DESCONECTADO":
                usuariosGlobales.remove(entrada.usuario);
                break;
        }

        // Broadcast a los clientes locales para reflejar el cambio de estado global
        nodo.enviarListaUsuarios();
    }

    /**
     * Incrementa el término actual. Llamado cuando hay una nueva elección.
     */
    public void nuevoTermino() {
        termActual++;
        nodo.getLog().registrarConsenso(nodo.getReloj().obtenerActual(),
                "NUEVO_TERMINO", "Término incrementado a " + termActual);
    }

    // ===== GETTERS =====
    public int getTermActual()                       { return termActual; }
    public long getCommitIndex()                     { return commitIndex; }
    public int getLogSize()                          { return log.size(); }
    public Map<String, Integer> getUsuariosGlobales() { return Collections.unmodifiableMap(usuariosGlobales); }
    public long getTotalEntradas()                   { return totalEntradas; }
    public long getTotalMensajesConsenso()           { return totalMensajesConsenso; }

    /**
     * Remueve de la lista global de usuarios a todos aquellos conectados a un nodo que cayó.
     */
    public void removerUsuariosDeNodo(int nodoIdCaido) {
        usuariosGlobales.entrySet().removeIf(entry -> entry.getValue() == nodoIdCaido);
    }
}
