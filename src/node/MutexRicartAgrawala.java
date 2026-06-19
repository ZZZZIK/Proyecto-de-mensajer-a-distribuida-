package node;

import common.Mensaje;
import java.util.*;
import java.util.concurrent.*;

/**
 * Implementación del algoritmo de Ricart-Agrawala para exclusión mutua distribuida.
 * 
 * Protege el acceso concurrente al recurso compartido: la escritura en la base
 * de datos del historial de mensajes. Cuando un nodo necesita escribir, debe
 * obtener permiso de todos los demás nodos activos.
 * 
 * Flujo del algoritmo:
 * 1. Para entrar a la sección crítica: enviar MUTEX_REQUEST con timestamp a todos
 * 2. Cada nodo que recibe la solicitud:
 *    - Si NO está pidiendo acceso ni está en SC → responde MUTEX_REPLY inmediato
 *    - Si está pidiendo acceso y su timestamp es MAYOR → difiere la respuesta
 *    - Si está en la SC → difiere la respuesta
 * 3. Cuando recibe MUTEX_REPLY de todos los nodos activos → entra a la SC
 * 4. Al salir de la SC: enviar MUTEX_REPLY a todos los diferidos
 * 
 * Requisito §2.3: Exclusión mutua distribuida (Ricart-Agrawala).
 */
public class MutexRicartAgrawala {

    // Timeout para esperar las respuestas (evitar bloqueo indefinido)
    private static final long TIMEOUT_MUTEX_MS = 10000;  // 10 segundos

    private final NodoServidor nodo;
    private final int nodoId;
    private final List<InfoNodo> peers;

    // Estado del mutex
    private volatile boolean solicitando = false;       // ¿Estoy pidiendo acceso?
    private volatile boolean enSeccionCritica = false;   // ¿Estoy dentro de la SC?
    private int[] timestampSolicitud;                    // Mi reloj al momento de solicitar

    // Conteo de respuestas recibidas
    private final CountDownLatch[] latchRespuestas = new CountDownLatch[1];

    // Cola de nodos cuya respuesta fue diferida (les debo un REPLY)
    private final Queue<Integer> diferidos = new ConcurrentLinkedQueue<>();

    // Lock para sincronizar el estado interno
    private final Object lockEstado = new Object();

    // Métricas
    private volatile long totalSolicitudes = 0;
    private volatile long totalMensajesMutex = 0;

    public MutexRicartAgrawala(NodoServidor nodo, int nodoId, List<InfoNodo> peers) {
        this.nodo = nodo;
        this.nodoId = nodoId;
        this.peers = peers;
    }

    /**
     * Solicita acceso a la sección crítica.
     * Envía MUTEX_REQUEST a todos los nodos activos y espera sus respuestas.
     * 
     * @return true si se obtuvo el acceso, false si timeout
     */
    public boolean solicitarAcceso() {
        synchronized (lockEstado) {
            solicitando = true;
            timestampSolicitud = nodo.getReloj().prepararEnvio();
            totalSolicitudes++;
        }

        // Determinar cuántos nodos activos deben responder
        List<Integer> nodosActivos = new ArrayList<>();
        for (InfoNodo peer : peers) {
            if (peer.isActivo() && nodo.getConector().estaConectado(peer.getId())) {
                nodosActivos.add(peer.getId());
            }
        }

        nodo.getLog().registrarMutex(timestampSolicitud,
                "REQUEST_INICIO",
                "Solicitando acceso a SC. Esperando " + nodosActivos.size() + " respuestas");

        if (nodosActivos.isEmpty()) {
            // No hay otros nodos activos → acceso inmediato
            synchronized (lockEstado) {
                solicitando = false;
                enSeccionCritica = true;
            }
            nodo.getLog().registrarMutex(timestampSolicitud,
                    "SC_ENTRADA_INMEDIATA", "Sin peers activos, acceso directo");
            return true;
        }

        // Preparar latch para esperar las respuestas
        CountDownLatch latch = new CountDownLatch(nodosActivos.size());
        latchRespuestas[0] = latch;

        // Enviar MUTEX_REQUEST a todos los nodos activos
        Mensaje request = new Mensaje(Mensaje.MUTEX_REQUEST, nodoId, -1,
                String.valueOf(nodoId));
        request.setRelojVectorial(timestampSolicitud);

        for (int peerId : nodosActivos) {
            if (nodo.getConector().enviarAPeer(peerId, request)) {
                totalMensajesMutex++;
                nodo.getLog().registrarMutex(timestampSolicitud,
                        "REQUEST_ENVIADO", "→ Nodo-" + peerId);
            } else {
                // Si no se puede enviar, contar como respondido
                latch.countDown();
            }
        }

        // Esperar todas las respuestas o timeout
        try {
            boolean todosRespondieron = latch.await(TIMEOUT_MUTEX_MS, TimeUnit.MILLISECONDS);

            synchronized (lockEstado) {
                solicitando = false;
                if (todosRespondieron) {
                    enSeccionCritica = true;
                    nodo.getLog().registrarMutex(nodo.getReloj().obtenerActual(),
                            "SC_ENTRADA",
                            "★ Entró a la Sección Crítica (todas las respuestas recibidas)");
                    return true;
                } else {
                    nodo.getLog().registrarMutex(nodo.getReloj().obtenerActual(),
                            "SC_TIMEOUT",
                            "Timeout esperando respuestas de mutex. Entrando de emergencia.");
                    enSeccionCritica = true;
                    return true; // Entrar de todas formas para no bloquear indefinido
                }
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            synchronized (lockEstado) {
                solicitando = false;
            }
            return false;
        }
    }

    /**
     * Libera la sección crítica.
     * Envía MUTEX_REPLY a todos los nodos cuya respuesta fue diferida.
     */
    public void liberarAcceso() {
        synchronized (lockEstado) {
            enSeccionCritica = false;
        }

        int[] reloj = nodo.getReloj().prepararEnvio();
        nodo.getLog().registrarMutex(reloj,
                "SC_SALIDA", "Salió de la Sección Crítica. Respondiendo a " +
                diferidos.size() + " solicitudes diferidas.");

        // Enviar REPLY a todos los diferidos
        while (!diferidos.isEmpty()) {
            int nododiferido = diferidos.poll();
            Mensaje reply = new Mensaje(Mensaje.MUTEX_REPLY, nodoId, nododiferido,
                    "OK");
            reply.setRelojVectorial(nodo.getReloj().prepararEnvio());
            nodo.getConector().enviarAPeer(nododiferido, reply);
            totalMensajesMutex++;

            nodo.getLog().registrarMutex(reply.getRelojVectorial(),
                    "REPLY_DIFERIDO", "→ Nodo-" + nododiferido + " (diferido liberado)");
        }
    }

    /**
     * Procesa una solicitud MUTEX_REQUEST de otro nodo.
     * 
     * Reglas de Ricart-Agrawala:
     * 1. Si NO estoy solicitando NI en SC → respondo REPLY inmediato
     * 2. Si estoy en SC → difiero la respuesta
     * 3. Si estoy solicitando y MI timestamp es MENOR → difiero la respuesta
     * 4. Si estoy solicitando y MI timestamp es MAYOR → respondo REPLY
     * 
     * @param nodoSolicitante ID del nodo que solicita
     * @param relojRecibido   Reloj vectorial de la solicitud
     */
    public void procesarSolicitud(int nodoSolicitante, int[] relojRecibido) {
        nodo.getReloj().recibirMensaje(relojRecibido);
        totalMensajesMutex++;

        synchronized (lockEstado) {
            boolean debeResponder;

            if (!solicitando && !enSeccionCritica) {
                // Caso 1: No estoy interesado → responder inmediato
                debeResponder = true;
                nodo.getLog().registrarMutex(nodo.getReloj().obtenerActual(),
                        "REQUEST_RECIBIDO",
                        "De Nodo-" + nodoSolicitante + ". No estoy en SC ni solicitando → REPLY inmediato");

            } else if (enSeccionCritica) {
                // Caso 2: Estoy en SC → diferir
                debeResponder = false;
                nodo.getLog().registrarMutex(nodo.getReloj().obtenerActual(),
                        "REQUEST_DIFERIDO",
                        "De Nodo-" + nodoSolicitante + ". Estoy en SC → Respuesta diferida");

            } else {
                // Caso 3 o 4: Ambos solicitando → comparar timestamps
                int comparacion = RelojVectorial.compararPrioridad(
                        relojRecibido, nodoSolicitante,
                        timestampSolicitud, nodoId);

                if (comparacion < 0) {
                    // El solicitante tiene mayor prioridad (timestamp menor)
                    debeResponder = true;
                    nodo.getLog().registrarMutex(nodo.getReloj().obtenerActual(),
                            "REQUEST_PRIORIDAD",
                            "De Nodo-" + nodoSolicitante +
                            ". Su timestamp tiene prioridad → REPLY inmediato");
                } else {
                    // Yo tengo mayor prioridad → diferir
                    debeResponder = false;
                    nodo.getLog().registrarMutex(nodo.getReloj().obtenerActual(),
                            "REQUEST_DIFERIDO",
                            "De Nodo-" + nodoSolicitante +
                            ". Mi timestamp tiene prioridad → Respuesta diferida");
                }
            }

            if (debeResponder) {
                // Enviar REPLY inmediato
                Mensaje reply = new Mensaje(Mensaje.MUTEX_REPLY, nodoId, nodoSolicitante, "OK");
                reply.setRelojVectorial(nodo.getReloj().prepararEnvio());
                nodo.getConector().enviarAPeer(nodoSolicitante, reply);
                totalMensajesMutex++;

                nodo.getLog().registrarMutex(reply.getRelojVectorial(),
                        "REPLY_ENVIADO", "→ Nodo-" + nodoSolicitante);
            } else {
                // Diferir la respuesta
                diferidos.add(nodoSolicitante);
            }
        }
    }

    /**
     * Procesa una respuesta MUTEX_REPLY recibida.
     * Decrementa el latch de respuestas pendientes.
     * 
     * @param nodoRespondedor ID del nodo que respondió
     * @param relojRecibido   Reloj vectorial de la respuesta
     */
    public void procesarRespuesta(int nodoRespondedor, int[] relojRecibido) {
        nodo.getReloj().recibirMensaje(relojRecibido);
        totalMensajesMutex++;

        nodo.getLog().registrarMutex(nodo.getReloj().obtenerActual(),
                "REPLY_RECIBIDO", "De Nodo-" + nodoRespondedor);

        if (latchRespuestas[0] != null) {
            latchRespuestas[0].countDown();
        }
    }

    // ===== MÉTRICAS =====
    public long getTotalSolicitudes()    { return totalSolicitudes; }
    public long getTotalMensajesMutex()  { return totalMensajesMutex; }
    public boolean isEnSeccionCritica()  { return enSeccionCritica; }
}
