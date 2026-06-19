package node;

import common.Mensaje;
import java.util.*;
import java.util.concurrent.*;

/**
 * Implementación del Algoritmo Bully para elección de coordinador.
 * 
 * El algoritmo Bully selecciona al nodo con el ID más alto como coordinador.
 * Se dispara automáticamente cuando el gestor de heartbeats detecta que el
 * coordinador actual ha caído.
 * 
 * Flujo del algoritmo:
 * 1. El nodo que detecta la caída envía ELECCION a todos los nodos con ID mayor
 * 2. Si alguno responde ELECCION_OK → ese nodo se encarga (yo me retiro)
 * 3. Si nadie responde en el timeout → me declaro COORDINADOR
 * 4. El nuevo coordinador envía COORDINADOR a todos los nodos
 * 
 * Requisito §2.3: Elección de coordinador con algoritmo abusón (Bully).
 */
public class AlgoritmoBully {

    // Timeout para esperar respuestas ELECCION_OK
    private static final long TIMEOUT_ELECCION_MS = 4000;  // 4 segundos

    private final NodoServidor nodo;
    private final int nodoId;
    private final List<InfoNodo> peers;

    private volatile boolean eleccionEnProceso = false;
    private volatile boolean recibiOk = false;

    public AlgoritmoBully(NodoServidor nodo, int nodoId, List<InfoNodo> peers) {
        this.nodo = nodo;
        this.nodoId = nodoId;
        this.peers = peers;
    }

    /**
     * Inicia una elección Bully.
     * Envía mensajes ELECCION a todos los nodos con ID mayor que el mío.
     */
    public synchronized void iniciarEleccion() {
        if (eleccionEnProceso) {
            nodo.getLog().registrarEleccion(nodo.getReloj().obtenerActual(),
                    "IGNORADA", "Ya hay una elección en proceso");
            return;
        }

        eleccionEnProceso = true;
        recibiOk = false;

        int[] reloj = nodo.getReloj().prepararEnvio();
        nodo.getLog().registrarEleccion(reloj,
                "INICIO", "Nodo-" + nodoId + " inicia elección Bully");

        // Crear mensaje de elección
        Mensaje msgEleccion = new Mensaje(Mensaje.ELECCION, nodoId, -1,
                String.valueOf(nodoId));
        msgEleccion.setRelojVectorial(reloj);

        // Enviar ELECCION solo a nodos con ID mayor
        int enviados = 0;
        for (InfoNodo peer : peers) {
            if (peer.getId() > nodoId) {
                if (nodo.getConector().enviarAPeer(peer.getId(), msgEleccion)) {
                    enviados++;
                    nodo.getLog().registrarEleccion(reloj,
                            "ELECCION_ENVIADA", "→ Nodo-" + peer.getId());
                }
            }
        }

        if (enviados == 0) {
            // No hay nodos con ID mayor → yo soy el coordinador
            nodo.getLog().registrarEleccion(reloj,
                    "SIN_MAYORES", "No hay nodos con ID mayor. Me declaro coordinador.");
            declararseCoordinador();
            return;
        }

        // Esperar respuestas ELECCION_OK en un hilo separado
        Thread hiloTimeout = new Thread(() -> {
            try {
                Thread.sleep(TIMEOUT_ELECCION_MS);

                if (!recibiOk && eleccionEnProceso) {
                    // Nadie respondió OK → me declaro coordinador
                    nodo.getLog().registrarEleccion(nodo.getReloj().obtenerActual(),
                            "TIMEOUT", "Sin respuesta OK tras " + TIMEOUT_ELECCION_MS +
                            "ms. Me declaro coordinador.");
                    declararseCoordinador();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "Bully-Timeout-" + nodoId);
        hiloTimeout.setDaemon(true);
        hiloTimeout.start();
    }

    /**
     * Procesa un mensaje ELECCION recibido de un nodo con ID menor.
     * Responde con ELECCION_OK e inicia su propia elección.
     * 
     * @param nodoSolicitante ID del nodo que inició la elección
     * @param relojRecibido   Reloj vectorial del mensaje
     */
    public void procesarEleccion(int nodoSolicitante, int[] relojRecibido) {
        int[] reloj = nodo.getReloj().recibirMensaje(relojRecibido);

        nodo.getLog().registrarEleccion(reloj,
                "ELECCION_RECIBIDA", "De Nodo-" + nodoSolicitante +
                ". Mi ID (" + nodoId + ") > su ID (" + nodoSolicitante + "). Respondo OK.");

        // Responder OK al solicitante
        Mensaje msgOk = new Mensaje(Mensaje.ELECCION_OK, nodoId, nodoSolicitante,
                String.valueOf(nodoId));
        int[] relojOk = nodo.getReloj().prepararEnvio();
        msgOk.setRelojVectorial(relojOk);
        nodo.getConector().enviarAPeer(nodoSolicitante, msgOk);

        nodo.getLog().registrarEleccion(relojOk,
                "OK_ENVIADO", "→ Nodo-" + nodoSolicitante);

        // Iniciar mi propia elección (yo tengo ID mayor)
        iniciarEleccion();
    }

    /**
     * Procesa un mensaje ELECCION_OK recibido.
     * Significa que un nodo con ID mayor está vivo y se encargará.
     * 
     * @param nodoRespondedor ID del nodo que respondió OK
     * @param relojRecibido   Reloj vectorial del mensaje
     */
    public void procesarOk(int nodoRespondedor, int[] relojRecibido) {
        int[] reloj = nodo.getReloj().recibirMensaje(relojRecibido);

        nodo.getLog().registrarEleccion(reloj,
                "OK_RECIBIDO", "De Nodo-" + nodoRespondedor +
                ". Nodo con mayor ID existe. Me retiro de la elección.");

        recibiOk = true;
        // No me declaro coordinador; el nodo con mayor ID lo hará.
    }

    /**
     * Se declara como coordinador y lo anuncia a todos los nodos.
     */
    private void declararseCoordinador() {
        int[] reloj = nodo.getReloj().prepararEnvio();

        nodo.getLog().registrarEleccion(reloj,
                "NUEVO_COORDINADOR",
                "★ Nodo-" + nodoId + " es el nuevo COORDINADOR ★");

        // Actualizar coordinador local
        nodo.setCoordinadorActual(nodoId);

        // Anunciar a todos los peers
        Mensaje msgCoord = new Mensaje(Mensaje.COORDINADOR, nodoId, -1,
                String.valueOf(nodoId));
        msgCoord.setRelojVectorial(reloj);
        nodo.getConector().broadcast(msgCoord);

        eleccionEnProceso = false;
        nodo.enviarNotificacion("[SISTEMA RECUPERADO] El Nodo-" + nodoId + " es el nuevo coordinador.");
    }

    /**
     * Procesa un anuncio de nuevo coordinador.
     * 
     * @param nuevoCoordinador ID del nuevo coordinador
     * @param relojRecibido    Reloj vectorial del mensaje
     */
    public void procesarCoordinador(int nuevoCoordinador, int[] relojRecibido) {
        int[] reloj = nodo.getReloj().recibirMensaje(relojRecibido);

        int coordinadorAnterior = nodo.getCoordinadorActual();
        nodo.setCoordinadorActual(nuevoCoordinador);
        eleccionEnProceso = false;

        nodo.getLog().registrarEleccion(reloj,
                "COORDINADOR_ACEPTADO",
                "Nuevo coordinador: Nodo-" + nuevoCoordinador +
                " (anterior: Nodo-" + coordinadorAnterior + ")");
        nodo.enviarNotificacion("[SISTEMA RECUPERADO] El Nodo-" + nuevoCoordinador + " es el nuevo coordinador.");
    }

    /**
     * Verifica si hay una elección en progreso.
     */
    public boolean isEleccionEnProceso() {
        return eleccionEnProceso;
    }

    /**
     * Determina el coordinador inicial basándose en el mayor ID activo.
     * Se llama al inicio del sistema cuando todos los nodos se han conectado.
     */
    public void determinarCoordinadorInicial() {
        int mayorId = nodoId;
        for (InfoNodo peer : peers) {
            if (peer.isActivo() && peer.getId() > mayorId) {
                mayorId = peer.getId();
            }
        }

        nodo.setCoordinadorActual(mayorId);
        nodo.getLog().registrarEleccion(nodo.getReloj().obtenerActual(),
                "COORDINADOR_INICIAL",
                "Coordinador inicial determinado: Nodo-" + mayorId);

        // Si yo soy el coordinador, anunciar
        if (mayorId == nodoId) {
            declararseCoordinador();
        }
    }
}
