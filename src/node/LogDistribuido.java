package node;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

/**
 * Sistema de logging distribuido con marcas de reloj vectorial.
 * 
 * Cada entrada de log incluye:
 * - Timestamp real (hora del sistema)
 * - ID del nodo
 * - Reloj vectorial al momento del evento
 * - Categoría del evento
 * - Descripción del evento
 * 
 * Escribe simultáneamente a:
 * 1. Consola (con colores ANSI para legibilidad)
 * 2. Archivo de log rotativo (logs/nodoX_YYYY-MM-DD.log)
 */
public class LogDistribuido {

    // Categorías de eventos
    public static final String MENSAJE    = "MENSAJE";
    public static final String MUTEX      = "MUTEX";
    public static final String ELECCION   = "ELECCION";
    public static final String HEARTBEAT  = "HEARTBEAT";
    public static final String FALLO      = "FALLO";
    public static final String CONSENSO   = "CONSENSO";
    public static final String METRICAS   = "METRICAS";
    public static final String NODO       = "NODO";
    public static final String SISTEMA    = "SISTEMA";

    // Colores ANSI para consola
    private static final String RESET   = "\u001B[0m";
    private static final String ROJO    = "\u001B[31m";
    private static final String VERDE   = "\u001B[32m";
    private static final String AMARILLO = "\u001B[33m";
    private static final String AZUL    = "\u001B[34m";
    private static final String MAGENTA = "\u001B[35m";
    private static final String CYAN    = "\u001B[36m";
    private static final String BLANCO  = "\u001B[37m";
    private static final String NEGRITA = "\u001B[1m";

    private final int nodoId;
    private final String directorioLogs;
    private PrintWriter archivoLog;
    private final SimpleDateFormat formatoFecha = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final Object lockEscritura = new Object();

    // Contadores de eventos para métricas
    private volatile long contadorMensajes = 0;
    private volatile long contadorMutex = 0;
    private volatile long contadorElecciones = 0;
    private volatile long contadorHeartbeats = 0;
    private volatile long contadorFallos = 0;
    private volatile long contadorConsenso = 0;

    /**
     * Crea un sistema de logging para un nodo específico.
     * 
     * @param nodoId          ID del nodo
     * @param directorioLogs  Directorio donde guardar los archivos de log
     */
    public LogDistribuido(int nodoId, String directorioLogs) {
        this.nodoId = nodoId;
        this.directorioLogs = directorioLogs;
        inicializarArchivo();
    }

    /**
     * Crea el directorio de logs y abre el archivo.
     */
    private void inicializarArchivo() {
        try {
            File dir = new File(directorioLogs);
            if (!dir.exists()) {
                dir.mkdirs();
            }

            String fecha = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
            String nombreArchivo = directorioLogs + File.separator +
                    "nodo" + nodoId + "_" + fecha + ".log";

            archivoLog = new PrintWriter(
                    new BufferedWriter(new FileWriter(nombreArchivo, true)), true);

            registrar(SISTEMA, null, "=== LOG INICIADO — Nodo " + nodoId +
                    " — " + formatoFecha.format(new Date()) + " ===");

        } catch (IOException e) {
            System.err.println("[NODO-" + nodoId + "] Error abriendo archivo de log: " + e.getMessage());
        }
    }

    /**
     * Registra un evento en el log con marca vectorial.
     * 
     * @param categoria      Categoría del evento (MENSAJE, MUTEX, etc.)
     * @param relojVectorial Reloj vectorial al momento del evento (puede ser null)
     * @param descripcion    Descripción del evento
     */
    public void registrar(String categoria, int[] relojVectorial, String descripcion) {
        String timestamp = formatoFecha.format(new Date());
        String relojStr = (relojVectorial != null) ? "V=" + Arrays.toString(relojVectorial) : "V=[-]";
        String lineaCompleta = String.format("[%s] [NODO-%d] [%s] [%s] %s",
                timestamp, nodoId, relojStr, categoria, descripcion);

        // Incrementar contador
        incrementarContador(categoria);

        synchronized (lockEscritura) {
            // Escribir a consola con colores
            String color = obtenerColor(categoria);
            System.out.println(color + lineaCompleta + RESET);

            // Escribir a archivo sin colores
            if (archivoLog != null) {
                archivoLog.println(lineaCompleta);
            }
        }

        // Registrar en Base de Datos de forma asíncrona (para evitar bloquear hilos críticos)
        String relojDbStr = (relojVectorial != null) ? Arrays.toString(relojVectorial) : "[-]";
        Thread dbThread = new Thread(() -> {
            server.GestorBD.registrarEvento(nodoId, categoria, relojDbStr, descripcion);
        });
        dbThread.setDaemon(true);
        dbThread.start();
    }

    /**
     * Registra un evento de mensaje (Función 1 o 2).
     */
    public void registrarMensaje(int[] relojVectorial, String emisor, String receptor,
                                  String tipo, String contenido) {
        registrar(MENSAJE, relojVectorial,
                tipo + ": " + emisor + " → " + receptor + " \"" + contenido + "\"");
    }

    /**
     * Registra un evento de exclusión mutua.
     */
    public void registrarMutex(int[] relojVectorial, String accion, String detalle) {
        registrar(MUTEX, relojVectorial, accion + " — " + detalle);
    }

    /**
     * Registra un evento de elección de coordinador.
     */
    public void registrarEleccion(int[] relojVectorial, String accion, String detalle) {
        registrar(ELECCION, relojVectorial, accion + " — " + detalle);
    }

    /**
     * Registra un evento de heartbeat.
     */
    public void registrarHeartbeat(int[] relojVectorial, String detalle) {
        registrar(HEARTBEAT, relojVectorial, detalle);
    }

    /**
     * Registra un evento de fallo detectado.
     */
    public void registrarFallo(int[] relojVectorial, String detalle) {
        registrar(FALLO, relojVectorial, "⚠ " + detalle);
    }

    /**
     * Registra un evento de consenso Raft.
     */
    public void registrarConsenso(int[] relojVectorial, String accion, String detalle) {
        registrar(CONSENSO, relojVectorial, accion + " — " + detalle);
    }

    /**
     * Obtiene el color ANSI para una categoría de evento.
     */
    private String obtenerColor(String categoria) {
        switch (categoria) {
            case MENSAJE:    return CYAN;
            case MUTEX:      return MAGENTA;
            case ELECCION:   return AMARILLO + NEGRITA;
            case HEARTBEAT:  return VERDE;
            case FALLO:      return ROJO + NEGRITA;
            case CONSENSO:   return AZUL;
            case METRICAS:   return BLANCO;
            case NODO:       return VERDE + NEGRITA;
            case SISTEMA:    return NEGRITA;
            default:         return RESET;
        }
    }

    /**
     * Incrementa el contador de eventos de una categoría.
     */
    private void incrementarContador(String categoria) {
        switch (categoria) {
            case MENSAJE:    contadorMensajes++; break;
            case MUTEX:      contadorMutex++; break;
            case ELECCION:   contadorElecciones++; break;
            case HEARTBEAT:  contadorHeartbeats++; break;
            case FALLO:      contadorFallos++; break;
            case CONSENSO:   contadorConsenso++; break;
        }
    }

    // ===== GETTERS DE CONTADORES =====
    public long getContadorMensajes()     { return contadorMensajes; }
    public long getContadorMutex()        { return contadorMutex; }
    public long getContadorElecciones()   { return contadorElecciones; }
    public long getContadorHeartbeats()   { return contadorHeartbeats; }
    public long getContadorFallos()       { return contadorFallos; }
    public long getContadorConsenso()     { return contadorConsenso; }

    /**
     * Total de mensajes de coordinación (mutex + elecciones + consenso).
     * Esta es una de las métricas requeridas en §3.2.
     */
    public long getTotalMensajesCoordinacion() {
        return contadorMutex + contadorElecciones + contadorConsenso;
    }

    /**
     * Cierra el archivo de log.
     */
    public void cerrar() {
        synchronized (lockEscritura) {
            registrar(SISTEMA, null, "=== LOG CERRADO — Nodo " + nodoId + " ===");
            if (archivoLog != null) {
                archivoLog.close();
            }
        }
    }
}
