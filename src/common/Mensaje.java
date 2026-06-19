package common;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

/**
 * Representa un mensaje transferido entre clientes, nodos y el sistema.
 * Implementa Serializable para poder ser enviado a través de sockets TCP.
 * 
 * Soporta tanto comunicación cliente-servidor como comunicación inter-nodo
 * para los protocolos distribuidos (relojes vectoriales, exclusión mutua,
 * elección de coordinador, consenso Raft y heartbeats).
 */
public class Mensaje implements Serializable {

    // Versión de serialización para garantizar compatibilidad entre procesos
    private static final long serialVersionUID = 2L;

    // ===== TIPOS DE MENSAJE: FUNCIONES PRINCIPALES =====
    public static final String TEXTO   = "TEXTO";    // Función 1: Mensajería de texto
    public static final String ARCHIVO = "ARCHIVO";  // Función 2: Envío de archivos multimedia

    // ===== TIPOS DE MENSAJE: CONTROL CLIENTE-SERVIDOR =====
    public static final String CONECTAR        = "CONECTAR";
    public static final String LOGIN           = "LOGIN";
    public static final String REGISTRO        = "REGISTRO";
    public static final String DESCONECTAR     = "DESCONECTAR";
    public static final String LISTA_USUARIOS  = "LISTA_USUARIOS";
    public static final String LISTA_OFFLINE   = "LISTA_OFFLINE";
    public static final String NOTIFICACION    = "NOTIFICACION";
    public static final String AUTH_OK         = "AUTH_OK";
    public static final String AUTH_FAIL       = "AUTH_FAIL";
    public static final String HISTORIAL       = "HISTORIAL";

    // ===== TIPOS DE MENSAJE: PROTOCOLO INTER-NODO =====
    // Heartbeats (Tolerancia a fallos §2.4)
    public static final String HEARTBEAT       = "HEARTBEAT";
    public static final String HEARTBEAT_ACK   = "HEARTBEAT_ACK";

    // Algoritmo Bully — Elección de coordinador (§2.3)
    public static final String ELECCION        = "ELECCION";
    public static final String ELECCION_OK     = "ELECCION_OK";
    public static final String COORDINADOR     = "COORDINADOR";

    // Ricart-Agrawala — Exclusión mutua distribuida (§2.3)
    public static final String MUTEX_REQUEST   = "MUTEX_REQUEST";
    public static final String MUTEX_REPLY     = "MUTEX_REPLY";
    public static final String MUTEX_RELEASE   = "MUTEX_RELEASE";

    // Raft simplificado — Consenso (§2.3, puntaje extra)
    public static final String RAFT_APPEND     = "RAFT_APPEND";
    public static final String RAFT_APPEND_ACK = "RAFT_APPEND_ACK";
    public static final String RAFT_VOTE_REQ   = "RAFT_VOTE_REQ";
    public static final String RAFT_VOTE_RESP  = "RAFT_VOTE_RESP";

    // Descubrimiento y sincronización de nodos
    public static final String NODO_JOIN       = "NODO_JOIN";
    public static final String NODO_LEAVE      = "NODO_LEAVE";
    public static final String SYNC_ESTADO     = "SYNC_ESTADO";

    // Reenvío inter-nodo de mensajes de usuario
    public static final String REENVIO_TEXTO   = "REENVIO_TEXTO";
    public static final String REENVIO_ARCHIVO = "REENVIO_ARCHIVO";

    // ===== CAMPOS ORIGINALES DEL MENSAJE =====
    private String emisor;          // Quién envía
    private String receptor;        // A quién va dirigido
    private String tipo;            // Tipo de mensaje (constantes arriba)
    private String contenido;       // Contenido textual
    private byte[] datosAdjuntos;   // Datos binarios del archivo (marshalling de bytes)
    private String nombreArchivo;   // Nombre del archivo adjunto
    private long timestamp;         // Marca de tiempo (milisegundos desde epoch)

    // ===== CAMPOS DISTRIBUIDOS (Entrega 2) =====
    private int[] relojVectorial;   // Reloj vectorial [nodo0, nodo1, nodo2, ...]
    private int nodoOrigen;         // ID del nodo que origina el mensaje (0, 1, 2)
    private int nodoDestino;        // ID del nodo destino (-1 = broadcast a todos)
    private long secuenciaGlobal;   // Número de secuencia para consenso Raft
    private int termRaft;           // Término actual en protocolo Raft

    // ===== CONSTRUCTORES =====

    /**
     * Constructor para mensajes de TEXTO y de control del sistema.
     */
    public Mensaje(String emisor, String receptor, String tipo, String contenido) {
        this.emisor = emisor;
        this.receptor = receptor;
        this.tipo = tipo;
        this.contenido = contenido;
        this.timestamp = System.currentTimeMillis();
        this.nodoOrigen = -1;
        this.nodoDestino = -1;
    }

    /**
     * Constructor para mensajes de tipo ARCHIVO con datos adjuntos.
     */
    public Mensaje(String emisor, String receptor, String nombreArchivo, byte[] datosAdjuntos) {
        this.emisor = emisor;
        this.receptor = receptor;
        this.tipo = ARCHIVO;
        this.contenido = "Archivo enviado: " + nombreArchivo;
        this.datosAdjuntos = datosAdjuntos;
        this.nombreArchivo = nombreArchivo;
        this.timestamp = System.currentTimeMillis();
        this.nodoOrigen = -1;
        this.nodoDestino = -1;
    }

    /**
     * Constructor para mensajes inter-nodo del protocolo distribuido.
     * 
     * @param tipo         Tipo de mensaje inter-nodo
     * @param nodoOrigen   ID del nodo que envía
     * @param nodoDestino  ID del nodo destino (-1 = broadcast)
     * @param contenido    Datos del protocolo
     */
    public Mensaje(String tipo, int nodoOrigen, int nodoDestino, String contenido) {
        this.emisor = "NODO-" + nodoOrigen;
        this.receptor = nodoDestino == -1 ? "TODOS" : "NODO-" + nodoDestino;
        this.tipo = tipo;
        this.contenido = contenido;
        this.nodoOrigen = nodoOrigen;
        this.nodoDestino = nodoDestino;
        this.timestamp = System.currentTimeMillis();
    }

    // ===== GETTERS ORIGINALES =====
    public String getEmisor()         { return emisor; }
    public String getReceptor()       { return receptor; }
    public String getTipo()           { return tipo; }
    public String getContenido()      { return contenido; }
    public byte[] getDatosAdjuntos()  { return datosAdjuntos; }
    public String getNombreArchivo()  { return nombreArchivo; }
    public long   getTimestamp()      { return timestamp; }

    // ===== GETTERS DISTRIBUIDOS =====
    public int[]  getRelojVectorial()   { return relojVectorial; }
    public int    getNodoOrigen()       { return nodoOrigen; }
    public int    getNodoDestino()      { return nodoDestino; }
    public long   getSecuenciaGlobal()  { return secuenciaGlobal; }
    public int    getTermRaft()         { return termRaft; }

    // ===== SETTERS ORIGINALES =====
    public void setEmisor(String emisor)                 { this.emisor = emisor; }
    public void setReceptor(String receptor)             { this.receptor = receptor; }
    public void setTipo(String tipo)                     { this.tipo = tipo; }
    public void setContenido(String contenido)           { this.contenido = contenido; }
    public void setDatosAdjuntos(byte[] datosAdjuntos)   { this.datosAdjuntos = datosAdjuntos; }
    public void setNombreArchivo(String nombreArchivo)   { this.nombreArchivo = nombreArchivo; }

    // ===== SETTERS DISTRIBUIDOS =====
    public void setRelojVectorial(int[] relojVectorial)  { this.relojVectorial = relojVectorial; }
    public void setNodoOrigen(int nodoOrigen)             { this.nodoOrigen = nodoOrigen; }
    public void setNodoDestino(int nodoDestino)           { this.nodoDestino = nodoDestino; }
    public void setSecuenciaGlobal(long secuencia)        { this.secuenciaGlobal = secuencia; }
    public void setTermRaft(int termRaft)                 { this.termRaft = termRaft; }

    /**
     * Devuelve la marca de tiempo formateada como hora legible.
     */
    public String getHoraFormateada() {
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
        return sdf.format(new Date(timestamp));
    }

    /**
     * Devuelve el reloj vectorial formateado como cadena legible.
     */
    public String getRelojFormateado() {
        if (relojVectorial == null) return "[-]";
        return Arrays.toString(relojVectorial);
    }

    /**
     * Verifica si este mensaje es de un protocolo inter-nodo.
     */
    public boolean esInterNodo() {
        return tipo.equals(HEARTBEAT) || tipo.equals(HEARTBEAT_ACK) ||
               tipo.equals(ELECCION) || tipo.equals(ELECCION_OK) || tipo.equals(COORDINADOR) ||
               tipo.equals(MUTEX_REQUEST) || tipo.equals(MUTEX_REPLY) || tipo.equals(MUTEX_RELEASE) ||
               tipo.equals(RAFT_APPEND) || tipo.equals(RAFT_APPEND_ACK) ||
               tipo.equals(RAFT_VOTE_REQ) || tipo.equals(RAFT_VOTE_RESP) ||
               tipo.equals(NODO_JOIN) || tipo.equals(NODO_LEAVE) || tipo.equals(SYNC_ESTADO) ||
               tipo.equals(REENVIO_TEXTO) || tipo.equals(REENVIO_ARCHIVO);
    }

    @Override
    public String toString() {
        String relojStr = (relojVectorial != null) ? " V=" + getRelojFormateado() : "";
        String nodoStr = (nodoOrigen >= 0) ? " N" + nodoOrigen + "→N" + nodoDestino : "";
        return "[" + getHoraFormateada() + "]" + relojStr + nodoStr +
               " " + emisor + " -> " + receptor + " (" + tipo + "): " + contenido;
    }
}
