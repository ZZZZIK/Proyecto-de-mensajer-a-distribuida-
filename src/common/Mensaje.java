package common;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * =====================================================================
 * CLASE MENSAJE — Objeto Serializable para Marshalling
 * =====================================================================
 * 
 * MARSHALLING / SERIALIZACIÓN:
 * Esta clase implementa la interfaz Serializable, lo que permite que
 * los objetos Mensaje sean convertidos automáticamente a una secuencia
 * de bytes (serialización) al ser enviados a través del socket TCP
 * mediante ObjectOutputStream, y reconstruidos en el extremo receptor
 * (deserialización) mediante ObjectInputStream.
 * 
 * Esto es fundamental en sistemas distribuidos porque permite enviar
 * estructuras de datos complejas (no solo Strings primitivos) entre
 * procesos remotos de forma completamente TRANSPARENTE para el
 * programador.
 * 
 * El campo serialVersionUID garantiza la compatibilidad entre versiones
 * del objeto serializado en distintos procesos/JVMs.
 * 
 * ESTRUCTURA:
 * - emisor:        Nombre del usuario que envía el mensaje
 * - receptor:      Nombre del destinatario (o "TODOS" para broadcast)
 * - tipo:          Tipo de mensaje (TEXTO, ARCHIVO, CONECTAR, etc.)
 * - contenido:     Contenido textual del mensaje
 * - datosAdjuntos: Bytes del archivo adjunto (para tipo ARCHIVO)
 * - nombreArchivo: Nombre original del archivo adjunto
 * - timestamp:     Marca de tiempo de creación del mensaje
 * =====================================================================
 */
public class Mensaje implements Serializable {

    // Versión de serialización para garantizar compatibilidad entre procesos
    private static final long serialVersionUID = 1L;

    // ===== TIPOS DE MENSAJE =====
    // Las dos funciones principales del sistema:
    public static final String TEXTO   = "TEXTO";    // Función 1: Mensajería de texto
    public static final String ARCHIVO = "ARCHIVO";  // Función 2: Envío de archivos multimedia

    // Tipos de control del sistema (señalización interna):
    public static final String CONECTAR        = "CONECTAR";        // Cliente solicita conexión (legacy)
    public static final String LOGIN           = "LOGIN";           // Cliente solicita inicio de sesión
    public static final String REGISTRO        = "REGISTRO";        // Cliente solicita crear cuenta nueva
    public static final String DESCONECTAR     = "DESCONECTAR";     // Cliente notifica desconexión
    public static final String LISTA_USUARIOS  = "LISTA_USUARIOS";  // Servidor envía lista de conectados
    public static final String LISTA_OFFLINE   = "LISTA_OFFLINE";   // Servidor envía lista de desconectados
    public static final String NOTIFICACION    = "NOTIFICACION";    // Notificaciones del sistema
    public static final String AUTH_OK         = "AUTH_OK";         // Autenticación exitosa
    public static final String AUTH_FAIL       = "AUTH_FAIL";       // Autenticación fallida
    public static final String HISTORIAL       = "HISTORIAL";       // Servidor envía mensaje del historial

    // ===== CAMPOS DEL MENSAJE =====
    private String emisor;          // Quién envía
    private String receptor;        // A quién va dirigido
    private String tipo;            // Tipo de mensaje (constantes arriba)
    private String contenido;       // Contenido textual
    private byte[] datosAdjuntos;   // Datos binarios del archivo (marshalling de bytes)
    private String nombreArchivo;   // Nombre del archivo adjunto
    private long timestamp;         // Marca de tiempo (milisegundos desde epoch)

    /**
     * Constructor para mensajes de TEXTO y de control del sistema.
     * 
     * @param emisor    Nombre del usuario emisor
     * @param receptor  Nombre del usuario receptor (o "TODOS")
     * @param tipo      Tipo de mensaje (TEXTO, CONECTAR, etc.)
     * @param contenido Contenido textual del mensaje
     */
    public Mensaje(String emisor, String receptor, String tipo, String contenido) {
        this.emisor = emisor;
        this.receptor = receptor;
        this.tipo = tipo;
        this.contenido = contenido;
        this.timestamp = System.currentTimeMillis();
    }

    /**
     * Constructor para mensajes de tipo ARCHIVO.
     * Los datosAdjuntos (byte[]) son serializados junto con el objeto,
     * permitiendo el envío de archivos binarios a través del socket TCP.
     * 
     * @param emisor        Nombre del usuario emisor
     * @param receptor      Nombre del usuario receptor
     * @param nombreArchivo Nombre original del archivo
     * @param datosAdjuntos Contenido binario del archivo (bytes)
     */
    public Mensaje(String emisor, String receptor, String nombreArchivo, byte[] datosAdjuntos) {
        this.emisor = emisor;
        this.receptor = receptor;
        this.tipo = ARCHIVO;
        this.contenido = "Archivo enviado: " + nombreArchivo;
        this.datosAdjuntos = datosAdjuntos;
        this.nombreArchivo = nombreArchivo;
        this.timestamp = System.currentTimeMillis();
    }

    // ===== GETTERS =====
    public String getEmisor()         { return emisor; }
    public String getReceptor()       { return receptor; }
    public String getTipo()           { return tipo; }
    public String getContenido()      { return contenido; }
    public byte[] getDatosAdjuntos()  { return datosAdjuntos; }
    public String getNombreArchivo()  { return nombreArchivo; }
    public long   getTimestamp()      { return timestamp; }

    // ===== SETTERS =====
    public void setEmisor(String emisor)                 { this.emisor = emisor; }
    public void setReceptor(String receptor)             { this.receptor = receptor; }
    public void setTipo(String tipo)                     { this.tipo = tipo; }
    public void setContenido(String contenido)           { this.contenido = contenido; }
    public void setDatosAdjuntos(byte[] datosAdjuntos)   { this.datosAdjuntos = datosAdjuntos; }
    public void setNombreArchivo(String nombreArchivo)   { this.nombreArchivo = nombreArchivo; }

    /**
     * Devuelve la marca de tiempo formateada como hora legible.
     */
    public String getHoraFormateada() {
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
        return sdf.format(new Date(timestamp));
    }

    @Override
    public String toString() {
        return "[" + getHoraFormateada() + "] " + emisor + " -> " + receptor + " (" + tipo + "): " + contenido;
    }
}
