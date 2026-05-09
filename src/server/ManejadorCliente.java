package server;

import common.Mensaje;
import java.io.*;
import java.net.*;

/**
 * =====================================================================
 * MANEJADOR DE CLIENTE — Implementa Runnable (Un hilo por cliente)
 * =====================================================================
 * 
 * CONCURRENCIA — Un hilo por cliente:
 * Esta clase implementa Runnable y cada instancia se ejecuta en un
 * Thread independiente. Esto significa que el servidor puede atender
 * a múltiples clientes de forma CONCURRENTE. Cada ManejadorCliente
 * tiene su propio ciclo de vida y puede leer/escribir en su socket
 * sin bloquear a los demás clientes.
 * 
 * RESILIENCIA — Manejo de fallos independientes:
 * Si un cliente se desconecta abruptamente (cierra la aplicación,
 * pierde conexión de red, etc.), solo el hilo de ESTE ManejadorCliente
 * se ve afectado. El bloque try-catch captura la IOException, limpia
 * los recursos, y el hilo termina de forma ordenada. Los demás clientes
 * y el servidor continúan operando normalmente.
 * 
 * MARSHALLING:
 * Utiliza ObjectInputStream y ObjectOutputStream para deserializar
 * y serializar objetos Mensaje. Esto permite enviar estructuras de
 * datos complejas (incluyendo arrays de bytes para archivos) de forma
 * transparente a través del socket TCP.
 * =====================================================================
 */
public class ManejadorCliente implements Runnable {

    private Socket socket;                  // Socket TCP del cliente
    private ObjectOutputStream salida;      // Stream de salida (serialización)
    private ObjectInputStream entrada;      // Stream de entrada (deserialización)
    private String nombreUsuario;           // Nombre del usuario asignado a este hilo

    /**
     * Constructor: recibe el socket de la conexión TCP establecida.
     * 
     * @param socket Socket TCP del cliente conectado
     */
    public ManejadorCliente(Socket socket) {
        this.socket = socket;
    }

    /**
     * Método principal del hilo — se ejecuta de forma concurrente.
     * 
     * Flujo:
     * 1. Crear los streams de objetos (marshalling)
     * 2. Leer el primer mensaje (CONECTAR) para obtener el nombre de usuario
     * 3. Registrar al cliente en el servidor
     * 4. Entrar en bucle de lectura de mensajes
     * 5. Al finalizar (desconexión o error), limpiar recursos
     */
    @Override
    public void run() {
        try {
            // ===== MARSHALLING: Inicialización de streams de objetos =====
            // ObjectOutputStream DEBE crearse ANTES que ObjectInputStream
            // para evitar deadlock (ambos lados esperando el header del stream).
            // El ObjectOutputStream envía un header de serialización al crearse.
            salida = new ObjectOutputStream(socket.getOutputStream());
            salida.flush();
            entrada = new ObjectInputStream(socket.getInputStream());

            // ===== PASO 1: Leer mensaje de conexión (LOGIN) =====
            // El primer mensaje que envía el cliente debe ser de tipo CONECTAR
            // con su nombre de usuario en el campo emisor.
            Mensaje primerMensaje = (Mensaje) entrada.readObject();

            if (!primerMensaje.getTipo().equals(Mensaje.CONECTAR)) {
                // Si el primer mensaje no es CONECTAR, rechazar la conexión
                Mensaje error = new Mensaje("SERVIDOR", "DESCONOCIDO",
                        Mensaje.NOTIFICACION, "Error: Debe enviar mensaje CONECTAR primero.");
                enviarMensaje(error);
                socket.close();
                return;
            }

            nombreUsuario = primerMensaje.getEmisor();

            // ===== SINCRONIZACIÓN: Registro del cliente =====
            // registrarCliente() está protegido con synchronized
            // para evitar condiciones de carrera al registrar.
            boolean registrado = Servidor.registrarCliente(nombreUsuario, this);

            if (!registrado) {
                // Nombre de usuario ya en uso
                Mensaje error = new Mensaje("SERVIDOR", nombreUsuario,
                        Mensaje.NOTIFICACION,
                        "Error: El nombre '" + nombreUsuario + "' ya está en uso. Elija otro.");
                enviarMensaje(error);
                socket.close();
                return;
            }

            // Confirmar conexión exitosa al cliente
            Mensaje bienvenida = new Mensaje("SERVIDOR", nombreUsuario,
                    Mensaje.NOTIFICACION,
                    "¡Bienvenido al chat, " + nombreUsuario + "! Conexión exitosa.");
            enviarMensaje(bienvenida);

            System.out.println("[HILO-" + nombreUsuario + "] Hilo iniciado para cliente: " + nombreUsuario);

            // ===== PASO 2: Bucle principal de lectura de mensajes =====
            // Este bucle se ejecuta de forma CONCURRENTE con los hilos de
            // otros clientes. Cada hilo lee de su propio socket.
            while (true) {
                // ===== MARSHALLING: Deserialización del objeto Mensaje =====
                // readObject() deserializa los bytes del stream TCP de vuelta
                // a un objeto Mensaje completo, incluyendo todos sus campos
                // (emisor, receptor, tipo, contenido, datosAdjuntos, etc.)
                Mensaje mensaje = (Mensaje) entrada.readObject();

                System.out.println("[HILO-" + nombreUsuario + "] Recibido: " + mensaje);

                // Procesar según el tipo de mensaje
                switch (mensaje.getTipo()) {
                    case Mensaje.TEXTO:
                        // ===== FUNCIÓN 1: MENSAJERÍA DE TEXTO =====
                        // TRANSPARENCIA DE ACCESO: El cliente envía un objeto
                        // Mensaje con el nombre del destinatario. No necesita
                        // conocer la dirección IP ni el socket del destinatario.
                        // El servidor resuelve la ubicación internamente.
                        Servidor.reenviarMensaje(mensaje);
                        break;

                    case Mensaje.ARCHIVO:
                        // ===== FUNCIÓN 2: ENVÍO DE ARCHIVOS MULTIMEDIA =====
                        // Los datos binarios del archivo viajan como byte[]
                        // dentro del objeto Mensaje serializado. El marshalling
                        // de Java maneja automáticamente la serialización de
                        // arrays de bytes de cualquier tamaño.
                        System.out.println("[HILO-" + nombreUsuario + "] Archivo recibido: " +
                                mensaje.getNombreArchivo() + " (" +
                                (mensaje.getDatosAdjuntos() != null ?
                                        mensaje.getDatosAdjuntos().length : 0) + " bytes)");
                        Servidor.reenviarMensaje(mensaje);
                        break;

                    case Mensaje.DESCONECTAR:
                        // El cliente notifica desconexión voluntaria
                        System.out.println("[HILO-" + nombreUsuario + "] Desconexión voluntaria.");
                        return; // Sale del bucle, ejecuta finally

                    default:
                        System.out.println("[HILO-" + nombreUsuario + "] Tipo de mensaje desconocido: " +
                                mensaje.getTipo());
                        break;
                }
            }

        } catch (IOException e) {
            // ===== RESILIENCIA: Manejo de fallo de red (IOException) =====
            // Esta excepción se produce cuando:
            // - El cliente se desconecta abruptamente (cierra la ventana)
            // - Se pierde la conexión de red
            // - Ocurre un error de I/O en el socket
            //
            // IMPORTANTE: Solo ESTE hilo se ve afectado. Los demás clientes
            // y el servidor continúan funcionando normalmente. Esto es lo que
            // significa "manejo de fallos independientes" en sistemas distribuidos.
            System.out.println("[HILO-" + nombreUsuario + "] Error de red (IOException): " +
                    e.getMessage());
            System.out.println("[HILO-" + nombreUsuario + "] El cliente probablemente se desconectó.");

        } catch (ClassNotFoundException e) {
            // ===== RESILIENCIA: Error de deserialización =====
            // Ocurre si el cliente envía un objeto de una clase desconocida.
            // Esto puede pasar por incompatibilidad de versiones.
            System.err.println("[HILO-" + nombreUsuario + "] Error de deserialización: " +
                    e.getMessage());

        } finally {
            // ===== RESILIENCIA: Limpieza de recursos =====
            // El bloque finally SIEMPRE se ejecuta, garantizando que:
            // 1. El cliente se remueve del mapa de clientes activos
            // 2. El socket se cierra para liberar el recurso de red
            // 3. Los demás clientes reciben la lista actualizada
            //
            // Esto previene fugas de recursos (resource leaks) y mantiene
            // la consistencia del sistema ante cualquier tipo de fallo.
            if (nombreUsuario != null) {
                Servidor.removerCliente(nombreUsuario);
            }
            try {
                if (socket != null && !socket.isClosed()) {
                    socket.close();
                    System.out.println("[HILO-" + nombreUsuario + "] Socket cerrado correctamente.");
                }
            } catch (IOException e) {
                System.err.println("[HILO-" + nombreUsuario + "] Error cerrando socket: " +
                        e.getMessage());
            }
            System.out.println("[HILO-" + nombreUsuario + "] Hilo finalizado.");
        }
    }

    /**
     * Envía un objeto Mensaje al cliente a través del stream de salida.
     * 
     * MARSHALLING (Serialización):
     * writeObject() convierte el objeto Mensaje completo a una secuencia
     * de bytes que se envían a través del socket TCP. En el extremo receptor,
     * readObject() reconstruye el objeto original.
     * 
     * SINCRONIZACIÓN:
     * El bloque synchronized en el ObjectOutputStream evita que múltiples
     * hilos escriban simultáneamente en el mismo stream, lo cual corrompería
     * los datos serializados. Esto es necesario porque un ManejadorCliente
     * puede recibir mensajes de múltiples hilos (cuando distintos usuarios
     * le envían mensajes al mismo tiempo).
     * 
     * RESILIENCIA:
     * Si falla el envío (IOException), se captura la excepción y se registra
     * el error. El hilo que llamó a este método NO se cae.
     * 
     * @param mensaje Objeto Mensaje a enviar (será serializado)
     */
    public void enviarMensaje(Mensaje mensaje) {
        try {
            // ===== SINCRONIZACIÓN del stream de salida =====
            // synchronized evita que dos hilos escriban al mismo tiempo
            // en el ObjectOutputStream, lo cual corrompería los datos.
            synchronized (salida) {
                salida.writeObject(mensaje);  // Serialización (marshalling)
                salida.flush();               // Forzar envío inmediato
                salida.reset();               // Limpiar cache del stream
                // reset() es importante: sin él, ObjectOutputStream cachea
                // referencias a objetos ya enviados y no serializa las
                // versiones actualizadas. Esto causaría que mensajes
                // posteriores lleguen con datos obsoletos.
            }
        } catch (IOException e) {
            // ===== RESILIENCIA: Fallo al enviar (IOException) =====
            // Si no se puede enviar al cliente (desconectado, error de red),
            // solo se registra el error. El servidor sigue funcionando.
            System.err.println("[HILO-" + nombreUsuario + "] Error enviando mensaje: " +
                    e.getMessage());
        }
    }

    /**
     * Devuelve el nombre de usuario asociado a este manejador.
     */
    public String getNombreUsuario() {
        return nombreUsuario;
    }
}
