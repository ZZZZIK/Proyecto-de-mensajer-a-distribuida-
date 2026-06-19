package server;

import common.Mensaje;
import node.NodoServidor;
import java.io.*;
import java.net.*;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Manejador de cada conexión de cliente en el servidor.
 * Utiliza dos hilos por cliente (lectura y escritura) y una cola
 * para manejar el envío de mensajes sin bloquear al servidor.
 * 
 * Soporta dos modos de operación:
 * 1. Modo standalone: con Servidor centralizado (compatibilidad hacia atrás)
 * 2. Modo distribuido: con NodoServidor (Entrega 2)
 */
public class ManejadorCliente implements Runnable {

    private Socket socket;
    private ObjectOutputStream salida;
    private ObjectInputStream entrada;
    private String nombreUsuario;
    private volatile boolean activo;

    // Referencia al NodoServidor (modo distribuido, null en modo standalone)
    private NodoServidor nodoServidor;

    // Cola de mensajes pendientes de enviar al cliente
    private final LinkedBlockingQueue<Mensaje> buzonMensajes = new LinkedBlockingQueue<>(1000);

    /**
     * Constructor modo standalone (compatibilidad con Servidor original).
     */
    public ManejadorCliente(Socket socket) {
        this.socket = socket;
        this.activo = true;
        this.nodoServidor = null;
    }

    /**
     * Constructor modo distribuido (con NodoServidor).
     */
    public ManejadorCliente(Socket socket, NodoServidor nodoServidor) {
        this.socket = socket;
        this.activo = true;
        this.nodoServidor = nodoServidor;
    }

    /**
     * Hilo principal: lee mensajes entrantes del socket y los procesa.
     */
    @Override
    public void run() {
        try {
            // Inicializar streams
            salida = new ObjectOutputStream(socket.getOutputStream());
            salida.flush();
            entrada = new ObjectInputStream(socket.getInputStream());

            // Iniciar hilo despachador de mensajes
            Thread hiloReceptor = new Thread(this::procesarBuzon);
            hiloReceptor.setDaemon(true);
            hiloReceptor.start();

            // Procesar autenticación: el primer mensaje debe ser LOGIN o REGISTRO
            Mensaje primerMensaje = (Mensaje) entrada.readObject();
            String tipo = primerMensaje.getTipo();
            String nombre = primerMensaje.getEmisor();
            String password = primerMensaje.getContenido();

            boolean autenticado = false;

            if (tipo.equals(Mensaje.REGISTRO)) {
                // ===== REGISTRO: Crear cuenta nueva en MySQL =====
                boolean registrado = GestorBD.registrarUsuario(nombre, password);
                if (registrado) {
                    autenticado = true;
                    logServidor(nombre, "Cuenta creada exitosamente.");
                } else {
                    // Nombre ya existe en la BD
                    Mensaje error = new Mensaje("SERVIDOR", nombre,
                            Mensaje.AUTH_FAIL,
                            "El nombre de usuario '" + nombre + "' ya está registrado. Intente con otro.");
                    enviarMensaje(error);
                    Thread.sleep(500);
                    socket.close();
                    return;
                }

            } else if (tipo.equals(Mensaje.LOGIN)) {
                // ===== LOGIN: Verificar credenciales en MySQL =====
                boolean credencialesOk = GestorBD.autenticarUsuario(nombre, password);
                if (credencialesOk) {
                    autenticado = true;
                    logServidor(nombre, "Login exitoso.");
                } else {
                    Mensaje error = new Mensaje("SERVIDOR", nombre,
                            Mensaje.AUTH_FAIL,
                            "Credenciales incorrectas. Verifique su nombre de usuario y contraseña.");
                    enviarMensaje(error);
                    Thread.sleep(500);
                    socket.close();
                    return;
                }

            } else {
                // Tipo de mensaje no reconocido como autenticación
                Mensaje error = new Mensaje("SERVIDOR", "DESCONOCIDO",
                        Mensaje.AUTH_FAIL,
                        "Error: Debe enviar LOGIN o REGISTRO como primer mensaje.");
                enviarMensaje(error);
                Thread.sleep(500);
                socket.close();
                return;
            }

            // Si se autenticó correctamente, se añade a la lista de conectados
            if (autenticado) {
                nombreUsuario = nombre;

                // Indicar en qué nodo está conectado
                String infoNodo = "";
                if (nodoServidor != null) {
                    infoNodo = " [Nodo-" + nodoServidor.getNodoId() + "]";
                }

                // Enviar confirmación de autenticación
                Mensaje bienvenida = new Mensaje("SERVIDOR", nombreUsuario,
                        Mensaje.AUTH_OK,
                        "¡Bienvenido al chat, " + nombreUsuario + "!" + infoNodo +
                        " Conexión exitosa.");
                enviarMensaje(bienvenida);

                // Registrar en el sistema correspondiente
                boolean enMemoria;
                if (nodoServidor != null) {
                    // Modo distribuido
                    enMemoria = nodoServidor.registrarClienteLocal(nombreUsuario, this);
                } else {
                    // Modo standalone
                    enMemoria = Servidor.registrarClienteEnMemoria(nombreUsuario, this);
                }

                if (!enMemoria) {
                    Mensaje error = new Mensaje("SERVIDOR", nombreUsuario,
                            Mensaje.AUTH_FAIL,
                            "El usuario '" + nombreUsuario + "' ya tiene una sesión activa.");
                    enviarMensaje(error);
                    Thread.sleep(500);
                    socket.close();
                    return;
                }

                // Enviar historial al usuario
                enviarHistorial(nombreUsuario);

                logServidor(nombreUsuario, "Hilo Enviador iniciado.");
            }

            // Bucle principal de escucha de mensajes
            while (activo) {
                Mensaje mensaje = (Mensaje) entrada.readObject();

                logServidor(nombreUsuario, "Recibido: " + mensaje);

                switch (mensaje.getTipo()) {
                    case Mensaje.TEXTO:
                        if (nodoServidor != null) {
                            // Modo distribuido: procesar via NodoServidor
                            nodoServidor.procesarMensajeUsuario(mensaje);
                        } else {
                            // Modo standalone
                            Servidor.reenviarMensaje(mensaje);
                        }
                        break;

                    case Mensaje.ARCHIVO:
                        logServidor(nombreUsuario, "Archivo recibido: " +
                                mensaje.getNombreArchivo() + " (" +
                                (mensaje.getDatosAdjuntos() != null ?
                                        mensaje.getDatosAdjuntos().length : 0) + " bytes)");
                        if (nodoServidor != null) {
                            nodoServidor.procesarMensajeUsuario(mensaje);
                        } else {
                            Servidor.reenviarMensaje(mensaje);
                        }
                        break;

                    case Mensaje.DESCONECTAR:
                        logServidor(nombreUsuario, "Desconexión voluntaria.");
                        return;

                    default:
                        logServidor(nombreUsuario, "Tipo desconocido: " + mensaje.getTipo());
                        break;
                }
            }

        } catch (IOException e) {
            logServidor(nombreUsuario, "Error de red: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            System.err.println("[MANEJADOR-" + nombreUsuario + "] Error de deserialización: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            activo = false;
            if (nombreUsuario != null) {
                if (nodoServidor != null) {
                    nodoServidor.removerClienteLocal(nombreUsuario);
                } else {
                    Servidor.removerCliente(nombreUsuario);
                }
            }
            try {
                if (socket != null && !socket.isClosed()) {
                    socket.close();
                    logServidor(nombreUsuario, "Socket cerrado.");
                }
            } catch (IOException e) {
                System.err.println("[MANEJADOR-" + nombreUsuario + "] Error cerrando socket: " + e.getMessage());
            }
            logServidor(nombreUsuario, "Hilo finalizado.");
        }
    }

    /**
     * Envía el historial y mensajes offline al usuario que se conecta.
     */
    private void enviarHistorial(String nombre) {
        // 1. Enviar historial permanente
        java.util.List<Mensaje> historial = GestorBD.obtenerHistorialUsuario(nombre);
        if (!historial.isEmpty()) {
            Mensaje avisoHistorial = new Mensaje("SERVIDOR", nombre,
                    Mensaje.NOTIFICACION,
                    "📜 Restaurando tu historial de conversaciones (" + historial.size() + " mensajes):");
            enviarMensaje(avisoHistorial);
            for (Mensaje msgHistorial : historial) {
                enviarMensaje(msgHistorial);
            }
        }

        // 2. Enviar mensajes offline
        java.util.List<Mensaje> pendientes = GestorBD.obtenerYBorrarMensajesOffline(nombre);
        if (!pendientes.isEmpty()) {
            Mensaje avisoOffline = new Mensaje("SERVIDOR", nombre,
                    Mensaje.NOTIFICACION,
                    "📬 Tienes " + pendientes.size() + " mensaje(s) nuevo(s) mientras estabas desconectado:");
            enviarMensaje(avisoOffline);
            for (Mensaje msgPendiente : pendientes) {
                enviarMensaje(msgPendiente);
                GestorBD.guardarMensajeHistorial(msgPendiente);
            }
        }

        // 3. Enviar listas de usuarios
        if (nodoServidor != null) {
            nodoServidor.enviarListaUsuarios();
        } else {
            Servidor.enviarListaUsuarios();
            Servidor.enviarListaOffline();
        }

        // 4. Notificar a todos
        String infoNodo = (nodoServidor != null) ?
                " (Nodo-" + nodoServidor.getNodoId() + ")" : "";
        if (nodoServidor != null) {
            nodoServidor.enviarNotificacion(nombre + " se ha conectado al chat." + infoNodo);
        } else {
            Servidor.enviarNotificacion(nombre + " se ha conectado al chat.");
        }
    }

    /**
     * Hilo despachador: extrae mensajes de la cola y los envía por el socket.
     */
    private void procesarBuzon() {
        try {
            while (activo) {
                Mensaje mensaje = buzonMensajes.take();

                salida.writeObject(mensaje);
                salida.flush();
                salida.reset();
            }
        } catch (IOException e) {
            if (activo) {
                System.err.println("[MANEJADOR-" + nombreUsuario +
                        "] Error enviando por red: " + e.getMessage());
                try {
                    if (socket != null && !socket.isClosed()) {
                        socket.close();
                    }
                } catch (IOException ex) {
                    // Ignorar
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Deposita un mensaje en el buzón para ser enviado al cliente.
     * Operación instantánea en RAM (nunca se bloquea).
     */
    public void enviarMensaje(Mensaje mensaje) {
        boolean aceptado = buzonMensajes.offer(mensaje);
        if (!aceptado) {
            System.err.println("[MANEJADOR-" + (nombreUsuario != null ? nombreUsuario : "NO_AUTH") +
                    "] ADVERTENCIA: Buzón en memoria lleno (límite 1000). Mensaje descartado.");
        }
    }

    /**
     * Devuelve el nombre de usuario asociado a este manejador.
     */
    public String getNombreUsuario() {
        return nombreUsuario;
    }

    /**
     * Log helper que usa el sistema apropiado según el modo.
     */
    private void logServidor(String usuario, String mensaje) {
        String nombre = (usuario != null) ? usuario : "NO_AUTH";
        if (nodoServidor != null) {
            nodoServidor.getLog().registrar(node.LogDistribuido.SISTEMA, null,
                    "[MANEJADOR-" + nombre + "] " + mensaje);
        } else {
            System.out.println("[MANEJADOR-" + nombre + "] " + mensaje);
        }
    }
}

