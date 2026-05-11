package server;

import common.Mensaje;
import java.io.*;
import java.net.*;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Manejador de cada conexión de cliente en el servidor.
 * Utiliza dos hilos por cliente (lectura y escritura) y una cola
 * para manejar el envío de mensajes sin bloquear al servidor.
 */
public class ManejadorCliente implements Runnable {

    private Socket socket;
    private ObjectOutputStream salida;
    private ObjectInputStream entrada;
    private String nombreUsuario;
    private volatile boolean activo;

    // Cola de mensajes pendientes de enviar al cliente
    private final LinkedBlockingQueue<Mensaje> buzonMensajes = new LinkedBlockingQueue<>(1000);

    public ManejadorCliente(Socket socket) {
        this.socket = socket;
        this.activo = true;
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
                    System.out.println("[HILO-ENVIADOR-" + nombre + "] Cuenta creada exitosamente.");
                } else {
                    // Nombre ya existe en la BD
                    Mensaje error = new Mensaje("SERVIDOR", nombre,
                            Mensaje.AUTH_FAIL,
                            "El nombre de usuario '" + nombre + "' ya está registrado. Intente con otro.");
                    enviarMensaje(error);
                    Thread.sleep(500); // Dar tiempo al hilo receptor para enviar
                    socket.close();
                    return;
                }

            } else if (tipo.equals(Mensaje.LOGIN)) {
                // ===== LOGIN: Verificar credenciales en MySQL =====
                boolean credencialesOk = GestorBD.autenticarUsuario(nombre, password);
                if (credencialesOk) {
                    autenticado = true;
                    System.out.println("[HILO-ENVIADOR-" + nombre + "] Login exitoso.");
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

                // Enviar confirmación de autenticación
                Mensaje bienvenida = new Mensaje("SERVIDOR", nombreUsuario,
                        Mensaje.AUTH_OK,
                        "¡Bienvenido al chat, " + nombreUsuario + "! Conexión exitosa.");
                enviarMensaje(bienvenida);

                boolean enMemoria = Servidor.registrarClienteEnMemoria(nombreUsuario, this);
                if (!enMemoria) {
                    Mensaje error = new Mensaje("SERVIDOR", nombreUsuario,
                            Mensaje.AUTH_FAIL,
                            "El usuario '" + nombreUsuario + "' ya tiene una sesión activa.");
                    enviarMensaje(error);
                    Thread.sleep(500);
                    socket.close();
                    return;
                }

                System.out.println("[HILO-ENVIADOR-" + nombreUsuario + "] Hilo Enviador iniciado.");
            }

            // Bucle principal de escucha de mensajes
            while (activo) {
                Mensaje mensaje = (Mensaje) entrada.readObject();

                System.out.println("[HILO-ENVIADOR-" + nombreUsuario + "] Recibido: " + mensaje);

                switch (mensaje.getTipo()) {
                    case Mensaje.TEXTO:
                        Servidor.reenviarMensaje(mensaje);
                        break;

                    case Mensaje.ARCHIVO:
                        System.out.println("[HILO-ENVIADOR-" + nombreUsuario + "] Archivo recibido: " +
                                mensaje.getNombreArchivo() + " (" +
                                (mensaje.getDatosAdjuntos() != null ?
                                        mensaje.getDatosAdjuntos().length : 0) + " bytes)");
                        Servidor.reenviarMensaje(mensaje);
                        break;

                    case Mensaje.DESCONECTAR:
                        System.out.println("[HILO-ENVIADOR-" + nombreUsuario + "] Desconexión voluntaria.");
                        return;

                    default:
                        System.out.println("[HILO-ENVIADOR-" + nombreUsuario + "] Tipo desconocido: " +
                                mensaje.getTipo());
                        break;
                }
            }

        } catch (IOException e) {
            System.out.println("[HILO-ENVIADOR-" + nombreUsuario + "] Error de red: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            System.err.println("[HILO-ENVIADOR-" + nombreUsuario + "] Error de deserialización: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            activo = false;
            if (nombreUsuario != null) {
                Servidor.removerCliente(nombreUsuario);
            }
            try {
                if (socket != null && !socket.isClosed()) {
                    socket.close();
                    System.out.println("[HILO-ENVIADOR-" + nombreUsuario + "] Socket cerrado.");
                }
            } catch (IOException e) {
                System.err.println("[HILO-ENVIADOR-" + nombreUsuario + "] Error cerrando socket: " + e.getMessage());
            }
            System.out.println("[HILO-ENVIADOR-" + nombreUsuario + "] Hilo finalizado.");
        }
    }

    /**
     * Hilo despachador: extrae mensajes de la cola y los envía por el socket.
     */
    private void procesarBuzon() {
        System.out.println("[HILO-RECEPTOR-" + nombreUsuario + "] Hilo Receptor iniciado.");

        try {
            while (activo) {
                Mensaje mensaje = buzonMensajes.take();

                salida.writeObject(mensaje);
                salida.flush();
                salida.reset();
            }
        } catch (IOException e) {
            if (activo) {
                System.err.println("[HILO-RECEPTOR-" + nombreUsuario +
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

        System.out.println("[HILO-RECEPTOR-" + nombreUsuario + "] Hilo Receptor finalizado.");
    }

    /**
     * Deposita un mensaje en el buzón para ser enviado al cliente.
     * Operación instantánea en RAM (nunca se bloquea).
     */
    public void enviarMensaje(Mensaje mensaje) {
        boolean aceptado = buzonMensajes.offer(mensaje);
        if (!aceptado) {
            System.err.println("[HILO-ENVIADOR-" + (nombreUsuario != null ? nombreUsuario : "NO_AUTENTICADO") + 
                    "] ADVERTENCIA: Buzón en memoria lleno (límite 1000). Mensaje descartado.");
        }
    }

    /**
     * Devuelve el nombre de usuario asociado a este manejador.
     */
    public String getNombreUsuario() {
        return nombreUsuario;
    }
}
