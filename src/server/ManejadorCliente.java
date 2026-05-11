package server;

import common.Mensaje;
import java.io.*;
import java.net.*;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * =====================================================================
 * MANEJADOR DE CLIENTE — Arquitectura de 2 Hilos + Buzón + Autenticación
 * =====================================================================
 * 
 * CONCURRENCIA — Dos hilos por cliente:
 * Cada cliente conectado tiene DOS hilos independientes:
 * 
 *   1. HILO ENVIADOR (método run()):
 *      - Escucha los mensajes que llegan desde la red del cliente.
 *      - Cuando recibe un mensaje destinado a otro usuario, lo deposita
 *        en el BUZÓN (LinkedBlockingQueue) del destinatario.
 *      - NUNCA toca el ObjectOutputStream de otro cliente directamente.
 * 
 *   2. HILO RECEPTOR (método procesarBuzon()):
 *      - Vigila constantemente su propio Buzón (Queue).
 *      - Cuando cae un mensaje en el buzón, lo saca y lo envía por
 *        la red hacia el cliente usando ObjectOutputStream.
 *      - Si la red del cliente es lenta, SOLO ESTE HILO se bloquea.
 * 
 * AUTENTICACIÓN:
 * El primer mensaje del cliente DEBE ser de tipo LOGIN o REGISTRO.
 * El ManejadorCliente delega la validación a GestorBD (MySQL).
 * Si la autenticación falla, se rechaza la conexión inmediatamente.
 * 
 * PATRÓN MESSAGE QUEUE (BUZÓN):
 * El buzón (LinkedBlockingQueue) actúa como amortiguador entre la
 * velocidad a la que otros usuarios generan mensajes y la velocidad
 * a la que la red de ESTE cliente puede consumirlos.
 * =====================================================================
 */
public class ManejadorCliente implements Runnable {

    private Socket socket;
    private ObjectOutputStream salida;
    private ObjectInputStream entrada;
    private String nombreUsuario;
    private volatile boolean activo;

    /**
     * ===== PATRÓN MESSAGE QUEUE: El Buzón de mensajes =====
     */
    private final LinkedBlockingQueue<Mensaje> buzonMensajes = new LinkedBlockingQueue<>();

    public ManejadorCliente(Socket socket) {
        this.socket = socket;
        this.activo = true;
    }

    /**
     * ===== HILO ENVIADOR — Método principal del hilo =====
     * 
     * Flujo:
     * 1. Crear streams de objetos (marshalling)
     * 2. Iniciar el Hilo Receptor (procesarBuzon) en segundo plano
     * 3. Leer el primer mensaje (LOGIN o REGISTRO) para autenticar
     * 4. Si la autenticación es exitosa, registrar en memoria
     * 5. Entrar en bucle escuchando mensajes del cliente
     * 6. Al finalizar, limpiar recursos
     */
    @Override
    public void run() {
        try {
            // ===== MARSHALLING: Inicialización de streams =====
            salida = new ObjectOutputStream(socket.getOutputStream());
            salida.flush();
            entrada = new ObjectInputStream(socket.getInputStream());

            // ===== CONCURRENCIA: Iniciar Hilo Receptor =====
            Thread hiloReceptor = new Thread(this::procesarBuzon);
            hiloReceptor.setDaemon(true);
            hiloReceptor.start();

            // ===== PASO 1: AUTENTICACIÓN =====
            // El primer mensaje del cliente debe ser LOGIN o REGISTRO.
            // La contraseña viaja en el campo "contenido" del Mensaje.
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

            // ===== PASO 2: REGISTRAR EN MEMORIA =====
            if (autenticado) {
                nombreUsuario = nombre;

                // Enviar AUTH_OK PRIMERO, ANTES de registrar en memoria.
                // registrarClienteEnMemoria() envía listas de usuarios y notificaciones
                // al buzón. Si AUTH_OK se enviara después, el cliente recibiría una
                // LISTA_USUARIOS como primer mensaje en vez de AUTH_OK, y no abriría
                // la ventana del chat.
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

            // ===== PASO 3: Bucle principal de lectura de mensajes =====
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
     * ===== HILO RECEPTOR — Procesa el buzón y envía por la red =====
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
        buzonMensajes.offer(mensaje);
    }

    /**
     * Devuelve el nombre de usuario asociado a este manejador.
     */
    public String getNombreUsuario() {
        return nombreUsuario;
    }
}
