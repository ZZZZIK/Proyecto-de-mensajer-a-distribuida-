package server;

import common.Mensaje;
import java.io.*;
import java.net.*;
import java.util.*;

/**
 * =====================================================================
 * SERVIDOR CENTRAL DEL SISTEMA DISTRIBUIDO
 * =====================================================================
 * 
 * TRANSPARENCIA DE UBICACIÓN:
 * El servidor actúa como punto central de comunicación (arquitectura
 * cliente-servidor). Los clientes NO necesitan conocer la dirección IP
 * ni el puerto de otros clientes. Solo conocen la dirección del servidor.
 * El servidor resuelve internamente la ubicación de cada destinatario
 * consultando su mapa de clientes conectados. Esto implementa
 * transparencia de ubicación: el cliente envía un mensaje indicando
 * solo el NOMBRE del destinatario, sin saber dónde está físicamente.
 * 
 * TRANSPARENCIA DE ACCESO:
 * El cliente interactúa con el sistema enviando objetos Mensaje de forma
 * uniforme, sin importar si el destinatario está en la misma máquina,
 * en la misma red, o en un host remoto. La interfaz de acceso es
 * siempre la misma: crear un Mensaje y enviarlo por el socket.
 * 
 * CONCURRENCIA — Modelo "Un hilo por cliente":
 * Por cada conexión entrante, el servidor crea una nueva instancia de
 * ManejadorCliente y la ejecuta en un Thread independiente. Esto permite
 * atender a múltiples clientes de forma simultánea y concurrente.
 * 
 * SINCRONIZACIÓN:
 * La lista de clientes conectados (HashMap) es un recurso compartido
 * accedido por múltiples hilos concurrentemente. Se protege con bloques
 * synchronized para evitar condiciones de carrera (race conditions).
 * 
 * RESILIENCIA — Manejo de fallos independientes:
 * Si un cliente se desconecta o su hilo falla, el servidor no se cae.
 * Solo el hilo afectado termina, los demás clientes continúan operando.
 * El servidor limpia los recursos del cliente desconectado y notifica
 * a los demás usuarios.
 * =====================================================================
 */
public class Servidor {

    // Puerto por defecto del servidor
    private static final int PUERTO_DEFECTO = 5000;

    /**
     * ===== SINCRONIZACIÓN: Recurso compartido protegido =====
     * 
     * Este HashMap almacena todos los clientes conectados, mapeando
     * el nombre de usuario a su ManejadorCliente correspondiente.
     * 
     * Es accedido por MÚLTIPLES HILOS concurrentemente (cada ManejadorCliente
     * puede registrar, remover o buscar clientes). Por eso, todos los métodos
     * que acceden a este mapa están protegidos con synchronized.
     * 
     * Se usa synchronized en vez de ConcurrentHashMap para demostrar
     * explícitamente el mecanismo de sincronización requerido.
     */
    private static final HashMap<String, ManejadorCliente> clientesConectados = new HashMap<>();

    /**
     * Punto de entrada del servidor.
     * Crea un ServerSocket TCP y entra en un bucle infinito aceptando conexiones.
     * 
     * @param args Opcionalmente el puerto como primer argumento
     */
    public static void main(String[] args) {
        int puerto = PUERTO_DEFECTO;
        if (args.length > 0) {
            try {
                puerto = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("[SERVIDOR] Puerto inválido, usando " + PUERTO_DEFECTO);
            }
        }

        ServerSocket serverSocket = null;

        try {
            // ===== COMUNICACIÓN TCP: Creación del ServerSocket =====
            // ServerSocket escucha conexiones entrantes en el puerto especificado.
            // Esto establece el punto de entrada del sistema distribuido.
            serverSocket = new ServerSocket(puerto);

            System.out.println("SERVIDOR WHATSAPP DISTRIBUIDO INICIADO");
            System.out.println("Puerto: " + puerto);
            System.out.println("Esperando conexiones de clientes...");


            // Bucle principal: acepta conexiones indefinidamente
            while (true) {
                // ===== COMUNICACIÓN TCP: Aceptar conexión entrante =====
                // accept() es BLOQUEANTE: el hilo principal se detiene aquí
                // hasta que un nuevo cliente se conecte via Socket.
                Socket socketCliente = serverSocket.accept();

                System.out.println("[SERVIDOR] Nueva conexión desde: " +
                        socketCliente.getInetAddress().getHostAddress() + ":" +
                        socketCliente.getPort());

                // ===== CONCURRENCIA: Un hilo por cliente =====
                // Se crea una nueva instancia de ManejadorCliente (Runnable)
                // y se lanza en un Thread independiente. Cada cliente tiene
                // su propio hilo de ejecución, permitiendo atender a todos
                // de forma concurrente.
                ManejadorCliente manejador = new ManejadorCliente(socketCliente);
                Thread hiloCliente = new Thread(manejador);
                hiloCliente.setDaemon(true); // Hilo daemon: termina cuando el servidor termina
                hiloCliente.start();
            }

        } catch (IOException e) {
            // ===== RESILIENCIA: Manejo de fallo fatal del servidor =====
            // Si el ServerSocket falla (puerto ocupado, error de red, etc.),
            // se captura la excepción y se muestra un mensaje de error claro.
            System.err.println("[SERVIDOR] Error fatal del servidor: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Cierre del ServerSocket en caso de error
            if (serverSocket != null && !serverSocket.isClosed()) {
                try {
                    serverSocket.close();
                    System.out.println("[SERVIDOR] ServerSocket cerrado.");
                } catch (IOException e) {
                    System.err.println("[SERVIDOR] Error cerrando ServerSocket: " + e.getMessage());
                }
            }
        }
    }

    // =====================================================================
    // MÉTODOS SINCRONIZADOS PARA GESTIÓN DE CLIENTES
    // Todos estos métodos usan synchronized para proteger el HashMap
    // compartido de accesos concurrentes desde múltiples hilos.
    // =====================================================================

    /**
     * Registra un nuevo cliente en el sistema.
     * 
     * SINCRONIZACIÓN: El bloque synchronized garantiza que solo un hilo
     * a la vez pueda modificar el mapa de clientes, evitando condiciones
     * de carrera cuando múltiples clientes se conectan simultáneamente.
     * 
     * @param nombre    Nombre de usuario del cliente
     * @param manejador Instancia del ManejadorCliente asociado
     * @return true si el registro fue exitoso, false si el nombre ya existe
     */
    public static boolean registrarCliente(String nombre, ManejadorCliente manejador) {
        synchronized (clientesConectados) {
            // Verificar si el nombre de usuario ya está en uso
            if (clientesConectados.containsKey(nombre)) {
                System.out.println("[SERVIDOR] Registro rechazado: '" + nombre + "' ya está conectado.");
                return false;
            }
            clientesConectados.put(nombre, manejador);
            System.out.println("[SERVIDOR] Cliente registrado: " + nombre +
                    " (Total conectados: " + clientesConectados.size() + ")");
        }
        // Notificar a todos los clientes la lista actualizada de usuarios
        enviarListaUsuarios();
        // Notificar a todos que un nuevo usuario se conectó
        enviarNotificacion(nombre + " se ha conectado al chat.");
        return true;
    }

    /**
     * Elimina un cliente del sistema (desconexión o fallo).
     * 
     * RESILIENCIA: Este método se llama tanto en desconexiones voluntarias
     * como en fallos de red (IOException en el ManejadorCliente). El sistema
     * limpia los recursos y continúa operando normalmente.
     * 
     * @param nombre Nombre del usuario a remover
     */
    public static void removerCliente(String nombre) {
        synchronized (clientesConectados) {
            clientesConectados.remove(nombre);
            System.out.println("[SERVIDOR] Cliente removido: " + nombre +
                    " (Total conectados: " + clientesConectados.size() + ")");
        }
        // Notificar a los demás la lista actualizada y la desconexión
        enviarListaUsuarios();
        enviarNotificacion(nombre + " se ha desconectado.");
    }

    /**
     * Reenvía un mensaje al destinatario correcto.
     * 
     * TRANSPARENCIA DE UBICACIÓN: El emisor solo indica el NOMBRE del
     * destinatario en el campo "receptor" del Mensaje. Este método
     * resuelve internamente la ubicación del destinatario buscándolo
     * en el mapa de clientes conectados y reenviando el mensaje a
     * través de su ManejadorCliente. El emisor nunca necesita conocer
     * la IP o el puerto del destinatario.
     * 
     * TRANSPARENCIA DE ACCESO: El mecanismo de envío es idéntico
     * independientemente de dónde se encuentre el destinatario
     * (misma máquina, misma red, red remota).
     * 
     * @param mensaje Objeto Mensaje a reenviar
     */
    public static void reenviarMensaje(Mensaje mensaje) {
        String receptor = mensaje.getReceptor();

        synchronized (clientesConectados) {
            // Buscar el destinatario en el mapa de clientes conectados
            ManejadorCliente destino = clientesConectados.get(receptor);

            if (destino != null) {
                // ===== Destinatario encontrado: reenviar el mensaje =====
                destino.enviarMensaje(mensaje);
                System.out.println("[SERVIDOR] Mensaje reenviado: " + mensaje.getEmisor() +
                        " -> " + receptor + " [" + mensaje.getTipo() + "]");
            } else {
                // ===== RESILIENCIA: Destinatario no encontrado =====
                // Si el destinatario no está conectado, se envía una
                // notificación de error al emisor. El sistema NO se cae.
                ManejadorCliente emisor = clientesConectados.get(mensaje.getEmisor());
                if (emisor != null) {
                    Mensaje error = new Mensaje("SERVIDOR", mensaje.getEmisor(),
                            Mensaje.NOTIFICACION,
                            "El usuario '" + receptor + "' no está conectado.");
                    emisor.enviarMensaje(error);
                }
                System.out.println("[SERVIDOR] Destinatario no encontrado: " + receptor);
            }
        }
    }

    /**
     * Envía la lista actualizada de usuarios conectados a TODOS los clientes.
     * 
     * Se envía como un Mensaje de tipo LISTA_USUARIOS, donde el contenido
     * es una cadena con los nombres separados por comas.
     */
    public static void enviarListaUsuarios() {
        synchronized (clientesConectados) {
            // Construir la lista de nombres separados por coma
            String lista = String.join(",", clientesConectados.keySet());
            Mensaje msgLista = new Mensaje("SERVIDOR", "TODOS",
                    Mensaje.LISTA_USUARIOS, lista);

            // Enviar a cada cliente conectado
            for (ManejadorCliente cliente : clientesConectados.values()) {
                cliente.enviarMensaje(msgLista);
            }
        }
    }

    /**
     * Envía una notificación del sistema a todos los clientes conectados.
     * 
     * @param texto Texto de la notificación
     */
    public static void enviarNotificacion(String texto) {
        synchronized (clientesConectados) {
            Mensaje notificacion = new Mensaje("SERVIDOR", "TODOS",
                    Mensaje.NOTIFICACION, texto);

            for (ManejadorCliente cliente : clientesConectados.values()) {
                cliente.enviarMensaje(notificacion);
            }
        }
    }
}
