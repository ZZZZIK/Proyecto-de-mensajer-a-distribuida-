package server;

import common.Mensaje;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.List;

/**
 * Servidor principal de la aplicación de mensajería.
 * Maneja las conexiones entrantes y mantiene un registro de los clientes conectados.
 */
public class Servidor {

    // Puerto por defecto del servidor
    private static final int PUERTO_DEFECTO = 5000;

    /**
     * Mapa de clientes actualmente conectados en memoria.
     * Llave: nombre de usuario, Valor: manejador del cliente.
     */
    private static final HashMap<String, ManejadorCliente> clientesConectados = new HashMap<>();

    /**
     * Punto de entrada del servidor.
     * Verifica la conexión a MySQL y luego acepta conexiones de clientes.
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

        // Verificar conexión a la base de datos antes de iniciar
        System.out.println("============================================");
        System.out.println("  SERVIDOR WHATSAPP DISTRIBUIDO");
        System.out.println("============================================");
        System.out.println("[SERVIDOR] Verificando conexión a MySQL...");

        if (!GestorBD.verificarConexion()) {
            System.err.println("[SERVIDOR] No se pudo conectar a MySQL. Abortando.");
            System.err.println("[SERVIDOR] Asegúrate de:");
            System.err.println("  1. Tener XAMPP abierto con MySQL encendido.");
            System.err.println("  2. Haber ejecutado el script SQL (database/chat_distribuido.sql).");
            return;
        }

        ServerSocket serverSocket = null;

        try {
            serverSocket = new ServerSocket(puerto);

            System.out.println("[SERVIDOR] Puerto: " + puerto);
            System.out.println("[SERVIDOR] Base de datos: chat_distribuido (MySQL)");
            System.out.println("[SERVIDOR] Esperando conexiones de clientes...");
            System.out.println("============================================");

            while (true) {
                Socket socketCliente = serverSocket.accept();

                System.out.println("[SERVIDOR] Nueva conexión desde: " +
                        socketCliente.getInetAddress().getHostAddress() + ":" +
                        socketCliente.getPort());

                ManejadorCliente manejador = new ManejadorCliente(socketCliente);
                Thread hiloCliente = new Thread(manejador);
                hiloCliente.setDaemon(true);
                hiloCliente.start();
            }

        } catch (IOException e) {
            System.err.println("[SERVIDOR] Error fatal del servidor: " + e.getMessage());
            e.printStackTrace();
        } finally {
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


    /**
     * Registra a un usuario recién conectado en la memoria del servidor.
     * Envía primero el historial y luego los mensajes no leídos.
     * 
     * @param nombre Nombre de usuario
     * @param manejador Instancia del hilo que maneja al cliente
     * @return true si se registró exitosamente
     */
    public static boolean registrarClienteEnMemoria(String nombre, ManejadorCliente manejador) {
        synchronized (clientesConectados) {
            if (clientesConectados.containsKey(nombre)) {
                System.out.println("[SERVIDOR] Registro rechazado: '" + nombre + "' ya está conectado.");
                return false;
            }
            clientesConectados.put(nombre, manejador);
            System.out.println("[SERVIDOR] Cliente conectado: " + nombre +
                    " (Total online: " + clientesConectados.size() + ")");
        }

        // 1. Enviar historial de conversaciones permanentes
        List<Mensaje> historial = GestorBD.obtenerHistorialUsuario(nombre);

        if (!historial.isEmpty()) {
            Mensaje avisoHistorial = new Mensaje("SERVIDOR", nombre,
                    Mensaje.NOTIFICACION,
                    "📜 Restaurando tu historial de conversaciones (" + historial.size() + " mensajes):");
            manejador.enviarMensaje(avisoHistorial);

            for (Mensaje msgHistorial : historial) {
                manejador.enviarMensaje(msgHistorial);
            }
            System.out.println("[SERVIDOR] Historial enviado a '" + nombre + "': " + historial.size() + " mensajes.");
        }

        // 2. Enviar y borrar mensajes del buzón offline
        List<Mensaje> mensajesPendientes = GestorBD.obtenerYBorrarMensajesOffline(nombre);

        if (!mensajesPendientes.isEmpty()) {
            Mensaje avisoOffline = new Mensaje("SERVIDOR", nombre,
                    Mensaje.NOTIFICACION,
                    "📬 Tienes " + mensajesPendientes.size() +
                    " mensaje(s) nuevo(s) que recibiste mientras estabas desconectado:");
            manejador.enviarMensaje(avisoOffline);

            for (Mensaje msgPendiente : mensajesPendientes) {
                manejador.enviarMensaje(msgPendiente);
                // Ahora que se entregó, guardarlo en historial permanente
                GestorBD.guardarMensajeHistorial(msgPendiente);
            }
            System.out.println("[SERVIDOR] Mensajes offline entregados a '" + nombre + "' y guardados en historial.");
        }

        // Notificar a todos los clientes las listas actualizadas
        enviarListaUsuarios();
        enviarListaOffline();
        enviarNotificacion(nombre + " se ha conectado al chat.");
        return true;
    }

    /**
     * Elimina un cliente del sistema (desconexión o fallo).
     */
    public static void removerCliente(String nombre) {
        synchronized (clientesConectados) {
            clientesConectados.remove(nombre);
            System.out.println("[SERVIDOR] Cliente removido: " + nombre +
                    " (Total online: " + clientesConectados.size() + ")");
        }
        enviarListaUsuarios();
        enviarListaOffline();
        enviarNotificacion(nombre + " se ha desconectado.");
    }

    /**
     * Intenta reenviar un mensaje a su destinatario.
     * Si está conectado, se le envía. Si no, se guarda en su buzón offline.
     */
    public static void reenviarMensaje(Mensaje mensaje) {
        String receptor = mensaje.getReceptor();
        ManejadorCliente emisor = null;

        synchronized (clientesConectados) {
            ManejadorCliente destino = clientesConectados.get(receptor);

            if (destino != null) {
                // Destinatario conectado: enviar y guardar en historial
                destino.enviarMensaje(mensaje);
                System.out.println("[SERVIDOR] Mensaje reenviado: " + mensaje.getEmisor() +
                        " -> " + receptor + " [" + mensaje.getTipo() + "]");

                // Guardar en historial permanente
                GestorBD.guardarMensajeHistorial(mensaje);
                return;
            }

            emisor = clientesConectados.get(mensaje.getEmisor());
        }

        // Validar si el usuario existe en BD
        if (!GestorBD.usuarioExiste(receptor)) {
            if (emisor != null) {
                Mensaje error = new Mensaje("SERVIDOR", mensaje.getEmisor(),
                        Mensaje.NOTIFICACION,
                        "❌ El usuario '" + receptor + "' no existe en el chat.");
                emisor.enviarMensaje(error);
            }
            System.out.println("[SERVIDOR] Usuario '" + receptor +
                    "' no existe. Mensaje de '" + mensaje.getEmisor() + "' rechazado.");
            return;
        }

        // Guardar mensaje para cuando se conecte
        int resultado = GestorBD.guardarMensajeOffline(mensaje);

        if (resultado == 1) {
            if (emisor != null) {
                Mensaje error = new Mensaje("SERVIDOR", mensaje.getEmisor(),
                        Mensaje.NOTIFICACION,
                        "⚠ El buzón offline de '" + receptor +
                        "' está lleno (Máx 100). Mensaje no guardado.");
                emisor.enviarMensaje(error);
            }
            return;
        } else if (resultado == 2) {
            // Error de almacenamiento
            if (emisor != null) {
                Mensaje error = new Mensaje("SERVIDOR", mensaje.getEmisor(),
                        Mensaje.NOTIFICACION,
                        "❌ Error al guardar el mensaje offline. Intente de nuevo.");
                emisor.enviarMensaje(error);
            }
            return;
        }

        // Notificar al emisor que se guardó en offline
        if (emisor != null) {
            Mensaje aviso = new Mensaje("SERVIDOR", mensaje.getEmisor(),
                    Mensaje.NOTIFICACION,
                    "📩 '" + receptor + "' está desconectado. Tu mensaje se ha guardado.");
            emisor.enviarMensaje(aviso);
        }
    }

    /**
     * Envía la lista actualizada de usuarios conectados a TODOS los clientes.
     */
    public static void enviarListaUsuarios() {
        List<ManejadorCliente> clientesCopia;
        Mensaje msgLista;

        synchronized (clientesConectados) {
            String lista = String.join(",", clientesConectados.keySet());
            msgLista = new Mensaje("SERVIDOR", "TODOS",
                    Mensaje.LISTA_USUARIOS, lista);
            clientesCopia = new ArrayList<>(clientesConectados.values());
        }
        for (ManejadorCliente cliente : clientesCopia) {
            cliente.enviarMensaje(msgLista);
        }
    }

    /**
     * Envía una notificación del sistema a todos los clientes conectados.
     */
    public static void enviarNotificacion(String texto) {
        List<ManejadorCliente> clientesCopia;
        Mensaje notificacion;

        synchronized (clientesConectados) {
            notificacion = new Mensaje("SERVIDOR", "TODOS",
                    Mensaje.NOTIFICACION, texto);
            clientesCopia = new ArrayList<>(clientesConectados.values());
        }
        for (ManejadorCliente cliente : clientesCopia) {
            cliente.enviarMensaje(notificacion);
        }
    }

    /**
     * Envía la lista de usuarios desconectados (offline) a todos los clientes.
     */
    public static void enviarListaOffline() {
        List<ManejadorCliente> clientesCopia;
        Mensaje msgOffline;

        List<String> todosRegistrados = GestorBD.obtenerUsuariosRegistrados();

        synchronized (clientesConectados) {
            List<String> offline = new ArrayList<>();
            for (String registrado : todosRegistrados) {
                if (!clientesConectados.containsKey(registrado)) {
                    offline.add(registrado);
                }
            }

            String listaOffline = String.join(",", offline);
            msgOffline = new Mensaje("SERVIDOR", "TODOS",
                    Mensaje.LISTA_OFFLINE, listaOffline);
            clientesCopia = new ArrayList<>(clientesConectados.values());
        }
        for (ManejadorCliente cliente : clientesCopia) {
            cliente.enviarMensaje(msgOffline);
        }
    }
}
