package server;

import common.Mensaje;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.List;

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
 * CONCURRENCIA — Modelo "Dos hilos por cliente":
 * Por cada conexión entrante, el servidor crea una nueva instancia de
 * ManejadorCliente con DOS hilos independientes: un Hilo Enviador que
 * escucha al cliente, y un Hilo Receptor que despacha mensajes desde
 * un buzón (LinkedBlockingQueue) hacia la red del cliente.
 * 
 * PATRÓN MESSAGE QUEUE (BUZÓN):
 * Los métodos de broadcast (enviarListaUsuarios, enviarNotificacion)
 * depositan mensajes en los buzones de cada cliente en lugar de enviarlos
 * directamente por la red. Esto permite que el candado global (synchronized)
 * se mantenga solo por microsegundos (operaciones en RAM), eliminando el
 * problema del "Consumidor Lento" donde un cliente con lag congelaba
 * todo el servidor.
 * 
 * PERSISTENCIA (MySQL via GestorBD):
 * El registro de usuarios y los mensajes offline ahora se almacenan en
 * una base de datos MySQL en lugar de variables en RAM. Esto permite que
 * los datos sobrevivan reinicios del servidor y escalen a gran volumen.
 * La clase GestorBD (Patrón DAO) encapsula toda la lógica JDBC.
 * 
 * AUTENTICACIÓN:
 * Los clientes deben iniciar sesión (LOGIN) o registrarse (REGISTRO)
 * con nombre de usuario y contraseña antes de poder chatear.
 * 
 * SINCRONIZACIÓN:
 * La lista de clientes conectados (HashMap) es un recurso compartido
 * accedido por múltiples hilos concurrentemente. Se protege con bloques
 * synchronized para evitar condiciones de carrera (race conditions).
 * El candado se mantiene solo durante operaciones en RAM (buscar, copiar)
 * y NUNCA durante operaciones de red, garantizando que jamás se bloquee.
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
     * Este HashMap almacena los clientes ACTUALMENTE CONECTADOS, mapeando
     * el nombre de usuario a su ManejadorCliente correspondiente.
     * 
     * NOTA: Este es el ÚNICO recurso en RAM. El registro permanente de
     * usuarios y los mensajes offline ahora viven en MySQL (GestorBD).
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

        // ===== PERSISTENCIA: Verificar conexión a MySQL =====
        // Antes de aceptar clientes, verificamos que la BD esté disponible.
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

    // =====================================================================
    // MÉTODOS SINCRONIZADOS PARA GESTIÓN DE CLIENTES EN MEMORIA
    // Estos métodos gestionan SOLO la lista de clientes conectados (RAM).
    // La persistencia de usuarios y mensajes offline la maneja GestorBD.
    // =====================================================================

    /**
     * Registra un cliente en la lista de conectados EN MEMORIA.
     * Este método se llama DESPUÉS de que la autenticación (login/registro)
     * fue exitosa en la base de datos.
     * 
     * ORDEN DE CARGA AL CONECTARSE:
     * 1. Primero se envía el HISTORIAL completo de conversaciones.
     * 2. Luego se envían los mensajes del BUZÓN OFFLINE (no leídos).
     * Esto permite que el usuario vea el contexto pasado y luego los nuevos.
     * 
     * @param nombre    Nombre de usuario autenticado
     * @param manejador Instancia del ManejadorCliente asociado
     * @return true si se registró correctamente, false si ya está conectado
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

        // ===== PASO 1: Enviar HISTORIAL de conversaciones (permanente) =====
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

        // ===== PASO 2: Enviar BUZÓN OFFLINE (mensajes no leídos, se borran) =====
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
     * Reenvía un mensaje al destinatario correcto.
     * 
     * Si está conectado → envío en vivo + historial.
     * Si está desconectado → solo buzón offline (historial se guarda al entregar).
     * Si no existe → rechaza con error.
     */
    public static void reenviarMensaje(Mensaje mensaje) {
        String receptor = mensaje.getReceptor();
        ManejadorCliente emisor = null;

        synchronized (clientesConectados) {
            ManejadorCliente destino = clientesConectados.get(receptor);

            if (destino != null) {
                // ===== Destinatario CONECTADO: reenviar al buzón vivo =====
                destino.enviarMensaje(mensaje);
                System.out.println("[SERVIDOR] Mensaje reenviado: " + mensaje.getEmisor() +
                        " -> " + receptor + " [" + mensaje.getTipo() + "]");

                // ===== HISTORIAL: Guardar permanentemente (cifrado) =====
                GestorBD.guardarMensajeHistorial(mensaje);
                return;
            }

            emisor = clientesConectados.get(mensaje.getEmisor());
        }

        // ===== FUERA del candado: operaciones de BD (pueden tardar) =====

        // ===== VALIDACIÓN: ¿El usuario existe en la BD? =====
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

        // NOTA: NO guardamos en historial aquí. Se guardará cuando
        // el usuario se reconecte y se le entregue del buzón.

        // ===== BUZÓN OFFLINE: Guardar temporalmente (cifrado) =====
        boolean guardado = GestorBD.guardarMensajeOffline(mensaje);

        if (!guardado) {
            // Buzón lleno (100 mensajes)
            if (emisor != null) {
                Mensaje error = new Mensaje("SERVIDOR", mensaje.getEmisor(),
                        Mensaje.NOTIFICACION,
                        "⚠ El buzón offline de '" + receptor +
                        "' está lleno (Máx 100). Mensaje guardado en historial pero no en buzón.");
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
     * Envía la lista de usuarios DESCONECTADOS (offline) a todos los clientes.
     * 
     * PERSISTENCIA: Ahora consulta la BD (GestorBD.obtenerUsuariosRegistrados)
     * para obtener todos los usuarios que se han registrado alguna vez,
     * y calcula la diferencia con los conectados actualmente.
     */
    public static void enviarListaOffline() {
        List<ManejadorCliente> clientesCopia;
        Mensaje msgOffline;

        // Obtener todos los registrados desde MySQL
        List<String> todosRegistrados = GestorBD.obtenerUsuariosRegistrados();

        synchronized (clientesConectados) {
            // Calcular usuarios offline = registrados - conectados
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
