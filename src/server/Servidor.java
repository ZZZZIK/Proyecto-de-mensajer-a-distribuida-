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
 * BUZÓN OFFLINE (Mensajería asíncrona):
 * Si un destinatario no está conectado, los mensajes se almacenan en una
 * "bodega offline" (HashMap<String, LinkedList<Mensaje>>) con un límite
 * máximo de mensajes por usuario (estilo "Buzón de Voz"). Cuando el
 * usuario vuelve a conectarse, los mensajes pendientes se inyectan en
 * su buzón vivo y se libera la memoria de la bodega.
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
     * ===== BUZÓN OFFLINE: Bodega de mensajes para usuarios desconectados =====
     * 
     * Este HashMap almacena los mensajes pendientes para usuarios que NO están
     * conectados actualmente. Funciona como una "Oficina de Correos Central":
     * 
     * - Clave: Nombre del usuario destinatario (String)
     * - Valor: Lista de mensajes pendientes (LinkedList<Mensaje>)
     * 
     * Cuando un usuario se conecta (registrarCliente), se revisa esta bodega.
     * Si tiene mensajes pendientes, se inyectan en su buzón vivo y se elimina
     * la entrada para liberar memoria.
     * 
     * LÍMITE: Cada usuario puede tener como máximo MAX_MENSAJES_OFFLINE mensajes
     * acumulados. Si se excede el límite, los nuevos mensajes se rechazan y se
     * notifica al emisor (estilo "Buzón de Voz").
     * 
     * SINCRONIZACIÓN: Se protege con el mismo candado (clientesConectados) para
     * evitar condiciones de carrera entre la verificación de conexión y el
     * almacenamiento offline.
     */
    private static final HashMap<String, LinkedList<Mensaje>> mensajesOffline = new HashMap<>();

    /**
     * Límite máximo de mensajes que se pueden almacenar en el buzón offline
     * de un usuario desconectado. Si se excede este límite, los nuevos
     * mensajes se rechazan (estilo "Buzón de Voz").
     */
    private static final int MAX_MENSAJES_OFFLINE = 5;

    /**
     * ===== REGISTRO PERMANENTE DE USUARIOS =====
     * 
     * Este HashSet almacena los nombres de TODOS los usuarios que se han
     * conectado alguna vez al servidor. Sirve para:
     * 
     * 1. Mostrar la lista de usuarios desconectados en la interfaz.
     * 2. Validar que el destinatario de un mensaje offline realmente
     *    existe (ha iniciado sesión al menos una vez). Si alguien
     *    intenta enviar un mensaje a un nombre que nunca se ha registrado,
     *    el servidor rechaza el mensaje con un error.
     * 
     * NOTA: Este registro es en memoria y se pierde al reiniciar el servidor.
     * Para persistencia real, se necesitaría guardar en un archivo o base de datos.
     */
    private static final HashSet<String> usuariosRegistrados = new HashSet<>();

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
        List<Mensaje> mensajesPendientes = null;

        synchronized (clientesConectados) {
            // Verificar si el nombre de usuario ya está en uso
            if (clientesConectados.containsKey(nombre)) {
                System.out.println("[SERVIDOR] Registro rechazado: '" + nombre + "' ya está conectado.");
                return false;
            }
            clientesConectados.put(nombre, manejador);

            // ===== REGISTRO PERMANENTE: Guardar el nombre como usuario conocido =====
            usuariosRegistrados.add(nombre);

            System.out.println("[SERVIDOR] Cliente registrado: " + nombre +
                    " (Total conectados: " + clientesConectados.size() +
                    ", Total registrados: " + usuariosRegistrados.size() + ")");

            // ===== BUZÓN OFFLINE: Entregar mensajes pendientes =====
            if (mensajesOffline.containsKey(nombre)) {
                mensajesPendientes = mensajesOffline.remove(nombre);
                System.out.println("[SERVIDOR] Buzón offline de '" + nombre +
                        "': " + mensajesPendientes.size() + " mensajes pendientes encontrados.");
            }
        }

        // ===== FUERA del candado: inyectar mensajes pendientes al buzón vivo =====
        if (mensajesPendientes != null) {
            Mensaje avisoOffline = new Mensaje("SERVIDOR", nombre,
                    Mensaje.NOTIFICACION,
                    "📬 Tienes " + mensajesPendientes.size() +
                    " mensaje(s) que recibiste mientras estabas desconectado:");
            manejador.enviarMensaje(avisoOffline);

            for (Mensaje msgPendiente : mensajesPendientes) {
                manejador.enviarMensaje(msgPendiente);
            }
            System.out.println("[SERVIDOR] Mensajes offline entregados a '" + nombre + "'.");
        }

        // Notificar a todos los clientes las listas actualizadas
        enviarListaUsuarios();
        enviarListaOffline();
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
        // Notificar a los demás las listas actualizadas y la desconexión
        enviarListaUsuarios();
        enviarListaOffline();
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
        ManejadorCliente emisor = null;

        synchronized (clientesConectados) {
            // Buscar el destinatario en el mapa de clientes conectados
            ManejadorCliente destino = clientesConectados.get(receptor);

            if (destino != null) {
                // ===== Destinatario CONECTADO: reenviar al buzón vivo =====
                destino.enviarMensaje(mensaje);
                System.out.println("[SERVIDOR] Mensaje reenviado: " + mensaje.getEmisor() +
                        " -> " + receptor + " [" + mensaje.getTipo() + "]");
                return;
            }

            // ===== VALIDACIÓN: ¿El usuario existe en el registro permanente? =====
            // Si el destinatario nunca se ha conectado al servidor, rechazamos
            // el mensaje con un error claro.
            if (!usuariosRegistrados.contains(receptor)) {
                emisor = clientesConectados.get(mensaje.getEmisor());
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

            // ===== Destinatario DESCONECTADO pero REGISTRADO: Guardar offline =====
            emisor = clientesConectados.get(mensaje.getEmisor());

            LinkedList<Mensaje> buzonOffline = mensajesOffline.get(receptor);
            if (buzonOffline == null) {
                buzonOffline = new LinkedList<>();
                mensajesOffline.put(receptor, buzonOffline);
            }

            // ===== ESTILO BUZÓN DE VOZ: Verificar límite =====
            if (buzonOffline.size() >= MAX_MENSAJES_OFFLINE) {
                if (emisor != null) {
                    Mensaje error = new Mensaje("SERVIDOR", mensaje.getEmisor(),
                            Mensaje.NOTIFICACION,
                            "⚠ El buzón offline de '" + receptor +
                            "' está lleno (Máx " + MAX_MENSAJES_OFFLINE +
                            "). Mensaje no entregado.");
                    emisor.enviarMensaje(error);
                }
                System.out.println("[SERVIDOR] Buzón offline de '" + receptor +
                        "' lleno. Mensaje de '" + mensaje.getEmisor() + "' rechazado.");
                return;
            }

            // ===== Hay espacio: guardar el mensaje en la bodega offline =====
            buzonOffline.add(mensaje);
            System.out.println("[SERVIDOR] Mensaje de '" + mensaje.getEmisor() +
                    "' guardado en buzón offline de '" + receptor +
                    "' (" + buzonOffline.size() + "/" + MAX_MENSAJES_OFFLINE + ")");
        }

        // ===== FUERA del candado: notificar al emisor =====
        if (emisor != null) {
            Mensaje aviso = new Mensaje("SERVIDOR", mensaje.getEmisor(),
                    Mensaje.NOTIFICACION,
                    "📩 '" + receptor + "' está desconectado. Tu mensaje se ha guardado en su buzón offline.");
            emisor.enviarMensaje(aviso);
        }
    }

    /**
     * Envía la lista actualizada de usuarios conectados a TODOS los clientes.
     * 
     * Se envía como un Mensaje de tipo LISTA_USUARIOS, donde el contenido
     * es una cadena con los nombres separados por comas.
     * 
     * PATRÓN MESSAGE QUEUE: Se copia la lista de clientes bajo el candado
     * (operación en RAM, tarda microsegundos) y se suelta el candado
     * INMEDIATAMENTE. Luego se depositan los mensajes en los buzones
     * de cada cliente FUERA del candado. Como enviarMensaje() ahora
     * solo hace un offer() al buzón (RAM), esta operación jamás se
     * bloquea por culpa de una conexión de red lenta.
     */
    public static void enviarListaUsuarios() {
        List<ManejadorCliente> clientesCopia;
        Mensaje msgLista;

        synchronized (clientesConectados) {
            // Construir la lista de nombres separados por coma
            String lista = String.join(",", clientesConectados.keySet());
            msgLista = new Mensaje("SERVIDOR", "TODOS",
                    Mensaje.LISTA_USUARIOS, lista);

            // Copiar la lista de clientes (operación en RAM, instantánea)
            clientesCopia = new ArrayList<>(clientesConectados.values());
        }
        // ===== FUERA del candado: depositar mensajes en los buzones =====
        // El candado global ya fue liberado. Ahora podemos iterar
        // tranquilamente sin bloquear a nadie.
        for (ManejadorCliente cliente : clientesCopia) {
            cliente.enviarMensaje(msgLista);
        }
    }

    /**
     * Envía una notificación del sistema a todos los clientes conectados.
     * 
     * @param texto Texto de la notificación
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
     * Calcula la diferencia entre usuariosRegistrados (todos los que han
     * existido) y clientesConectados (los que están online ahora).
     * El resultado son los usuarios que se han registrado alguna vez
     * pero actualmente no están conectados.
     */
    public static void enviarListaOffline() {
        List<ManejadorCliente> clientesCopia;
        Mensaje msgOffline;

        synchronized (clientesConectados) {
            // Calcular usuarios offline = registrados - conectados
            List<String> offline = new ArrayList<>();
            for (String registrado : usuariosRegistrados) {
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
