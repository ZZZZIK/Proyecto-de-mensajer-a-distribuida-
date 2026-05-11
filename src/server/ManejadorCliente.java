package server;

import common.Mensaje;
import java.io.*;
import java.net.*;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * =====================================================================
 * MANEJADOR DE CLIENTE — Arquitectura de 2 Hilos + Buzón (Message Queue)
 * =====================================================================
 * 
 * CONCURRENCIA — Dos hilos por cliente:
 * Cada cliente conectado tiene DOS hilos independientes:
 * 
 *   1. HILO ENVIADOR (método run()):
 *      - Escucha los mensajes que llegan desde la red del cliente.
 *      - Cuando recibe un mensaje destinado a otro usuario, lo deposita
 *        en el BUZÓN (LinkedBlockingQueue) del destinatario.
 *      - NUNCA toca el ObjectOutputStream de otro cliente directamente,
 *        por lo que NUNCA se bloquea por culpa de una red lenta ajena.
 * 
 *   2. HILO RECEPTOR (método procesarBuzon()):
 *      - Vigila constantemente su propio Buzón (Queue).
 *      - Cuando cae un mensaje en el buzón, lo saca y lo envía por
 *        la red hacia el cliente usando ObjectOutputStream.
 *      - Si la red del cliente es lenta, SOLO ESTE HILO se bloquea.
 *        El resto del servidor sigue funcionando sin problema.
 * 
 * PATRÓN MESSAGE QUEUE (BUZÓN):
 * El buzón (LinkedBlockingQueue) actúa como amortiguador entre la
 * velocidad a la que otros usuarios generan mensajes y la velocidad
 * a la que la red de ESTE cliente puede consumirlos. Esto elimina
 * el problema del "Consumidor Lento" (Slow Consumer) que en la versión
 * anterior podía congelar todo el servidor.
 * 
 * RESILIENCIA — Manejo de fallos independientes:
 * Si un cliente se desconecta abruptamente, solo sus dos hilos se ven
 * afectados. Los demás clientes y el servidor continúan operando
 * normalmente. El bloque finally limpia los recursos del cliente.
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
    private volatile boolean activo;        // Flag para controlar el ciclo de vida de ambos hilos

    /**
     * ===== PATRÓN MESSAGE QUEUE: El Buzón de mensajes =====
     * 
     * LinkedBlockingQueue es una cola (Queue) a prueba de hilos (thread-safe).
     * Funciona como el buzón de correo en la entrada de una casa:
     * 
     * - Cualquier hilo puede depositar mensajes aquí con offer() (instantáneo).
     * - El Hilo Receptor los extrae con take() (se bloquea si el buzón está vacío).
     * 
     * Al usar esta cola, el método enviarMensaje() ya no toca la red directamente.
     * Solo deposita el mensaje en la cola (operación en RAM, tarda microsegundos).
     * Esto garantiza que el hilo que llama a enviarMensaje() NUNCA se bloqueará
     * por culpa de una conexión de red lenta, eliminando el problema del
     * "Consumidor Lento" que congelaba todo el servidor.
     */
    private final LinkedBlockingQueue<Mensaje> buzonMensajes = new LinkedBlockingQueue<>();

    /**
     * Constructor: recibe el socket de la conexión TCP establecida.
     * 
     * @param socket Socket TCP del cliente conectado
     */
    public ManejadorCliente(Socket socket) {
        this.socket = socket;
        this.activo = true;
    }

    /**
     * ===== HILO ENVIADOR — Método principal del hilo =====
     * 
     * Este es el primer hilo de los dos. Su responsabilidad es:
     * 1. Crear los streams de objetos (marshalling)
     * 2. Iniciar el Hilo Receptor (procesarBuzon) en segundo plano
     * 3. Leer el primer mensaje (CONECTAR) para obtener el nombre de usuario
     * 4. Registrar al cliente en el servidor
     * 5. Entrar en bucle escuchando mensajes del cliente
     * 6. Al finalizar (desconexión o error), limpiar recursos
     * 
     * IMPORTANTE: Este hilo NUNCA escribe directamente al ObjectOutputStream
     * de otro cliente. Cuando necesita enviar un mensaje a otro usuario,
     * deposita el mensaje en el buzón del destinatario (vía enviarMensaje()).
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

            // ===== CONCURRENCIA: Iniciar el Hilo Receptor (procesarBuzon) =====
            // Una vez que los streams están listos, arrancamos el segundo hilo.
            // Este hilo se encargará EXCLUSIVAMENTE de sacar mensajes del buzón
            // y enviarlos por la red hacia el cliente. Es un hilo daemon para
            // que se detenga automáticamente cuando el servidor termine.
            Thread hiloReceptor = new Thread(this::procesarBuzon);
            hiloReceptor.setDaemon(true);
            hiloReceptor.start();

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

            System.out.println("[HILO-ENVIADOR-" + nombreUsuario + "] Hilo Enviador iniciado.");

            // ===== PASO 2: Bucle principal de lectura de mensajes =====
            // Este bucle se ejecuta de forma CONCURRENTE con los hilos de
            // otros clientes. Cada hilo lee de su propio socket.
            while (activo) {
                // ===== MARSHALLING: Deserialización del objeto Mensaje =====
                // readObject() deserializa los bytes del stream TCP de vuelta
                // a un objeto Mensaje completo, incluyendo todos sus campos
                // (emisor, receptor, tipo, contenido, datosAdjuntos, etc.)
                Mensaje mensaje = (Mensaje) entrada.readObject();

                System.out.println("[HILO-ENVIADOR-" + nombreUsuario + "] Recibido: " + mensaje);

                // Procesar según el tipo de mensaje
                switch (mensaje.getTipo()) {
                    case Mensaje.TEXTO:
                        // ===== FUNCIÓN 1: MENSAJERÍA DE TEXTO =====
                        // TRANSPARENCIA DE ACCESO: El cliente envía un objeto
                        // Mensaje con el nombre del destinatario. No necesita
                        // conocer la dirección IP ni el socket del destinatario.
                        // El servidor resuelve la ubicación internamente.
                        //
                        // PATRÓN BUZÓN: reenviarMensaje() busca al destinatario
                        // y deposita el mensaje en SU buzón (no en la red).
                        // Esto es instantáneo y nunca se bloquea.
                        Servidor.reenviarMensaje(mensaje);
                        break;

                    case Mensaje.ARCHIVO:
                        // ===== FUNCIÓN 2: ENVÍO DE ARCHIVOS MULTIMEDIA =====
                        // Los datos binarios del archivo viajan como byte[]
                        // dentro del objeto Mensaje serializado. El marshalling
                        // de Java maneja automáticamente la serialización de
                        // arrays de bytes de cualquier tamaño.
                        System.out.println("[HILO-ENVIADOR-" + nombreUsuario + "] Archivo recibido: " +
                                mensaje.getNombreArchivo() + " (" +
                                (mensaje.getDatosAdjuntos() != null ?
                                        mensaje.getDatosAdjuntos().length : 0) + " bytes)");
                        Servidor.reenviarMensaje(mensaje);
                        break;

                    case Mensaje.DESCONECTAR:
                        // El cliente notifica desconexión voluntaria
                        System.out.println("[HILO-ENVIADOR-" + nombreUsuario + "] Desconexión voluntaria.");
                        return; // Sale del bucle, ejecuta finally

                    default:
                        System.out.println("[HILO-ENVIADOR-" + nombreUsuario + "] Tipo desconocido: " +
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
            System.out.println("[HILO-ENVIADOR-" + nombreUsuario + "] Error de red (IOException): " +
                    e.getMessage());
            System.out.println("[HILO-ENVIADOR-" + nombreUsuario + "] El cliente probablemente se desconectó.");

        } catch (ClassNotFoundException e) {
            // ===== RESILIENCIA: Error de deserialización =====
            // Ocurre si el cliente envía un objeto de una clase desconocida.
            // Esto puede pasar por incompatibilidad de versiones.
            System.err.println("[HILO-ENVIADOR-" + nombreUsuario + "] Error de deserialización: " +
                    e.getMessage());

        } finally {
            // ===== RESILIENCIA: Limpieza de recursos =====
            // El bloque finally SIEMPRE se ejecuta, garantizando que:
            // 1. Se marca el cliente como inactivo (detiene el Hilo Receptor)
            // 2. El cliente se remueve del mapa de clientes activos
            // 3. El socket se cierra para liberar el recurso de red
            // 4. Los demás clientes reciben la lista actualizada
            //
            // Esto previene fugas de recursos (resource leaks) y mantiene
            // la consistencia del sistema ante cualquier tipo de fallo.
            activo = false; // Señal para que el Hilo Receptor termine
            if (nombreUsuario != null) {
                Servidor.removerCliente(nombreUsuario);
            }
            try {
                if (socket != null && !socket.isClosed()) {
                    socket.close();
                    System.out.println("[HILO-ENVIADOR-" + nombreUsuario + "] Socket cerrado correctamente.");
                }
            } catch (IOException e) {
                System.err.println("[HILO-ENVIADOR-" + nombreUsuario + "] Error cerrando socket: " +
                        e.getMessage());
            }
            System.out.println("[HILO-ENVIADOR-" + nombreUsuario + "] Hilo Enviador finalizado.");
        }
    }

    /**
     * ===== HILO RECEPTOR — Procesa el buzón y envía por la red =====
     * 
     * Este es el segundo hilo (el "Mayordomo"). Su ÚNICA responsabilidad es:
     * 1. Esperar a que caigan mensajes en el buzón (buzonMensajes.take()).
     * 2. Sacar cada mensaje y enviarlo por la red al cliente (writeObject).
     * 
     * ¿Por qué es seguro?
     * - Si la red del cliente es lenta, SOLO ESTE HILO se bloquea en writeObject().
     * - El Hilo Enviador (run) sigue libre para seguir depositando mensajes
     *   en buzones de otros clientes sin bloquearse.
     * - Los mensajes que llegan mientras este hilo está bloqueado simplemente
     *   se acumulan en el buzón (LinkedBlockingQueue) y serán procesados
     *   cuando la red del cliente se recupere.
     * 
     * SINCRONIZACIÓN:
     * Ya NO se necesita synchronized en el ObjectOutputStream porque este
     * método es el ÚNICO punto del código que escribe en él. Solo un hilo
     * (este Hilo Receptor) accede al stream de salida, eliminando la necesidad
     * de candados (synchronized) para protegerlo.
     */
    private void procesarBuzon() {
        System.out.println("[HILO-RECEPTOR-" + nombreUsuario + "] Hilo Receptor iniciado. Vigilando buzón...");

        try {
            while (activo) {
                // ===== PATRÓN MESSAGE QUEUE: Extraer mensaje del buzón =====
                // take() es BLOQUEANTE: si el buzón está vacío, este hilo se
                // duerme hasta que alguien deposite un mensaje con offer().
                // Esto es eficiente: no consume CPU mientras espera.
                Mensaje mensaje = buzonMensajes.take();

                // ===== MARSHALLING: Serialización y envío por la red =====
                // writeObject() convierte el objeto Mensaje a bytes y los envía
                // por el socket TCP. Si la red del cliente es lenta, SOLO ESTE
                // HILO se bloquea aquí. El resto del servidor sigue funcionando.
                salida.writeObject(mensaje);  // Serialización (marshalling)
                salida.flush();               // Forzar envío inmediato
                salida.reset();               // Limpiar cache del stream
                // reset() es importante: sin él, ObjectOutputStream cachea
                // referencias a objetos ya enviados y no serializa las
                // versiones actualizadas. Esto causaría que mensajes
                // posteriores lleguen con datos obsoletos.
            }
        } catch (IOException e) {
            // ===== RESILIENCIA: Fallo de red aislado =====
            // Si la red del cliente falla mientras se intenta enviar un mensaje,
            // SOLO este Hilo Receptor se detiene. El Hilo Enviador y el resto
            // del servidor continúan funcionando sin interrupciones.
            //
            // Se cierra el socket para forzar que el Hilo Enviador (run())
            // también detecte la desconexión vía IOException en readObject().
            if (activo) {
                System.err.println("[HILO-RECEPTOR-" + nombreUsuario +
                        "] Error enviando mensaje por red: " + e.getMessage());
                try {
                    if (socket != null && !socket.isClosed()) {
                        socket.close(); // Forzar cierre para que el Hilo Enviador también se entere
                    }
                } catch (IOException ex) {
                    // Ignorar error al cerrar
                }
            }
        } catch (InterruptedException e) {
            // ===== take() fue interrumpido (el hilo está siendo detenido) =====
            Thread.currentThread().interrupt();
        }

        System.out.println("[HILO-RECEPTOR-" + nombreUsuario + "] Hilo Receptor finalizado.");
    }

    /**
     * Deposita un mensaje en el buzón para ser enviado al cliente.
     * 
     * PATRÓN MESSAGE QUEUE (Antes vs Ahora):
     * 
     * ANTES: Este método escribía DIRECTAMENTE en el ObjectOutputStream
     * (la red). Si la red del cliente era lenta, el hilo que llamaba a
     * este método se BLOQUEABA, potencialmente congelando todo el servidor
     * si el bloqueo ocurría dentro de un synchronized global.
     * 
     * AHORA: Este método solo deposita el mensaje en el buzón (Queue),
     * que es una operación en memoria RAM (tarda microsegundos, NUNCA
     * se bloquea). El Hilo Receptor (procesarBuzon) se encargará de
     * sacar el mensaje del buzón y enviarlo por la red a su propio ritmo.
     * 
     * Esto es equivalente al "Cartero Veloz" que tira la carta en el
     * buzón de la casa y sigue su camino sin esperar a que el dueño
     * (la red) la recoja.
     * 
     * @param mensaje Objeto Mensaje a depositar en el buzón
     */
    public void enviarMensaje(Mensaje mensaje) {
        // ===== Depositar en el buzón (operación instantánea en RAM) =====
        // offer() agrega el mensaje a la cola. Esta operación:
        // - Es thread-safe (múltiples hilos pueden llamarla simultáneamente)
        // - NUNCA se bloquea (a diferencia del writeObject anterior)
        // - Tarda microsegundos (es escritura en memoria, no en red)
        buzonMensajes.offer(mensaje);
    }

    /**
     * Devuelve el nombre de usuario asociado a este manejador.
     */
    public String getNombreUsuario() {
        return nombreUsuario;
    }
}
