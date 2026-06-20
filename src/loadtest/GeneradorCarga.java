package loadtest;

import common.Mensaje;
import java.io.*;
import java.net.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import javax.swing.*;
import java.awt.*;
import java.awt.image.BufferedImage;

/**
 * Generador de carga concurrente para prueba de tráfico real.
 * 
 * Lanza 50+ clientes simulados que envían mensajes concurrentemente
 * durante al menos 60 segundos, ejercitando las dos funciones principales
 * y el recurso protegido por exclusión mutua.
 * 
 * Requisito §3.1: Al menos 50 clientes o hilos simultáneos, 60 segundos.
 * 
 * Uso: java loadtest.GeneradorCarga <host> <puerto> <numClientes> <duracionSeg>
 */
public class GeneradorCarga {

    // Configuración por defecto
    private static final int NUM_CLIENTES_DEFAULT = 50;
    private static final int DURACION_DEFAULT_SEG = 60;
    private static final int TIEMPO_FALLA_SEG = 30;  // Falla inducida a los 30 seg

    // Conexión
    private final String[] hosts;
    private final int[] puertos;
    private final int numClientes;
    private final int duracionSegundos;

    // Métricas atómicas (thread-safe)
    private final AtomicLong peticionesEnviadas = new AtomicLong(0);
    private final AtomicLong peticionesExitosas = new AtomicLong(0);
    private final AtomicLong peticionesFallidas = new AtomicLong(0);
    private final CopyOnWriteArrayList<Long> latencias = new CopyOnWriteArrayList<>();
    private final AtomicLong bytesEnviados = new AtomicLong(0);
    private final AtomicLong mensajesCoordinacion = new AtomicLong(0);
    private final AtomicLong mutexRequests = new AtomicLong(0);
    private final AtomicLong eleccionesBully = new AtomicLong(0);
    private final AtomicLong archivosEnviados = new AtomicLong(0);
    private final AtomicLong clientesConectados = new AtomicLong(0);

    // Control
    private volatile boolean ejecutando = true;
    private volatile boolean fallaInducida = false;
    private volatile long tiempoInicio;
    private volatile long tiempoFalla = 0;
    private volatile long tiempoRecuperacion = 0;

    // Registro de muestras por segundo para gráficos
    private final List<MuestraSegundo> muestrasPorSegundo = new CopyOnWriteArrayList<>();

    /**
     * Registro de métricas de un segundo específico.
     */
    public static class MuestraSegundo {
        public final int segundo;
        public final long throughput;
        public final double latenciaAvg;
        public final double latenciaP95;
        public final long errores;
        public final long clientesActivos;
        public final boolean duranteFalla;
        public final long msgsCoordinacion;

        public MuestraSegundo(int segundo, long throughput, double latenciaAvg,
                              double latenciaP95, long errores, long clientesActivos,
                              boolean duranteFalla, long msgsCoordinacion) {
            this.segundo = segundo;
            this.throughput = throughput;
            this.latenciaAvg = latenciaAvg;
            this.latenciaP95 = latenciaP95;
            this.errores = errores;
            this.clientesActivos = clientesActivos;
            this.duranteFalla = duranteFalla;
            this.msgsCoordinacion = msgsCoordinacion;
        }
    }

    public GeneradorCarga(String[] hosts, int[] puertos, int numClientes, int duracionSegundos) {
        this.hosts = hosts;
        this.puertos = puertos;
        this.numClientes = numClientes;
        this.duracionSegundos = duracionSegundos;
    }

    /**
     * Ejecuta la prueba de carga completa.
     */
    public void ejecutar() {
        tiempoInicio = System.currentTimeMillis();

        imprimirBanner();

        // 1. Registrar usuarios de prueba
        System.out.println("\n[CARGA] Fase 1: Registrando " + numClientes + " usuarios de prueba...");
        registrarUsuarios();

        // 2. Conectar todos los clientes
        System.out.println("[CARGA] Fase 2: Conectando " + numClientes + " clientes simultáneos...");
        ExecutorService poolClientes = Executors.newFixedThreadPool(numClientes);
        List<ClienteSimulado> clientes = new ArrayList<>();

        for (int i = 0; i < numClientes; i++) {
            ClienteSimulado cliente = new ClienteSimulado(i);
            clientes.add(cliente);
            poolClientes.submit(cliente);
        }

        // Esperar a que se conecten
        try { Thread.sleep(3000); } catch (InterruptedException e) { return; }
        System.out.println("[CARGA] " + clientesConectados.get() + "/" +
                numClientes + " clientes conectados.");

        // 3. Iniciar hilo de monitoreo (dashboard)
        ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor();
        monitor.scheduleAtFixedRate(this::actualizarDashboard, 1, 1, TimeUnit.SECONDS);

        // 4. Esperar la duración completa
        System.out.println("[CARGA] Fase 3: Prueba de carga iniciada (" +
                duracionSegundos + " segundos)...\n");

        try {
            Thread.sleep(duracionSegundos * 1000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 5. Detener todo
        ejecutando = false;
        monitor.shutdownNow();
        poolClientes.shutdownNow();

        try { Thread.sleep(2000); } catch (InterruptedException e) { /* ignore */ }

        // 6. Generar reporte
        generarReporte();
    }

    /**
     * Registra los usuarios de prueba en el servidor.
     */
    private void registrarUsuarios() {
        for (int i = 0; i < numClientes; i++) {
            int index = i % puertos.length;
            String hostDestino = hosts[index];
            int puertoDestino = puertos[index];
            try {
                Socket socket = new Socket(hostDestino, puertoDestino);
                ObjectOutputStream salida = new ObjectOutputStream(socket.getOutputStream());
                salida.flush();
                ObjectInputStream entrada = new ObjectInputStream(socket.getInputStream());

                // Registrar usuario
                Mensaje registro = new Mensaje("loadtest_" + i, "SERVIDOR",
                        Mensaje.REGISTRO, "password123");
                salida.writeObject(registro);
                salida.flush();

                Mensaje respuesta = (Mensaje) entrada.readObject();
                // Ignorar si ya existe (AUTH_FAIL por duplicado)

                socket.close();
            } catch (Exception e) {
                // Si falla el registro, el usuario puede que ya exista
            }
        }
        System.out.println("[CARGA] Usuarios de prueba registrados.");
    }

    /**
     * Cliente simulado que envía mensajes continuamente.
     */
    private class ClienteSimulado implements Runnable {
        private final int clienteId;
        private Socket socket;
        private ObjectOutputStream salida;
        private ObjectInputStream entrada;
        private volatile boolean conectado = false;
        private volatile String hostActual;
        private volatile int puertoActual;

        public ClienteSimulado(int clienteId) {
            this.clienteId = clienteId;
            int index = clienteId % puertos.length;
            this.hostActual = hosts[index];
            this.puertoActual = puertos[index];
        }

        @Override
        public void run() {
            try {
                // Conectar
                socket = new Socket(hostActual, puertoActual);
                salida = new ObjectOutputStream(socket.getOutputStream());
                salida.flush();
                entrada = new ObjectInputStream(socket.getInputStream());

                // Login
                Mensaje login = new Mensaje("loadtest_" + clienteId, "SERVIDOR",
                        Mensaje.LOGIN, "password123");
                salida.writeObject(login);
                salida.flush();

                Mensaje respuesta = (Mensaje) entrada.readObject();
                if (!respuesta.getTipo().equals(Mensaje.AUTH_OK)) {
                    return;
                }

                conectado = true;
                clientesConectados.incrementAndGet();

                // Iniciar hilo receptor (procesa mensajes de control)
                iniciarReceptor();

                // Enviar mensajes continuamente
                Random random = new Random(clienteId);
                while (ejecutando) {
                    try {
                        long inicio = System.nanoTime();

                        // Elegir destinatario aleatorio
                        int destId = random.nextInt(numClientes);
                        while (destId == clienteId) {
                            destId = random.nextInt(numClientes);
                        }

                        // Crear mensaje: 90% TEXTO (Función 1), 10% ARCHIVO (Función 2)
                        Mensaje msg;
                        long numPet = peticionesEnviadas.get();
                        if (numPet % 10 == 0 && numPet > 0) {
                            // Enviar ARCHIVO pequeño (1-5 KB) — Función 2
                            byte[] datosArchivo = new byte[1024 + random.nextInt(4096)];
                            random.nextBytes(datosArchivo);
                            msg = new Mensaje("loadtest_" + clienteId,
                                    "loadtest_" + destId,
                                    "archivo_carga_" + numPet + ".bin",
                                    datosArchivo);
                            archivosEnviados.incrementAndGet();
                        } else {
                            // Enviar TEXTO — Función 1
                            msg = new Mensaje("loadtest_" + clienteId,
                                    "loadtest_" + destId, Mensaje.TEXTO,
                                    "Mensaje de carga #" + numPet +
                                    " desde cliente " + clienteId);
                        }

                        synchronized (salida) {
                            salida.writeObject(msg);
                            salida.flush();
                            salida.reset();
                        }

                        long fin = System.nanoTime();
                        long latenciaMs = (fin - inicio) / 1_000_000;

                        peticionesEnviadas.incrementAndGet();
                        peticionesExitosas.incrementAndGet();
                        latencias.add(latenciaMs);
                        bytesEnviados.addAndGet(msg.getTipo().equals(Mensaje.ARCHIVO) ?
                                msg.getDatosAdjuntos().length : msg.getContenido().length());

                        // Pausa entre mensajes (50-200ms)
                        Thread.sleep(50 + random.nextInt(150));

                    } catch (IOException e) {
                        peticionesFallidas.incrementAndGet();
                        if (ejecutando) {
                            // Intentar reconectar
                            conectado = false;
                            clientesConectados.decrementAndGet();
                            try {
                                Thread.sleep(1000);
                                reconectar();
                            } catch (InterruptedException ie) {
                                return;
                            }
                        }
                    } catch (InterruptedException e) {
                        return;
                    }
                }

            } catch (Exception e) {
                peticionesFallidas.incrementAndGet();
            } finally {
                if (conectado) {
                    clientesConectados.decrementAndGet();
                }
                try {
                    if (socket != null && !socket.isClosed()) socket.close();
                } catch (IOException e) { /* ignorar */ }
            }
        }

        private void iniciarReceptor() {
            Thread receptor = new Thread(() -> {
                try {
                    while (ejecutando && conectado) {
                        Mensaje msg = (Mensaje) entrada.readObject();
                        if (msg != null && msg.getTipo().equals(Mensaje.NOTIFICACION)) {
                            String cont = msg.getContenido();
                            if (cont.contains("[FALLO DETECTADO]")) {
                                if (!fallaInducida) {
                                    fallaInducida = true;
                                    tiempoFalla = System.currentTimeMillis();
                                }
                            } else if (cont.contains("[SISTEMA RECUPERADO]")) {
                                if (fallaInducida && tiempoRecuperacion == 0) {
                                    tiempoRecuperacion = System.currentTimeMillis();
                                }
                            } else if (cont.contains("[METRICAS_COORD]")) {
                                try {
                                    String datos = cont.substring(cont.indexOf("mutex="));
                                    String[] pparts = datos.split(",");
                                    for (String p : pparts) {
                                        String[] kv = p.trim().split("=");
                                        if (kv.length == 2) {
                                            switch (kv[0]) {
                                                case "mutex": mutexRequests.set(Long.parseLong(kv[1])); break;
                                                case "elecciones": eleccionesBully.set(Long.parseLong(kv[1])); break;
                                                case "total": mensajesCoordinacion.set(Long.parseLong(kv[1])); break;
                                            }
                                        }
                                    }
                                } catch (Exception ex) { /* ignorar errores de parseo */ }
                            }
                        }
                    }
                } catch (Exception e) { /* ignorar */ }
            });
            receptor.setDaemon(true);
            receptor.start();
        }

        private void reconectar() {
            // Encontrar el índice original
            int indexOriginal = -1;
            for (int i = 0; i < puertos.length; i++) {
                if (puertos[i] == puertoActual && hosts[i].equals(hostActual)) {
                    indexOriginal = i;
                    break;
                }
            }
            if (indexOriginal == -1) indexOriginal = clienteId % puertos.length;

            // Generar una lista de índices para probar, empezando por el actual
            List<Integer> indices = new ArrayList<>();
            indices.add(indexOriginal);
            for (int i = 0; i < puertos.length; i++) {
                if (i != indexOriginal) {
                    indices.add(i);
                }
            }

            for (int idx : indices) {
                String h = hosts[idx];
                int p = puertos[idx];
                try {
                    if (socket != null && !socket.isClosed()) socket.close();
                    socket = new Socket(h, p);
                    salida = new ObjectOutputStream(socket.getOutputStream());
                    salida.flush();
                    entrada = new ObjectInputStream(socket.getInputStream());

                    Mensaje login = new Mensaje("loadtest_" + clienteId, "SERVIDOR",
                            Mensaje.LOGIN, "password123");
                    salida.writeObject(login);
                    salida.flush();

                    Mensaje resp = (Mensaje) entrada.readObject();
                    if (resp.getTipo().equals(Mensaje.AUTH_OK)) {
                        conectado = true;
                        clientesConectados.incrementAndGet();
                        this.hostActual = h;
                        this.puertoActual = p;
                        iniciarReceptor();
                        return; // Reconectado con éxito
                    } else {
                        socket.close();
                    }
                } catch (Exception e) {
                    // Falló este endpoint, intentar el siguiente
                }
            }
        }
    }

    // ═══════════════════════════════════════════════════
    //  DASHBOARD EN CONSOLA
    // ═══════════════════════════════════════════════════

    private long ultimoPeticiones = 0;
    private int segundoActual = 0;

    /**
     * Actualiza el dashboard de métricas en consola cada segundo.
     */
    private void actualizarDashboard() {
        segundoActual++;
        long petActuales = peticionesExitosas.get();
        long throughput = petActuales - ultimoPeticiones;
        ultimoPeticiones = petActuales;

        // Calcular latencia promedio y p95
        double latAvg = 0;
        double latP95 = 0;
        if (!latencias.isEmpty()) {
            List<Long> copia = new ArrayList<>(latencias);
            Collections.sort(copia);
            long suma = 0;
            for (long l : copia) suma += l;
            latAvg = (double) suma / copia.size();
            int indiceP95 = (int) (copia.size() * 0.95);
            latP95 = copia.get(Math.min(indiceP95, copia.size() - 1));
        }

        long errores = peticionesFallidas.get();
        long activos = clientesConectados.get();

        // Guardar muestra
        muestrasPorSegundo.add(new MuestraSegundo(
                segundoActual, throughput, latAvg, latP95,
                errores, activos, fallaInducida, mensajesCoordinacion.get()));

        // Barra de progreso
        int progreso = (int) ((double) segundoActual / duracionSegundos * 40);
        StringBuilder barra = new StringBuilder();
        for (int i = 0; i < 40; i++) {
            barra.append(i < progreso ? "█" : "░");
        }

        // Imprimir dashboard
        System.out.print("\033[2J\033[H"); // Limpiar pantalla
        System.out.println("╔═══════════════════════════════════════════════════════════════╗");
        System.out.println("║         PRUEBA DE CARGA — WhatsApp Distribuido               ║");
        System.out.printf( "║         Tiempo: %ds/%ds                                      ║%n",
                segundoActual, duracionSegundos);
        System.out.println("╠════════════════╦══════════════╦═════════════╦═════════════════╣");
        System.out.println("║  Throughput    ║ Latencia Avg ║ Latencia p95║ Tasa Error      ║");
        System.out.printf( "║  %5d req/s   ║ %7.1f ms   ║ %7.1f ms  ║ %6.2f%%          ║%n",
                throughput, latAvg, latP95,
                peticionesEnviadas.get() > 0 ?
                        (double) errores / peticionesEnviadas.get() * 100 : 0);
        System.out.println("╠════════════════╩══════════════╩═════════════╩═════════════════╣");
        System.out.printf( "║  Clientes activos: %3d/%-3d  │  Mensajes totales: %,10d   ║%n",
                activos, numClientes, petActuales);
        System.out.printf( "║  Bytes enviados: %,12d  │  Archivos enviados: %,6d     ║%n",
                bytesEnviados.get(), archivosEnviados.get());
        System.out.printf( "║  Msgs coordinación: %,6d   │  Mutex requests: %,6d        ║%n",
                mensajesCoordinacion.get(), mutexRequests.get());
        System.out.printf( "║  Elecciones Bully: %,6d                                     ║%n",
                eleccionesBully.get());
        System.out.println("╠═══════════════════════════════════════════════════════════════╣");
        System.out.printf( "║  [%s] %d/%ds  ║%n",
                barra.toString(), segundoActual, duracionSegundos);

        if (segundoActual >= TIEMPO_FALLA_SEG && !fallaInducida) {
            System.out.println("║  👉 ¡DERRIBE EL COORDINADOR AHORA (Cierre Nodo 2 o 3)!       ║");
        }

        if (fallaInducida && tiempoRecuperacion > 0) {
            long tiempoRec = tiempoRecuperacion - tiempoFalla;
            System.out.printf( "║  ⚠ FALLA INDUCIDA en t=%ds                                  ║%n",
                    TIEMPO_FALLA_SEG);
            System.out.printf( "║  ✅ Recuperación completada en %.1fs                          ║%n",
                    tiempoRec / 1000.0);
        } else if (fallaInducida) {
            System.out.printf( "║  ⚠ FALLA INDUCIDA en t=%ds — Recuperando...                  ║%n",
                    TIEMPO_FALLA_SEG);
        }

        System.out.println("╚═══════════════════════════════════════════════════════════════╝");
    }

    /**
     * Genera el reporte final de la prueba de carga.
     */
    private void generarReporte() {
        long duracionReal = System.currentTimeMillis() - tiempoInicio;

        System.out.println("\n");
        System.out.println("╔═══════════════════════════════════════════════════════════════╗");
        System.out.println("║                REPORTE FINAL DE PRUEBA DE CARGA              ║");
        System.out.println("╠═══════════════════════════════════════════════════════════════╣");

        // Calcular estadísticas finales
        List<Long> todasLatencias = new ArrayList<>(latencias);
        Collections.sort(todasLatencias);

        double latAvg = 0, latP95 = 0, latP99 = 0, latMin = 0, latMax = 0;
        if (!todasLatencias.isEmpty()) {
            long suma = 0;
            for (long l : todasLatencias) suma += l;
            latAvg = (double) suma / todasLatencias.size();
            latP95 = todasLatencias.get((int) (todasLatencias.size() * 0.95));
            latP99 = todasLatencias.get((int) (todasLatencias.size() * 0.99));
            latMin = todasLatencias.get(0);
            latMax = todasLatencias.get(todasLatencias.size() - 1);
        }

        double throughputAvg = (double) peticionesExitosas.get() / (duracionReal / 1000.0);
        double tasaError = peticionesEnviadas.get() > 0 ?
                (double) peticionesFallidas.get() / peticionesEnviadas.get() * 100 : 0;

        System.out.printf("║  Duración total:         %,.1f segundos                      ║%n", duracionReal / 1000.0);
        System.out.printf("║  Clientes simultáneos:   %d                                  ║%n", numClientes);
        System.out.printf("║  Peticiones enviadas:    %,d                                 ║%n", peticionesEnviadas.get());
        System.out.printf("║  Peticiones exitosas:    %,d                                 ║%n", peticionesExitosas.get());
        System.out.printf("║  Peticiones fallidas:    %,d                                 ║%n", peticionesFallidas.get());
        System.out.println("╠═══════════════════════════════════════════════════════════════╣");
        System.out.printf("║  Throughput promedio:     %.1f req/s                         ║%n", throughputAvg);
        System.out.printf("║  Latencia promedio:       %.1f ms                            ║%n", latAvg);
        System.out.printf("║  Latencia p95:            %.1f ms                            ║%n", latP95);
        System.out.printf("║  Latencia p99:            %.1f ms                            ║%n", latP99);
        System.out.printf("║  Latencia mínima:         %.1f ms                            ║%n", latMin);
        System.out.printf("║  Latencia máxima:         %.1f ms                            ║%n", latMax);
        System.out.printf("║  Tasa de error:           %.2f%%                              ║%n", tasaError);
        System.out.println("╠═══════════════════════════════════════════════════════════════╣");
        System.out.printf("║  Archivos enviados:       %,d                                ║%n", archivosEnviados.get());
        System.out.printf("║  Msgs coordinación:       %,d                                ║%n", mensajesCoordinacion.get());
        System.out.printf("║  Mutex requests:          %,d                                ║%n", mutexRequests.get());
        System.out.printf("║  Elecciones Bully:        %,d                                ║%n", eleccionesBully.get());
        System.out.println("╚═══════════════════════════════════════════════════════════════╝");

        // Guardar CSV
        guardarCSV(todasLatencias, throughputAvg, latAvg, latP95, tasaError);
    }

    /**
     * Guarda los datos crudos en un archivo CSV.
     */
    private void guardarCSV(List<Long> todasLatencias, double throughput,
                            double latAvg, double latP95, double tasaError) {
        File dir = new File("resultados");
        if (!dir.exists()) dir.mkdirs();

        String fecha = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
        String archivo = "resultados/metricas_" + fecha + ".csv";

        try (PrintWriter pw = new PrintWriter(new FileWriter(archivo))) {
            pw.println("segundo,throughput,latencia_avg,latencia_p95,errores,clientes_activos,durante_falla,msgs_coordinacion");
            for (MuestraSegundo m : muestrasPorSegundo) {
                pw.printf("%d,%d,%.2f,%.2f,%d,%d,%b,%d%n",
                        m.segundo, m.throughput, m.latenciaAvg, m.latenciaP95,
                        m.errores, m.clientesActivos, m.duranteFalla,
                        m.msgsCoordinacion);
            }
            System.out.println("\n[CARGA] Datos guardados en: " + archivo);
        } catch (IOException e) {
            System.err.println("[CARGA] Error guardando CSV: " + e.getMessage());
        }
    }

    private void imprimirBanner() {
        System.out.println("╔═══════════════════════════════════════════════════════════════╗");
        System.out.println("║       GENERADOR DE CARGA — WhatsApp Distribuido              ║");
        System.out.println("╠═══════════════════════════════════════════════════════════════╣");
        System.out.println("║  Servidores:     " + Arrays.toString(getEndpointsFormatted()));
        System.out.println("║  Clientes:       " + numClientes);
        System.out.println("║  Duración:       " + duracionSegundos + " segundos");
        System.out.println("╚═══════════════════════════════════════════════════════════════╝");
    }

    private String[] getEndpointsFormatted() {
        String[] formatted = new String[puertos.length];
        for (int i = 0; i < puertos.length; i++) {
            formatted[i] = hosts[i] + ":" + puertos[i];
        }
        return formatted;
    }

    /**
     * Punto de entrada.
     * Uso: java loadtest.GeneradorCarga [host] [puerto(s)] [clientes] [duracion_seg]
     */
    public static void main(String[] args) {
        String hostDefault = args.length > 0 ? args[0] : "localhost";
        
        String[] hosts;
        int[] ports;
        
        if (args.length > 1) {
            String portArg = args[1];
            if (portArg.contains(",")) {
                String[] parts = portArg.split(",");
                hosts = new String[parts.length];
                ports = new int[parts.length];
                for (int i = 0; i < parts.length; i++) {
                    String part = parts[i].trim();
                    if (part.contains(":")) {
                        String[] hp = part.split(":");
                        hosts[i] = hp[0];
                        ports[i] = Integer.parseInt(hp[1]);
                    } else {
                        hosts[i] = hostDefault;
                        ports[i] = Integer.parseInt(part);
                    }
                }
            } else {
                if (portArg.contains(":")) {
                    String[] hp = portArg.split(":");
                    hosts = new String[]{hp[0]};
                    ports = new int[]{Integer.parseInt(hp[1])};
                } else {
                    hosts = new String[]{hostDefault};
                    ports = new int[]{Integer.parseInt(portArg)};
                }
            }
        } else {
            hosts = new String[]{"localhost", "localhost", "localhost"};
            ports = new int[]{5001, 5002, 5003};
        }
        
        int clientes = args.length > 2 ? Integer.parseInt(args[2]) : NUM_CLIENTES_DEFAULT;
        int duracion = args.length > 3 ? Integer.parseInt(args[3]) : DURACION_DEFAULT_SEG;

        GeneradorCarga carga = new GeneradorCarga(hosts, ports, clientes, duracion);
        carga.ejecutar();
    }
}
