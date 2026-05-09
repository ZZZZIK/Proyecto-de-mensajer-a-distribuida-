package client;

import common.Mensaje;
import javax.swing.*;
import javax.swing.border.*;
import java.awt.*;
import java.awt.event.*;
import java.io.*;
import java.net.*;

/**
 * =====================================================================
 * CLIENTE DEL SISTEMA DISTRIBUIDO — Interfaz Swing
 * =====================================================================
 * 
 * TRANSPARENCIA DE ACCESO:
 * El cliente se comunica con el servidor central enviando objetos
 * Mensaje. No necesita conocer la ubicación (IP/puerto) de otros
 * clientes. Solo conoce la dirección del servidor.
 * 
 * TRANSPARENCIA DE UBICACIÓN:
 * Los mensajes se dirigen por NOMBRE de usuario, no por dirección
 * de red. El servidor resuelve la ubicación internamente.
 * 
 * CONCURRENCIA:
 * El cliente utiliza un hilo separado (HiloReceptor) para escuchar
 * mensajes entrantes del servidor de forma continua, mientras el
 * hilo principal de Swing maneja la interfaz gráfica y el envío.
 * =====================================================================
 */
public class Cliente extends JFrame {

    // Conexión de red
    private Socket socket;
    private ObjectOutputStream salida;
    private ObjectInputStream entrada;
    private String nombreUsuario;
    private boolean conectado = false;

    // Componentes de la interfaz gráfica
    private JTextArea areaMensajes;
    private JTextField campoMensaje;
    private DefaultListModel<String> modeloUsuarios;
    private JList<String> listaUsuarios;
    private JButton btnEnviar, btnArchivo, btnDesconectar;
    private JLabel lblEstado;

    // Colores del tema (inspirado en WhatsApp)
    private static final Color COLOR_FONDO       = new Color(17, 27, 33);
    private static final Color COLOR_PANEL       = new Color(32, 44, 51);
    private static final Color COLOR_HEADER      = new Color(0, 92, 75);
    private static final Color COLOR_TEXTO       = new Color(233, 237, 239);
    private static final Color COLOR_VERDE       = new Color(37, 211, 102);
    private static final Color COLOR_INPUT       = new Color(42, 57, 66);
    private static final Color COLOR_BORDE       = new Color(52, 67, 76);

    public Cliente() {
        construirInterfaz();
        mostrarDialogoConexion();
    }

    /**
     * Construye la interfaz gráfica Swing.
     */
    private void construirInterfaz() {
        setTitle("WhatsApp Distribuido");
        setSize(900, 600);
        setMinimumSize(new Dimension(700, 450));
        setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);
        setLocationRelativeTo(null);
        getContentPane().setBackground(COLOR_FONDO);
        setLayout(new BorderLayout());

        // ===== HEADER =====
        JPanel panelHeader = new JPanel(new BorderLayout());
        panelHeader.setBackground(COLOR_HEADER);
        panelHeader.setPreferredSize(new Dimension(0, 50));
        panelHeader.setBorder(BorderFactory.createEmptyBorder(5, 15, 5, 15));

        JLabel lblTitulo = new JLabel("WhatsApp Distribuido");
        lblTitulo.setFont(new Font("Segoe UI", Font.BOLD, 18));
        lblTitulo.setForeground(Color.WHITE);
        panelHeader.add(lblTitulo, BorderLayout.WEST);

        lblEstado = new JLabel("Desconectado");
        lblEstado.setFont(new Font("Segoe UI", Font.PLAIN, 12));
        lblEstado.setForeground(new Color(180, 220, 200));
        panelHeader.add(lblEstado, BorderLayout.EAST);

        btnDesconectar = new JButton("Salir");
        btnDesconectar.setBackground(new Color(200, 60, 60));
        btnDesconectar.setForeground(Color.WHITE);
        btnDesconectar.setFocusPainted(false);
        btnDesconectar.setBorderPainted(false);
        btnDesconectar.setFont(new Font("Segoe UI", Font.BOLD, 12));
        btnDesconectar.addActionListener(e -> desconectar());

        JPanel panelHeaderDer = new JPanel(new FlowLayout(FlowLayout.RIGHT));
        panelHeaderDer.setOpaque(false);
        panelHeaderDer.add(lblEstado);
        panelHeaderDer.add(Box.createHorizontalStrut(10));
        panelHeaderDer.add(btnDesconectar);
        panelHeader.add(panelHeaderDer, BorderLayout.EAST);

        add(panelHeader, BorderLayout.NORTH);

        // ===== PANEL LATERAL: Lista de usuarios =====
        JPanel panelLateral = new JPanel(new BorderLayout());
        panelLateral.setBackground(COLOR_PANEL);
        panelLateral.setPreferredSize(new Dimension(200, 0));
        panelLateral.setBorder(BorderFactory.createMatteBorder(0, 0, 0, 1, COLOR_BORDE));

        JLabel lblUsuarios = new JLabel("  Usuarios en línea");
        lblUsuarios.setFont(new Font("Segoe UI", Font.BOLD, 13));
        lblUsuarios.setForeground(COLOR_VERDE);
        lblUsuarios.setPreferredSize(new Dimension(0, 35));
        panelLateral.add(lblUsuarios, BorderLayout.NORTH);

        modeloUsuarios = new DefaultListModel<>();
        listaUsuarios = new JList<>(modeloUsuarios);
        listaUsuarios.setBackground(COLOR_PANEL);
        listaUsuarios.setForeground(COLOR_TEXTO);
        listaUsuarios.setSelectionBackground(COLOR_INPUT);
        listaUsuarios.setSelectionForeground(COLOR_VERDE);
        listaUsuarios.setFont(new Font("Segoe UI", Font.PLAIN, 14));
        listaUsuarios.setFixedCellHeight(40);

        JScrollPane scrollUsuarios = new JScrollPane(listaUsuarios);
        scrollUsuarios.setBorder(null);
        scrollUsuarios.getViewport().setBackground(COLOR_PANEL);
        panelLateral.add(scrollUsuarios, BorderLayout.CENTER);

        add(panelLateral, BorderLayout.WEST);

        // ===== PANEL CENTRAL: Área de mensajes =====
        areaMensajes = new JTextArea();
        areaMensajes.setEditable(false);
        areaMensajes.setBackground(COLOR_FONDO);
        areaMensajes.setForeground(COLOR_TEXTO);
        areaMensajes.setFont(new Font("Segoe UI", Font.PLAIN, 14));
        areaMensajes.setLineWrap(true);
        areaMensajes.setWrapStyleWord(true);
        areaMensajes.setMargin(new Insets(10, 10, 10, 10));

        JScrollPane scrollMensajes = new JScrollPane(areaMensajes);
        scrollMensajes.setBorder(null);
        scrollMensajes.getViewport().setBackground(COLOR_FONDO);
        add(scrollMensajes, BorderLayout.CENTER);

        // ===== PANEL INFERIOR: Input y botones =====
        JPanel panelInput = new JPanel(new BorderLayout(5, 0));
        panelInput.setBackground(COLOR_PANEL);
        panelInput.setBorder(BorderFactory.createEmptyBorder(8, 10, 8, 10));

        btnArchivo = new JButton("\uD83D\uDCCE Archivo");
        btnArchivo.setBackground(COLOR_INPUT);
        btnArchivo.setForeground(COLOR_TEXTO);
        btnArchivo.setFont(new Font("Segoe UI", Font.PLAIN, 13));
        btnArchivo.setFocusPainted(false);
        btnArchivo.setBorderPainted(false);
        btnArchivo.setPreferredSize(new Dimension(100, 35));
        btnArchivo.addActionListener(e -> enviarArchivo());

        campoMensaje = new JTextField();
        campoMensaje.setBackground(COLOR_INPUT);
        campoMensaje.setForeground(COLOR_TEXTO);
        campoMensaje.setCaretColor(COLOR_TEXTO);
        campoMensaje.setFont(new Font("Segoe UI", Font.PLAIN, 14));
        campoMensaje.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createLineBorder(COLOR_BORDE),
                BorderFactory.createEmptyBorder(5, 10, 5, 10)));
        campoMensaje.addActionListener(e -> enviarMensajeTexto());

        btnEnviar = new JButton("Enviar ➤");
        btnEnviar.setBackground(COLOR_VERDE);
        btnEnviar.setForeground(Color.WHITE);
        btnEnviar.setFont(new Font("Segoe UI", Font.BOLD, 13));
        btnEnviar.setFocusPainted(false);
        btnEnviar.setBorderPainted(false);
        btnEnviar.setPreferredSize(new Dimension(100, 35));
        btnEnviar.addActionListener(e -> enviarMensajeTexto());

        panelInput.add(btnArchivo, BorderLayout.WEST);
        panelInput.add(campoMensaje, BorderLayout.CENTER);
        panelInput.add(btnEnviar, BorderLayout.EAST);
        add(panelInput, BorderLayout.SOUTH);

        // Manejar cierre de ventana
        addWindowListener(new WindowAdapter() {
            @Override
            public void windowClosing(WindowEvent e) {
                desconectar();
            }
        });
    }

    /**
     * Muestra el diálogo de conexión para ingresar nombre, IP y puerto.
     */
    private void mostrarDialogoConexion() {
        JPanel panel = new JPanel(new GridLayout(3, 2, 5, 5));
        JTextField campoNombre = new JTextField("Usuario1");
        JTextField campoIP = new JTextField("localhost");
        JTextField campoPuerto = new JTextField("5000");

        panel.add(new JLabel("Nombre de usuario:"));
        panel.add(campoNombre);
        panel.add(new JLabel("IP del servidor:"));
        panel.add(campoIP);
        panel.add(new JLabel("Puerto:"));
        panel.add(campoPuerto);

        int resultado = JOptionPane.showConfirmDialog(this, panel,
                "Conectar al Servidor", JOptionPane.OK_CANCEL_OPTION, JOptionPane.PLAIN_MESSAGE);

        if (resultado == JOptionPane.OK_OPTION) {
            String nombre = campoNombre.getText().trim();
            String ip = campoIP.getText().trim();
            int puerto;
            try {
                puerto = Integer.parseInt(campoPuerto.getText().trim());
            } catch (NumberFormatException e) {
                puerto = 5000;
            }

            if (nombre.isEmpty()) {
                JOptionPane.showMessageDialog(this, "Ingrese un nombre de usuario.");
                mostrarDialogoConexion();
                return;
            }

            conectar(nombre, ip, puerto);
        } else {
            System.exit(0);
        }
    }

    /**
     * Establece la conexión TCP con el servidor.
     * 
     * COMUNICACIÓN TCP: Se crea un Socket que se conecta al servidor.
     * Se crean ObjectOutputStream/ObjectInputStream para marshalling.
     * Se envía un mensaje CONECTAR como primer mensaje del protocolo.
     */
    private void conectar(String nombre, String ip, int puerto) {
        try {
            // ===== COMUNICACIÓN TCP: Conexión al servidor =====
            socket = new Socket(ip, puerto);

            // ===== MARSHALLING: Streams de objetos =====
            salida = new ObjectOutputStream(socket.getOutputStream());
            salida.flush();
            entrada = new ObjectInputStream(socket.getInputStream());

            nombreUsuario = nombre;
            conectado = true;

            // Enviar mensaje de conexión (protocolo de login)
            Mensaje msgConectar = new Mensaje(nombreUsuario, "SERVIDOR",
                    Mensaje.CONECTAR, "Solicitud de conexión");
            salida.writeObject(msgConectar);
            salida.flush();

            setTitle("WhatsApp 2 Distribuido — " + nombreUsuario);
            lblEstado.setText("Conectado como: " + nombreUsuario);
            setVisible(true);

            agregarMensajeSistema("Conectado al servidor " + ip + ":" + puerto);

            // ===== CONCURRENCIA: Hilo receptor =====
            // Se lanza un hilo separado para escuchar mensajes entrantes
            // del servidor de forma continua, sin bloquear la interfaz.
            Thread hiloReceptor = new Thread(new HiloReceptor());
            hiloReceptor.setDaemon(true);
            hiloReceptor.start();

        } catch (IOException e) {
            // ===== RESILIENCIA: Fallo de conexión =====
            JOptionPane.showMessageDialog(this,
                    "No se pudo conectar al servidor:\n" + e.getMessage(),
                    "Error de Conexión", JOptionPane.ERROR_MESSAGE);
            mostrarDialogoConexion();
        }
    }

    /**
     * ===== FUNCIÓN 1: Envío de mensaje de texto =====
     * 
     * TRANSPARENCIA DE ACCESO: El usuario selecciona un destinatario
     * por NOMBRE (no por IP), escribe un texto, y se crea un objeto
     * Mensaje que se serializa y envía al servidor. El servidor se
     * encarga de resolver la ubicación del destinatario.
     */

    
    private void enviarMensajeTexto() {
        if (!conectado) return;

        String texto = campoMensaje.getText().trim();
        if (texto.isEmpty()) return;

        String destinatario = listaUsuarios.getSelectedValue();
        if (destinatario == null) {
            agregarMensajeSistema("⚠ Seleccione un usuario de la lista para enviar el mensaje.");
            return;
        }

        try {
            // TRANSPARENCIA: Se indica el nombre del destinatario, no su IP
            Mensaje msg = new Mensaje(nombreUsuario, destinatario, Mensaje.TEXTO, texto);

            // MARSHALLING: Serialización del objeto para envío por TCP
            synchronized (salida) {
                salida.writeObject(msg);
                salida.flush();
                salida.reset();
            }

            agregarMensaje("Tú → " + destinatario + ": " + texto);
            campoMensaje.setText("");

        } catch (IOException e) {
            // RESILIENCIA: Fallo al enviar
            agregarMensajeSistema("❌ Error al enviar mensaje: " + e.getMessage());
        }
    }

    /**
     * ===== FUNCIÓN 2: Envío de archivo multimedia =====
     * 
     * MARSHALLING: El archivo se lee como byte[] y se empaqueta dentro
     * del objeto Mensaje en el campo datosAdjuntos. Java serializa
     * automáticamente el array de bytes junto con el resto del objeto.
     */
    private void enviarArchivo() {
        if (!conectado) return;

        String destinatario = listaUsuarios.getSelectedValue();
        if (destinatario == null) {
            agregarMensajeSistema("⚠ Seleccione un usuario de la lista antes de enviar un archivo.");
            return;
        }

        JFileChooser fileChooser = new JFileChooser();
        fileChooser.setDialogTitle("Seleccionar archivo para enviar");
        int resultado = fileChooser.showOpenDialog(this);

        if (resultado == JFileChooser.APPROVE_OPTION) {
            File archivo = fileChooser.getSelectedFile();

            // Limitar tamaño de archivo (10 MB)
            if (archivo.length() > 10 * 1024 * 1024) {
                agregarMensajeSistema("❌ El archivo excede el límite de 10 MB.");
                return;
            }

            try {
                // Leer archivo como array de bytes
                byte[] datosArchivo = new byte[(int) archivo.length()];
                try (FileInputStream fis = new FileInputStream(archivo)) {
                    fis.read(datosArchivo);
                }

                // MARSHALLING: Crear Mensaje con datos binarios adjuntos
                Mensaje msg = new Mensaje(nombreUsuario, destinatario,
                        archivo.getName(), datosArchivo);

                // Serializar y enviar
                synchronized (salida) {
                    salida.writeObject(msg);
                    salida.flush();
                    salida.reset();
                }

                agregarMensaje("Tú → " + destinatario + ": 📎 Archivo enviado: " +
                        archivo.getName() + " (" + formatearTamano(archivo.length()) + ")");

            } catch (IOException e) {
                // RESILIENCIA: Fallo al leer o enviar archivo
                agregarMensajeSistema("❌ Error al enviar archivo: " + e.getMessage());
            }
        }
    }

    /**
     * Desconecta el cliente del servidor de forma ordenada.
     */
    private void desconectar() {
        if (conectado) {
            try {
                conectado = false;
                Mensaje msgDesconectar = new Mensaje(nombreUsuario, "SERVIDOR",
                        Mensaje.DESCONECTAR, "Desconexión voluntaria");
                synchronized (salida) {
                    salida.writeObject(msgDesconectar);
                    salida.flush();
                }
            } catch (IOException e) {
                // Ignorar error al desconectar
            } finally {
                try { if (socket != null) socket.close(); } catch (IOException e) {}
            }
        }
        System.exit(0);
    }

    // ===== Métodos auxiliares de UI =====

    private void agregarMensaje(String texto) {
        SwingUtilities.invokeLater(() -> {
            areaMensajes.append(texto + "\n");
            areaMensajes.setCaretPosition(areaMensajes.getDocument().getLength());
        });
    }

    private void agregarMensajeSistema(String texto) {
        agregarMensaje("  [SISTEMA] " + texto);
    }

    private String formatearTamano(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        return String.format("%.1f MB", bytes / (1024.0 * 1024));
    }

    /**
     * ===== CONCURRENCIA: Hilo receptor de mensajes =====
     * 
     * Este hilo interno se ejecuta de forma CONCURRENTE con el hilo
     * de Swing (interfaz gráfica). Lee mensajes del servidor de forma
     * continua sin bloquear la UI, permitiendo que el usuario siga
     * interactuando con la aplicación mientras recibe mensajes.
     * 
     * RESILIENCIA: Si ocurre un IOException (desconexión del servidor),
     * el hilo termina y notifica al usuario, pero no crashea la app.
     */
    private class HiloReceptor implements Runnable {
        @Override
        public void run() {
            try {
                while (conectado) {
                    // MARSHALLING: Deserialización del objeto recibido
                    Mensaje mensaje = (Mensaje) entrada.readObject();

                    switch (mensaje.getTipo()) {
                        case Mensaje.TEXTO:
                            // FUNCIÓN 1: Recepción de mensaje de texto
                            agregarMensaje(mensaje.getEmisor() + ": " + mensaje.getContenido());
                            break;

                        case Mensaje.ARCHIVO:
                            // FUNCIÓN 2: Recepción de archivo multimedia
                            recibirArchivo(mensaje);
                            break;

                        case Mensaje.LISTA_USUARIOS:
                            // Actualizar lista de usuarios en la interfaz
                            actualizarListaUsuarios(mensaje.getContenido());
                            break;

                        case Mensaje.NOTIFICACION:
                            // Notificación del sistema
                            agregarMensajeSistema(mensaje.getContenido());
                            break;

                        default:
                            agregarMensajeSistema("Mensaje desconocido: " + mensaje.getTipo());
                            break;
                    }
                }
            } catch (IOException e) {
                // RESILIENCIA: El servidor se cayó o se perdió la conexión
                if (conectado) {
                    conectado = false;
                    agregarMensajeSistema("❌ Se perdió la conexión con el servidor.");
                    SwingUtilities.invokeLater(() ->
                        lblEstado.setText("Desconectado (error de red)")
                    );
                }
            } catch (ClassNotFoundException e) {
                agregarMensajeSistema("❌ Error de deserialización: " + e.getMessage());
            }
        }

        /**
         * Guarda un archivo recibido en el directorio "archivos_recibidos".
         */
        private void recibirArchivo(Mensaje mensaje) {
            try {
                File directorio = new File("archivos_recibidos");
                if (!directorio.exists()) directorio.mkdirs();

                File archivoDestino = new File(directorio, mensaje.getNombreArchivo());
                try (FileOutputStream fos = new FileOutputStream(archivoDestino)) {
                    fos.write(mensaje.getDatosAdjuntos());
                }

                agregarMensaje(mensaje.getEmisor() + ": 📎 Archivo recibido: " +
                        mensaje.getNombreArchivo() + " (" +
                        formatearTamano(mensaje.getDatosAdjuntos().length) +
                        ") → Guardado en: " + archivoDestino.getAbsolutePath());

            } catch (IOException e) {
                agregarMensajeSistema("❌ Error guardando archivo: " + e.getMessage());
            }
        }

        /**
         * Actualiza la lista visual de usuarios conectados.
         */
        private void actualizarListaUsuarios(String listaCSV) {
            SwingUtilities.invokeLater(() -> {
                String seleccionado = listaUsuarios.getSelectedValue();
                modeloUsuarios.clear();
                if (listaCSV != null && !listaCSV.isEmpty()) {
                    for (String usuario : listaCSV.split(",")) {
                        if (!usuario.equals(nombreUsuario)) {
                            modeloUsuarios.addElement(usuario);
                        }
                    }
                }
                // Restaurar selección si el usuario aún existe
                if (seleccionado != null && modeloUsuarios.contains(seleccionado)) {
                    listaUsuarios.setSelectedValue(seleccionado, true);
                }
            });
        }
    }

    /**
     * Punto de entrada del cliente.
     */
    public static void main(String[] args) {
        // Usar Look and Feel del sistema operativo
        try {
            UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
        } catch (Exception e) { /* usar default */ }

        SwingUtilities.invokeLater(() -> new Cliente());
    }
}
