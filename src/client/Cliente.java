package client;

import common.Mensaje;
import javax.swing.*;
import javax.swing.border.*;
import javax.swing.text.*;
import java.awt.*;
import java.awt.event.*;
import java.io.*;
import java.net.*;

/**
 * Cliente del sistema de mensajería.
 * Maneja la interfaz gráfica y la conexión de red con el servidor.
 */
public class Cliente extends JFrame {

    // Conexión de red
    private Socket socket;
    private ObjectOutputStream salida;
    private ObjectInputStream entrada;
    private String nombreUsuario;
    private boolean conectado = false;

    // Componentes de la interfaz gráfica
    private JTextPane areaMensajes;
    private JTextField campoMensaje;
    private DefaultListModel<String> modeloUsuarios;
    private JList<String> listaUsuarios;
    private DefaultListModel<String> modeloOffline;   
    private JList<String> listaOffline;                
    private JButton btnEnviar, btnArchivo, btnDesconectar;
    private JLabel lblEstado;
    private JTextField campoDestinatario; 

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

    private void construirInterfaz() {
        setTitle("WhatsApp Distribuido");
        setSize(900, 600);
        setMinimumSize(new Dimension(700, 450));
        setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);
        setLocationRelativeTo(null);
        getContentPane().setBackground(COLOR_FONDO);
        setLayout(new BorderLayout());

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

        // ===== PANEL LATERAL: Listas de usuarios =====
        JPanel panelLateral = new JPanel(new BorderLayout());
        panelLateral.setBackground(COLOR_PANEL);
        panelLateral.setPreferredSize(new Dimension(220, 0));
        panelLateral.setBorder(BorderFactory.createMatteBorder(0, 0, 0, 1, COLOR_BORDE));

        // Panel contenedor para ambas listas (online y offline)
        JPanel panelListas = new JPanel();
        panelListas.setLayout(new BoxLayout(panelListas, BoxLayout.Y_AXIS));
        panelListas.setBackground(COLOR_PANEL);

        // --- LISTA DE USUARIOS EN LÍNEA ---
        JLabel lblUsuarios = new JLabel("  ✅ Usuarios en línea");
        lblUsuarios.setFont(new Font("Segoe UI", Font.BOLD, 13));
        lblUsuarios.setForeground(COLOR_VERDE);
        lblUsuarios.setPreferredSize(new Dimension(220, 30));
        lblUsuarios.setMaximumSize(new Dimension(Integer.MAX_VALUE, 30));
        lblUsuarios.setAlignmentX(Component.LEFT_ALIGNMENT);
        panelListas.add(lblUsuarios);

        modeloUsuarios = new DefaultListModel<>();
        listaUsuarios = new JList<>(modeloUsuarios);
        listaUsuarios.setBackground(COLOR_PANEL);
        listaUsuarios.setForeground(COLOR_TEXTO);
        listaUsuarios.setSelectionBackground(COLOR_INPUT);
        listaUsuarios.setSelectionForeground(COLOR_VERDE);
        listaUsuarios.setFont(new Font("Segoe UI", Font.PLAIN, 14));
        listaUsuarios.setFixedCellHeight(35);

        JScrollPane scrollUsuarios = new JScrollPane(listaUsuarios);
        scrollUsuarios.setBorder(null);
        scrollUsuarios.getViewport().setBackground(COLOR_PANEL);
        scrollUsuarios.setAlignmentX(Component.LEFT_ALIGNMENT);
        panelListas.add(scrollUsuarios);

        // --- LISTA DE USUARIOS DESCONECTADOS ---
        JLabel lblOffline = new JLabel("  ⚪ Usuarios desconectados");
        lblOffline.setFont(new Font("Segoe UI", Font.BOLD, 13));
        lblOffline.setForeground(new Color(150, 150, 150));
        lblOffline.setPreferredSize(new Dimension(220, 30));
        lblOffline.setMaximumSize(new Dimension(Integer.MAX_VALUE, 30));
        lblOffline.setAlignmentX(Component.LEFT_ALIGNMENT);
        lblOffline.setBorder(BorderFactory.createMatteBorder(1, 0, 0, 0, COLOR_BORDE));
        panelListas.add(lblOffline);

        modeloOffline = new DefaultListModel<>();
        listaOffline = new JList<>(modeloOffline);
        listaOffline.setBackground(COLOR_PANEL);
        listaOffline.setForeground(new Color(150, 150, 150));
        listaOffline.setSelectionBackground(COLOR_INPUT);
        listaOffline.setSelectionForeground(new Color(200, 200, 200));
        listaOffline.setFont(new Font("Segoe UI", Font.ITALIC, 14));
        listaOffline.setFixedCellHeight(35);

        JScrollPane scrollOffline = new JScrollPane(listaOffline);
        scrollOffline.setBorder(null);
        scrollOffline.getViewport().setBackground(COLOR_PANEL);
        scrollOffline.setAlignmentX(Component.LEFT_ALIGNMENT);
        panelListas.add(scrollOffline);

        panelLateral.add(panelListas, BorderLayout.CENTER);

        // ===== CAMPO DESTINATARIO =====
        JPanel panelDestinatario = new JPanel(new BorderLayout(5, 5));
        panelDestinatario.setBackground(COLOR_PANEL);
        panelDestinatario.setBorder(BorderFactory.createEmptyBorder(5, 5, 8, 5));

        JLabel lblDestinatario = new JLabel("Para:");
        lblDestinatario.setFont(new Font("Segoe UI", Font.BOLD, 12));
        lblDestinatario.setForeground(COLOR_VERDE);
        panelDestinatario.add(lblDestinatario, BorderLayout.NORTH);

        campoDestinatario = new JTextField();
        campoDestinatario.setBackground(COLOR_INPUT);
        campoDestinatario.setForeground(COLOR_TEXTO);
        campoDestinatario.setCaretColor(COLOR_TEXTO);
        campoDestinatario.setFont(new Font("Segoe UI", Font.PLAIN, 13));
        campoDestinatario.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createLineBorder(COLOR_BORDE),
                BorderFactory.createEmptyBorder(4, 8, 4, 8)));
        campoDestinatario.setToolTipText("Seleccione un usuario de la lista o escriba su nombre");
        panelDestinatario.add(campoDestinatario, BorderLayout.CENTER);

        panelLateral.add(panelDestinatario, BorderLayout.SOUTH);

        // Al seleccionar un usuario ONLINE, rellenar el campo y deseleccionar la otra lista
        listaUsuarios.addListSelectionListener(e -> {
            if (!e.getValueIsAdjusting()) {
                String seleccionado = listaUsuarios.getSelectedValue();
                if (seleccionado != null) {
                    campoDestinatario.setText(seleccionado);
                    listaOffline.clearSelection();
                }
            }
        });

        // Al seleccionar un usuario OFFLINE, rellenar el campo y deseleccionar la otra lista
        listaOffline.addListSelectionListener(e -> {
            if (!e.getValueIsAdjusting()) {
                String seleccionado = listaOffline.getSelectedValue();
                if (seleccionado != null) {
                    campoDestinatario.setText(seleccionado);
                    listaUsuarios.clearSelection();
                }
            }
        });

        add(panelLateral, BorderLayout.WEST);

        // ===== PANEL CENTRAL: Área de mensajes (JTextPane para soportar botones) =====
        areaMensajes = new JTextPane();
        areaMensajes.setEditable(false);
        areaMensajes.setBackground(COLOR_FONDO);
        areaMensajes.setForeground(COLOR_TEXTO);
        areaMensajes.setFont(new Font("Segoe UI", Font.PLAIN, 14));
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

        addWindowListener(new WindowAdapter() {
            @Override
            public void windowClosing(WindowEvent e) {
                desconectar();
            }
        });
    }

    /**
     * El usuario puede elegir entre "Iniciar Sesión" o "Registrarse".
     */
    private void mostrarDialogoConexion() {
        JPanel panel = new JPanel(new GridLayout(4, 2, 5, 8));
        JTextField campoNombre = new JTextField("Usuario1");
        JPasswordField campoPassword = new JPasswordField();
        JTextField campoIP = new JTextField("localhost");
        JTextField campoPuerto = new JTextField("5000");

        panel.add(new JLabel("Nombre de usuario:"));
        panel.add(campoNombre);
        panel.add(new JLabel("Contraseña:"));
        panel.add(campoPassword);
        panel.add(new JLabel("IP del servidor:"));
        panel.add(campoIP);
        panel.add(new JLabel("Puerto:"));
        panel.add(campoPuerto);

        // Dos botones: Iniciar Sesión y Registrarse
        String[] opciones = {"Iniciar Sesión", "Registrarse", "Cancelar"};
        int resultado = JOptionPane.showOptionDialog(this, panel,
                "Conectar al Servidor",
                JOptionPane.DEFAULT_OPTION, JOptionPane.PLAIN_MESSAGE,
                null, opciones, opciones[0]);

        if (resultado == 0 || resultado == 1) {
            String nombre = campoNombre.getText().trim();
            String password = new String(campoPassword.getPassword()).trim();
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
            if (password.isEmpty()) {
                JOptionPane.showMessageDialog(this, "Ingrese una contraseña.");
                mostrarDialogoConexion();
                return;
            }

            // resultado 0 = LOGIN, resultado 1 = REGISTRO
            String tipoAuth = (resultado == 0) ? Mensaje.LOGIN : Mensaje.REGISTRO;
            conectar(nombre, password, ip, puerto, tipoAuth);
        } else {
            System.exit(0);
        }
    }

    /**
     * Establece la conexión TCP con el servidor y envía las credenciales.
     * 
     * @param nombre    Nombre de usuario
     * @param password  Contraseña
     * @param ip        IP del servidor
     * @param puerto    Puerto del servidor
     * @param tipoAuth  Mensaje.LOGIN o Mensaje.REGISTRO
     */
    private void conectar(String nombre, String password, String ip, int puerto, String tipoAuth) {
        try {
            socket = new Socket(ip, puerto);

            salida = new ObjectOutputStream(socket.getOutputStream());
            salida.flush();
            entrada = new ObjectInputStream(socket.getInputStream());

            nombreUsuario = nombre;

            // Enviar credenciales al servidor (la contraseña viaja en 'contenido')
            Mensaje msgAuth = new Mensaje(nombreUsuario, "SERVIDOR",
                    tipoAuth, password);
            salida.writeObject(msgAuth);
            salida.flush();

            // Esperar respuesta de autenticación del servidor
            Mensaje respuesta = (Mensaje) entrada.readObject();

            if (respuesta.getTipo().equals(Mensaje.AUTH_OK)) {
                // ¡Autenticación exitosa!
                conectado = true;
                setTitle("WhatsApp Distribuido — " + nombreUsuario);
                lblEstado.setText("Conectado como: " + nombreUsuario);
                setVisible(true);

                agregarMensajeSistema(respuesta.getContenido());

                // Lanzar hilo receptor para escuchar mensajes
                Thread hiloReceptor = new Thread(new HiloReceptor());
                hiloReceptor.setDaemon(true);
                hiloReceptor.start();

            } else if (respuesta.getTipo().equals(Mensaje.AUTH_FAIL)) {
                // Autenticación fallida
                socket.close();
                JOptionPane.showMessageDialog(this,
                        respuesta.getContenido(),
                        "Error de Autenticación", JOptionPane.ERROR_MESSAGE);
                mostrarDialogoConexion();
            }

        } catch (IOException e) {
            JOptionPane.showMessageDialog(this,
                    "No se pudo conectar al servidor:\n" + e.getMessage(),
                    "Error de Conexión", JOptionPane.ERROR_MESSAGE);
            mostrarDialogoConexion();
        } catch (ClassNotFoundException e) {
            JOptionPane.showMessageDialog(this,
                    "Error de comunicación con el servidor.",
                    "Error", JOptionPane.ERROR_MESSAGE);
            mostrarDialogoConexion();
        }
    }

    /**
     * Envía un mensaje de texto al destinatario seleccionado.
     */
    
    private void enviarMensajeTexto() {
        if (!conectado) return;

        String texto = campoMensaje.getText().trim();
        if (texto.isEmpty()) return;

        // Leer destinatario del campo de texto (puede ser online u offline)
        String destinatario = campoDestinatario.getText().trim();
        if (destinatario.isEmpty()) {
            agregarMensajeSistema("⚠ Escriba el nombre del destinatario en el campo 'Para:' o seleccione uno de la lista.");
            return;
        }

        try {
            // Crear mensaje y enviarlo al servidor
            Mensaje msg = new Mensaje(nombreUsuario, destinatario, Mensaje.TEXTO, texto);

            synchronized (salida) {
                salida.writeObject(msg);
                salida.flush();
                salida.reset();
            }

            agregarMensaje("Tú → " + destinatario + ": " + texto);
            campoMensaje.setText("");

        } catch (IOException e) {
            agregarMensajeSistema("❌ Error al enviar mensaje: " + e.getMessage());
        }
    }

    /**
     * Permite seleccionar un archivo del disco y enviarlo.
     */
    private void enviarArchivo() {
        if (!conectado) return;

        // Leer destinatario del campo de texto (puede ser online u offline)
        String destinatario = campoDestinatario.getText().trim();
        if (destinatario.isEmpty()) {
            agregarMensajeSistema("⚠ Escriba el nombre del destinatario en el campo 'Para:' o seleccione uno de la lista.");
            return;
        }

        JFileChooser fileChooser = new JFileChooser();
        fileChooser.setDialogTitle("Seleccionar archivo para enviar");
        int resultado = fileChooser.showOpenDialog(this);

        if (resultado == JFileChooser.APPROVE_OPTION) {
            File archivo = fileChooser.getSelectedFile();

            // Limitar tamaño de archivo
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

                // Empaquetar archivo en un mensaje
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
            try {
                StyledDocument doc = areaMensajes.getStyledDocument();
                doc.insertString(doc.getLength(), texto + "\n", null);
                areaMensajes.setCaretPosition(doc.getLength());
            } catch (BadLocationException e) {
                e.printStackTrace();
            }
        });
    }

    private void agregarMensajeSistema(String texto) {
        agregarMensaje("  [SISTEMA] " + texto);
    }

    /**
     * Inserta un botón de descarga dentro del área de mensajes.
     * El botón permite al usuario elegir dónde guardar el archivo.
     */
    private void agregarBotonDescarga(String emisor, String nombreArchivo, byte[] datos) {
        SwingUtilities.invokeLater(() -> {
            try {
                StyledDocument doc = areaMensajes.getStyledDocument();
                doc.insertString(doc.getLength(), emisor + ": \uD83D\uDCCE ", null);

                // Crear botón azul de descarga
                JButton btnDescargar = new JButton("\u2B07 Descargar: " + nombreArchivo +
                        " (" + formatearTamano(datos.length) + ")");
                btnDescargar.setBackground(new Color(30, 120, 220));
                btnDescargar.setForeground(Color.WHITE);
                btnDescargar.setFont(new Font("Segoe UI", Font.BOLD, 12));
                btnDescargar.setFocusPainted(false);
                btnDescargar.setBorderPainted(false);
                btnDescargar.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));

                btnDescargar.addActionListener(e -> {
                    JFileChooser chooser = new JFileChooser();
                    chooser.setSelectedFile(new File(nombreArchivo));
                    int resultado = chooser.showSaveDialog(this);
                    if (resultado == JFileChooser.APPROVE_OPTION) {
                        try (FileOutputStream fos = new FileOutputStream(chooser.getSelectedFile())) {
                            fos.write(datos);
                            agregarMensajeSistema("\u2705 Archivo guardado: " + chooser.getSelectedFile().getAbsolutePath());
                        } catch (IOException ex) {
                            agregarMensajeSistema("\u274C Error guardando archivo: " + ex.getMessage());
                        }
                    }
                });

                // Insertar el botón como componente en el JTextPane
                areaMensajes.setCaretPosition(doc.getLength());
                areaMensajes.insertComponent(btnDescargar);
                doc.insertString(doc.getLength(), "\n", null);
                areaMensajes.setCaretPosition(doc.getLength());
            } catch (BadLocationException e) {
                e.printStackTrace();
            }
        });
    }

    private String formatearTamano(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        return String.format("%.1f MB", bytes / (1024.0 * 1024));
    }

    /**
     * Hilo encargado de escuchar mensajes entrantes del servidor
     * de forma continua sin bloquear la interfaz gráfica.
     */
    private class HiloReceptor implements Runnable {
        @Override
        public void run() {
            try {
                while (conectado) {
                    // Deserializar mensaje recibido
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
                            // Actualizar lista de usuarios conectados
                            actualizarListaUsuarios(mensaje.getContenido());
                            break;

                        case Mensaje.LISTA_OFFLINE:
                            // Actualizar lista de usuarios desconectados
                            actualizarListaOffline(mensaje.getContenido());
                            break;

                        case Mensaje.NOTIFICACION:
                            // Notificación del sistema
                            agregarMensajeSistema(mensaje.getContenido());
                            break;

                        case Mensaje.HISTORIAL:
                            // Mensaje del historial de conversaciones 
                            agregarMensaje("  [📜] " + mensaje.getEmisor() + ": " + mensaje.getContenido());
                            break;

                        default:
                            agregarMensajeSistema("Mensaje desconocido: " + mensaje.getTipo());
                            break;
                    }
                }
            } catch (IOException e) {
                // Pérdida de conexión con el servidor
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

        private void recibirArchivo(Mensaje mensaje) {
            // En vez de descargar automáticamente, mostrar botón azul de descarga
            agregarBotonDescarga(
                    mensaje.getEmisor(),
                    mensaje.getNombreArchivo(),
                    mensaje.getDatosAdjuntos()
            );
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
                if (seleccionado != null && modeloUsuarios.contains(seleccionado)) {
                    listaUsuarios.setSelectedValue(seleccionado, true);
                }
            });
        }

        /**
         * Actualiza la lista visual de usuarios desconectados (offline).
         */
        private void actualizarListaOffline(String listaCSV) {
            SwingUtilities.invokeLater(() -> {
                String seleccionado = listaOffline.getSelectedValue();
                modeloOffline.clear();
                if (listaCSV != null && !listaCSV.isEmpty()) {
                    for (String usuario : listaCSV.split(",")) {
                        if (!usuario.equals(nombreUsuario)) {
                            modeloOffline.addElement(usuario);
                        }
                    }
                }
                if (seleccionado != null && modeloOffline.contains(seleccionado)) {
                    listaOffline.setSelectedValue(seleccionado, true);
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
