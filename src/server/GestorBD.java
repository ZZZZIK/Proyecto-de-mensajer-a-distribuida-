package server;

import common.Mensaje;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * =====================================================================
 * GESTOR DE BASE DE DATOS — Patrón DAO (Data Access Object)
 * =====================================================================
 * 
 * Esta clase aísla toda la lógica de acceso a la base de datos MySQL,
 * manteniendo el código del Servidor y ManejadorCliente limpio y
 * desacoplado de la tecnología de persistencia.
 * 
 * PERSISTENCIA:
 * Reemplaza las variables en RAM (HashSet, HashMap) por consultas a
 * MySQL, permitiendo que los datos sobrevivan reinicios del servidor.
 * 
 * SEGURIDAD:
 * - PreparedStatement para prevenir inyección SQL.
 * - Contraseñas hasheadas con SHA-256 (irreversible).
 * - Contenido de mensajes cifrado con AES-128 (reversible solo con clave).
 * 
 * TABLAS GESTIONADAS:
 * - usuarios: Registro y autenticación (con hash de contraseña).
 * - mensajes_offline: Buzón temporal (se borran al entregar).
 * - historial_mensajes: Registro permanente de TODAS las conversaciones.
 * 
 * LÍMITE DE BUZÓN OFFLINE:
 * Mantiene un límite máximo de 100 mensajes offline por usuario.
 * =====================================================================
 */
public class GestorBD {

    // Configuración de conexión a MySQL (XAMPP por defecto)
    private static final String URL = "jdbc:mysql://localhost:3306/chat_distribuido?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
    private static final String USUARIO_BD = "root";
    private static final String PASSWORD_BD = "";  // XAMPP por defecto no tiene contraseña

    // Límite máximo de mensajes offline por usuario
    private static final int MAX_MENSAJES_OFFLINE = 100;

    /**
     * Establece una conexión con la base de datos MySQL.
     */
    private static Connection conectar() throws SQLException {
        return DriverManager.getConnection(URL, USUARIO_BD, PASSWORD_BD);
    }

    /**
     * Verifica que la conexión a la base de datos esté operativa.
     */
    public static boolean verificarConexion() {
        try (Connection conn = conectar()) {
            System.out.println("[GESTOR-BD] Conexión a MySQL exitosa.");
            return true;
        } catch (SQLException e) {
            System.err.println("[GESTOR-BD] ERROR: No se pudo conectar a MySQL.");
            System.err.println("[GESTOR-BD] Verifica que XAMPP (MySQL) esté encendido.");
            System.err.println("[GESTOR-BD] Detalle: " + e.getMessage());
            return false;
        }
    }

    // =====================================================================
    // MÉTODOS DE AUTENTICACIÓN (con Hash SHA-256)
    // =====================================================================

    /**
     * Registra un nuevo usuario en la base de datos.
     * La contraseña se almacena como hash SHA-256 (irreversible).
     * 
     * @param nombre   Nombre de usuario (debe ser único)
     * @param password Contraseña en texto plano (será hasheada)
     * @return true si el registro fue exitoso, false si el nombre ya existe
     */
    public static boolean registrarUsuario(String nombre, String password) {
        String sql = "INSERT INTO usuarios (nombre_usuario, password) VALUES (?, ?)";
        try (Connection conn = conectar();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, nombre);
            stmt.setString(2, Seguridad.hashPassword(password)); // Hash SHA-256
            stmt.executeUpdate();

            System.out.println("[GESTOR-BD] Usuario registrado en BD: " + nombre + " (contraseña hasheada)");
            return true;

        } catch (SQLException e) {
            if (e.getErrorCode() == 1062) { // Duplicate entry
                System.out.println("[GESTOR-BD] Registro rechazado: '" + nombre + "' ya existe en BD.");
            } else {
                System.err.println("[GESTOR-BD] Error registrando usuario: " + e.getMessage());
            }
            return false;
        }
    }

    /**
     * Autentica un usuario comparando el hash de la contraseña ingresada
     * con el hash almacenado en la BD.
     * 
     * @param nombre   Nombre de usuario
     * @param password Contraseña en texto plano (será hasheada para comparar)
     * @return true si las credenciales son correctas
     */
    public static boolean autenticarUsuario(String nombre, String password) {
        String sql = "SELECT password FROM usuarios WHERE nombre_usuario = ?";
        try (Connection conn = conectar();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, nombre);
            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                String hashAlmacenado = rs.getString("password");
                String hashIngresado = Seguridad.hashPassword(password); // Hash SHA-256
                boolean valido = hashAlmacenado.equals(hashIngresado);
                System.out.println("[GESTOR-BD] Login de '" + nombre + "': " +
                        (valido ? "ÉXITO" : "CONTRASEÑA INCORRECTA"));
                return valido;
            } else {
                System.out.println("[GESTOR-BD] Login fallido: '" + nombre + "' no existe en BD.");
                return false;
            }

        } catch (SQLException e) {
            System.err.println("[GESTOR-BD] Error autenticando: " + e.getMessage());
            return false;
        }
    }

    /**
     * Verifica si un usuario existe en la base de datos.
     */
    public static boolean usuarioExiste(String nombre) {
        String sql = "SELECT 1 FROM usuarios WHERE nombre_usuario = ?";
        try (Connection conn = conectar();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, nombre);
            ResultSet rs = stmt.executeQuery();
            return rs.next();

        } catch (SQLException e) {
            System.err.println("[GESTOR-BD] Error verificando usuario: " + e.getMessage());
            return false;
        }
    }

    /**
     * Obtiene la lista de TODOS los usuarios registrados en la BD.
     */
    public static List<String> obtenerUsuariosRegistrados() {
        List<String> usuarios = new ArrayList<>();
        String sql = "SELECT nombre_usuario FROM usuarios ORDER BY nombre_usuario";
        try (Connection conn = conectar();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                usuarios.add(rs.getString("nombre_usuario"));
            }

        } catch (SQLException e) {
            System.err.println("[GESTOR-BD] Error obteniendo usuarios: " + e.getMessage());
        }
        return usuarios;
    }

    // =====================================================================
    // MÉTODOS DE BUZÓN OFFLINE (con Cifrado AES)
    // =====================================================================

    /**
     * Guarda un mensaje en el buzón offline, cifrando el contenido con AES.
     * 
     * ARCHIVOS: Los datos binarios se guardan directamente en la BD
     * como LONGBLOB en la columna datos_adjuntos. El contenido textual
     * se cifra con AES antes de almacenarse.
     * 
     * @param msg Objeto Mensaje a guardar
     * @return 0 si se guardó OK, 1 si buzón lleno, 2 si error de BD
     */
    public static int guardarMensajeOffline(Mensaje msg) {
        String sqlCount = "SELECT COUNT(*) AS total FROM mensajes_offline WHERE receptor = ?";
        String sqlInsert = "INSERT INTO mensajes_offline (emisor, receptor, tipo, contenido, datos_adjuntos, nombre_archivo, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)";

        try (Connection conn = conectar()) {

            // Verificar límite de 100 mensajes
            try (PreparedStatement stmtCount = conn.prepareStatement(sqlCount)) {
                stmtCount.setString(1, msg.getReceptor());
                ResultSet rs = stmtCount.executeQuery();
                if (rs.next() && rs.getInt("total") >= MAX_MENSAJES_OFFLINE) {
                    System.out.println("[GESTOR-BD] Buzón offline de '" + msg.getReceptor() +
                            "' lleno (" + MAX_MENSAJES_OFFLINE + "). Mensaje rechazado.");
                    return 1; // Buzón lleno
                }
            }

            // Guardar en BD (archivos como LONGBLOB directamente en la BD)
            try (PreparedStatement stmtInsert = conn.prepareStatement(sqlInsert)) {
                stmtInsert.setString(1, msg.getEmisor());
                stmtInsert.setString(2, msg.getReceptor());
                stmtInsert.setString(3, msg.getTipo());
                stmtInsert.setString(4, Seguridad.encriptar(msg.getContenido())); // Contenido cifrado

                // Guardar datos binarios del archivo directamente en la BD
                if (msg.getTipo().equals(Mensaje.ARCHIVO) && msg.getDatosAdjuntos() != null) {
                    stmtInsert.setBytes(5, msg.getDatosAdjuntos());
                    System.out.println("[GESTOR-BD] Archivo offline guardado en BD: " +
                            msg.getNombreArchivo() + " (" + msg.getDatosAdjuntos().length + " bytes)");
                } else {
                    stmtInsert.setNull(5, Types.BLOB);
                }

                stmtInsert.setString(6, msg.getNombreArchivo());
                stmtInsert.setLong(7, msg.getTimestamp());
                stmtInsert.executeUpdate();
            }

            System.out.println("[GESTOR-BD] Mensaje offline guardado (cifrado): " +
                    msg.getEmisor() + " -> " + msg.getReceptor());
            return 0; // Éxito

        } catch (SQLException e) {
            System.err.println("[GESTOR-BD] Error guardando mensaje offline: " + e.getMessage());
            return 2; // Error de BD
        }
    }

    /**
     * Extrae todos los mensajes offline de un usuario, los descifra y los borra.
     * Se llama cuando el usuario vuelve a conectarse.
     * 
     * Los archivos adjuntos se leen directamente desde la BD (LONGBLOB).
     * 
     * @param receptor Nombre del usuario que se reconectó
     * @return Lista de mensajes pendientes descifrados
     */
    public static List<Mensaje> obtenerYBorrarMensajesOffline(String receptor) {
        List<Mensaje> mensajes = new ArrayList<>();
        String sqlSelect = "SELECT emisor, receptor, tipo, contenido, datos_adjuntos, nombre_archivo, timestamp FROM mensajes_offline WHERE receptor = ? ORDER BY timestamp ASC";
        String sqlDelete = "DELETE FROM mensajes_offline WHERE receptor = ?";

        try (Connection conn = conectar()) {

            // Extraer los mensajes
            try (PreparedStatement stmtSelect = conn.prepareStatement(sqlSelect)) {
                stmtSelect.setString(1, receptor);
                ResultSet rs = stmtSelect.executeQuery();

                while (rs.next()) {
                    String tipo = rs.getString("tipo");
                    String nombreArchivo = rs.getString("nombre_archivo");

                    Mensaje msg;
                    if (tipo.equals(Mensaje.ARCHIVO) && nombreArchivo != null) {
                        // ARCHIVO: Leer bytes directamente desde la BD (LONGBLOB)
                        byte[] datos = rs.getBytes("datos_adjuntos");

                        if (datos != null && datos.length > 0) {
                            msg = new Mensaje(
                                    rs.getString("emisor"),
                                    rs.getString("receptor"),
                                    nombreArchivo,
                                    datos
                            );
                            System.out.println("[GESTOR-BD] Archivo offline recuperado de BD: " +
                                    nombreArchivo + " (" + datos.length + " bytes)");
                        } else {
                            System.err.println("[GESTOR-BD] Archivo offline sin datos en BD: " + nombreArchivo);
                            continue;
                        }
                    } else {
                        // TEXTO u otro tipo: descifrar contenido normalmente
                        msg = new Mensaje(
                                rs.getString("emisor"),
                                rs.getString("receptor"),
                                tipo,
                                Seguridad.desencriptar(rs.getString("contenido"))
                        );
                    }
                    mensajes.add(msg);
                }
            }

            // Borrar los mensajes entregados
            if (!mensajes.isEmpty()) {
                try (PreparedStatement stmtDelete = conn.prepareStatement(sqlDelete)) {
                    stmtDelete.setString(1, receptor);
                    stmtDelete.executeUpdate();
                }
                System.out.println("[GESTOR-BD] " + mensajes.size() +
                        " mensajes offline de '" + receptor + "' entregados y borrados.");
            }

        } catch (SQLException e) {
            System.err.println("[GESTOR-BD] Error extrayendo mensajes offline: " + e.getMessage());
        }
        return mensajes;
    }

    // =====================================================================
    // MÉTODOS DE HISTORIAL PERMANENTE (con Cifrado AES)
    // =====================================================================

    /**
     * Guarda un mensaje en el historial permanente, cifrando el contenido.
     * Este método se llama SIEMPRE que un mensaje pasa por el servidor
     * (sea entregado en vivo o guardado en offline).
     * 
     * Los archivos adjuntos también se guardan en la BD (LONGBLOB).
     * 
     * @param msg Objeto Mensaje a guardar en el historial
     */
    public static void guardarMensajeHistorial(Mensaje msg) {
        String sql = "INSERT INTO historial_mensajes (emisor, receptor, tipo, contenido_cifrado, datos_adjuntos, nombre_archivo, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)";

        try (Connection conn = conectar();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, msg.getEmisor());
            stmt.setString(2, msg.getReceptor());
            stmt.setString(3, msg.getTipo());
            stmt.setString(4, Seguridad.encriptar(msg.getContenido())); // CIFRADO AES

            // Guardar datos binarios del archivo en la BD
            if (msg.getTipo().equals(Mensaje.ARCHIVO) && msg.getDatosAdjuntos() != null) {
                stmt.setBytes(5, msg.getDatosAdjuntos());
            } else {
                stmt.setNull(5, Types.BLOB);
            }

            stmt.setString(6, msg.getNombreArchivo());
            stmt.setLong(7, msg.getTimestamp());
            stmt.executeUpdate();

            System.out.println("[GESTOR-BD] Historial guardado (cifrado): " +
                    msg.getEmisor() + " -> " + msg.getReceptor() + " [" + msg.getTipo() + "]");

        } catch (SQLException e) {
            System.err.println("[GESTOR-BD] Error guardando historial: " + e.getMessage());
        }
    }

    /**
     * Obtiene el historial completo de conversaciones de un usuario.
     * Retorna todos los mensajes donde el usuario sea emisor o receptor,
     * descifrando el contenido antes de retornarlo.
     * 
     * Se llama al iniciar sesión para restaurar las conversaciones pasadas.
     * 
     * @param nombreUsuario Nombre del usuario
     * @return Lista de mensajes históricos descifrados, ordenados por fecha
     */
    public static List<Mensaje> obtenerHistorialUsuario(String nombreUsuario) {
        List<Mensaje> historial = new ArrayList<>();
        String sql = "SELECT emisor, receptor, tipo, contenido_cifrado, nombre_archivo, timestamp " +
                     "FROM historial_mensajes " +
                     "WHERE emisor = ? OR receptor = ? " +
                     "ORDER BY timestamp ASC";

        try (Connection conn = conectar();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, nombreUsuario);
            stmt.setString(2, nombreUsuario);
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                String tipo = rs.getString("tipo");
                String contenidoDescifrado = Seguridad.desencriptar(rs.getString("contenido_cifrado")); // DESCIFRADO AES

                Mensaje msg;
                if (tipo.equals(Mensaje.ARCHIVO)) {
                    // Para archivos del historial, solo mostramos la referencia
                    msg = new Mensaje(
                            rs.getString("emisor"),
                            rs.getString("receptor"),
                            Mensaje.HISTORIAL,
                            contenidoDescifrado
                    );
                } else {
                    msg = new Mensaje(
                            rs.getString("emisor"),
                            rs.getString("receptor"),
                            Mensaje.HISTORIAL,
                            contenidoDescifrado
                    );
                }
                historial.add(msg);
            }

            if (!historial.isEmpty()) {
                System.out.println("[GESTOR-BD] Historial de '" + nombreUsuario +
                        "': " + historial.size() + " mensajes recuperados y descifrados.");
            }

        } catch (SQLException e) {
            System.err.println("[GESTOR-BD] Error obteniendo historial: " + e.getMessage());
        }
        return historial;
    }
}
