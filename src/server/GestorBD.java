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
 * Utiliza PreparedStatement para prevenir inyección SQL.
 * 
 * LÍMITE DE BUZÓN OFFLINE:
 * Mantiene un límite máximo de 100 mensajes offline por usuario
 * (estilo "Buzón de Voz") para prevenir abuso y saturación del disco.
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
     * 
     * @return Objeto Connection activo
     * @throws SQLException Si no se puede conectar
     */
    private static Connection conectar() throws SQLException {
        return DriverManager.getConnection(URL, USUARIO_BD, PASSWORD_BD);
    }

    /**
     * Verifica que la conexión a la base de datos esté operativa.
     * Se llama al iniciar el servidor para detectar problemas temprano.
     * 
     * @return true si la conexión es exitosa
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
    // MÉTODOS DE AUTENTICACIÓN
    // =====================================================================

    /**
     * Registra un nuevo usuario en la base de datos.
     * 
     * @param nombre   Nombre de usuario (debe ser único)
     * @param password Contraseña del usuario
     * @return true si el registro fue exitoso, false si el nombre ya existe
     */
    public static boolean registrarUsuario(String nombre, String password) {
        String sql = "INSERT INTO usuarios (nombre_usuario, password) VALUES (?, ?)";
        try (Connection conn = conectar();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, nombre);
            stmt.setString(2, password);
            stmt.executeUpdate();

            System.out.println("[GESTOR-BD] Usuario registrado en BD: " + nombre);
            return true;

        } catch (SQLException e) {
            if (e.getErrorCode() == 1062) { // Duplicate entry (UNIQUE constraint)
                System.out.println("[GESTOR-BD] Registro rechazado: '" + nombre + "' ya existe en BD.");
            } else {
                System.err.println("[GESTOR-BD] Error registrando usuario: " + e.getMessage());
            }
            return false;
        }
    }

    /**
     * Autentica un usuario verificando nombre y contraseña.
     * 
     * @param nombre   Nombre de usuario
     * @param password Contraseña proporcionada
     * @return true si las credenciales son correctas
     */
    public static boolean autenticarUsuario(String nombre, String password) {
        String sql = "SELECT password FROM usuarios WHERE nombre_usuario = ?";
        try (Connection conn = conectar();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, nombre);
            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                String passAlmacenada = rs.getString("password");
                boolean valido = passAlmacenada.equals(password);
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
     * Verifica si un usuario existe en la base de datos (se ha registrado alguna vez).
     * Se usa para validar destinatarios antes de enviar mensajes offline.
     * 
     * @param nombre Nombre de usuario a verificar
     * @return true si el usuario existe en la BD
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
     * Se usa para calcular la lista de usuarios offline (registrados - conectados).
     * 
     * @return Lista de nombres de usuario
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
    // MÉTODOS DE BUZÓN OFFLINE
    // =====================================================================

    /**
     * Guarda un mensaje en la bodega offline de un usuario desconectado.
     * 
     * LÍMITE DE SEGURIDAD: Si el destinatario ya tiene >= 100 mensajes
     * acumulados, se rechaza el nuevo mensaje (estilo "Buzón de Voz").
     * 
     * @param msg Objeto Mensaje a guardar
     * @return true si se guardó correctamente, false si el buzón está lleno
     */
    public static boolean guardarMensajeOffline(Mensaje msg) {
        // Primero verificar el límite
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
                    return false;
                }
            }

            // Guardar el mensaje
            try (PreparedStatement stmtInsert = conn.prepareStatement(sqlInsert)) {
                stmtInsert.setString(1, msg.getEmisor());
                stmtInsert.setString(2, msg.getReceptor());
                stmtInsert.setString(3, msg.getTipo());
                stmtInsert.setString(4, msg.getContenido());

                if (msg.getDatosAdjuntos() != null) {
                    stmtInsert.setBytes(5, msg.getDatosAdjuntos());
                } else {
                    stmtInsert.setNull(5, Types.BLOB);
                }

                stmtInsert.setString(6, msg.getNombreArchivo());
                stmtInsert.setLong(7, msg.getTimestamp());
                stmtInsert.executeUpdate();
            }

            System.out.println("[GESTOR-BD] Mensaje de '" + msg.getEmisor() +
                    "' guardado en buzón offline de '" + msg.getReceptor() + "'.");
            return true;

        } catch (SQLException e) {
            System.err.println("[GESTOR-BD] Error guardando mensaje offline: " + e.getMessage());
            return false;
        }
    }

    /**
     * Extrae todos los mensajes offline de un usuario y los borra de la BD.
     * Se llama cuando el usuario vuelve a conectarse.
     * 
     * @param receptor Nombre del usuario que se reconectó
     * @return Lista de mensajes pendientes (vacía si no tiene)
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
                    byte[] datos = rs.getBytes("datos_adjuntos");

                    Mensaje msg;
                    if (datos != null && tipo.equals(Mensaje.ARCHIVO)) {
                        // Reconstruir mensaje de tipo ARCHIVO
                        msg = new Mensaje(
                                rs.getString("emisor"),
                                rs.getString("receptor"),
                                rs.getString("nombre_archivo"),
                                datos
                        );
                    } else {
                        // Reconstruir mensaje de texto o notificación
                        msg = new Mensaje(
                                rs.getString("emisor"),
                                rs.getString("receptor"),
                                tipo,
                                rs.getString("contenido")
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
                        " mensajes offline de '" + receptor + "' entregados y borrados de BD.");
            }

        } catch (SQLException e) {
            System.err.println("[GESTOR-BD] Error extrayendo mensajes offline: " + e.getMessage());
        }
        return mensajes;
    }
}
