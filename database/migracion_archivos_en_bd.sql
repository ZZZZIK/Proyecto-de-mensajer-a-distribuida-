-- =====================================================================
-- MIGRACIÓN: Archivos almacenados en BD en vez de disco (server_files/)
-- =====================================================================
-- Ejecutar este script en phpMyAdmin (XAMPP) o en la consola MySQL
-- si la base de datos ya existe y tiene datos.
--
-- Agrega la columna datos_adjuntos (LONGBLOB) a historial_mensajes
-- para guardar archivos directamente en la BD.
-- La tabla mensajes_offline YA tiene esta columna.
-- =====================================================================

USE chat_distribuido;

-- Agregar columna datos_adjuntos a historial_mensajes si no existe
ALTER TABLE historial_mensajes
    ADD COLUMN datos_adjuntos LONGBLOB AFTER contenido_cifrado;

-- NOTA: Después de ejecutar este script, la carpeta server_files/
-- ya no es necesaria y puede eliminarse de forma segura.
