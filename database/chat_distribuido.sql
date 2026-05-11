-- =====================================================================
-- SCRIPT SQL — Base de datos para el Sistema de Mensajería Distribuida
-- =====================================================================
-- Ejecutar este script en phpMyAdmin (XAMPP) o en la consola MySQL.
-- Crea la base de datos 'chat_distribuido' con dos tablas:
--   1. usuarios: Registro permanente con autenticación (nombre + contraseña)
--   2. mensajes_offline: Bodega de mensajes para usuarios desconectados
-- =====================================================================

CREATE DATABASE IF NOT EXISTS chat_distribuido
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_unicode_ci;

USE chat_distribuido;

-- ===== TABLA DE USUARIOS =====
-- Almacena todos los usuarios registrados con su contraseña.
-- El campo nombre_usuario es UNIQUE para evitar duplicados.
CREATE TABLE IF NOT EXISTS usuarios (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nombre_usuario VARCHAR(50) NOT NULL UNIQUE,
    password VARCHAR(255) NOT NULL,
    fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- ===== TABLA DE MENSAJES OFFLINE =====
-- Almacena los mensajes pendientes para usuarios desconectados.
-- Cuando el usuario se conecta, los mensajes se extraen y se borran.
CREATE TABLE IF NOT EXISTS mensajes_offline (
    id INT AUTO_INCREMENT PRIMARY KEY,
    emisor VARCHAR(50) NOT NULL,
    receptor VARCHAR(50) NOT NULL,
    tipo VARCHAR(20) NOT NULL,
    contenido TEXT,
    datos_adjuntos LONGBLOB,
    nombre_archivo VARCHAR(255),
    timestamp BIGINT NOT NULL,
    fecha_guardado TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_receptor (receptor)
) ENGINE=InnoDB;
