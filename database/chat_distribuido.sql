-- =====================================================================
-- SCRIPT SQL — Base de datos para el Sistema de Mensajería Distribuida
-- =====================================================================
-- Ejecutar este script en phpMyAdmin (XAMPP) o en la consola MySQL.
-- Crea la base de datos 'chat_distribuido' con cuatro tablas:
--   1. usuarios: Registro permanente con autenticación (nombre + hash)
--   2. mensajes_offline: Buzón temporal de mensajes no leídos
--   3. historial_mensajes: Registro permanente de TODAS las conversaciones
--   4. log_eventos: Log de eventos distribuidos con marcas vectoriales
-- =====================================================================

CREATE DATABASE IF NOT EXISTS chat_distribuido
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_unicode_ci;

USE chat_distribuido;

-- ===== TABLA DE USUARIOS =====
-- Almacena todos los usuarios registrados con su contraseña hasheada.
-- El campo nombre_usuario es UNIQUE para evitar duplicados.
-- El campo password almacena un hash SHA-256 (64 caracteres hex).
CREATE TABLE IF NOT EXISTS usuarios (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nombre_usuario VARCHAR(50) NOT NULL UNIQUE,
    password VARCHAR(255) NOT NULL,
    fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- ===== TABLA DE MENSAJES OFFLINE (BUZÓN TEMPORAL) =====
-- Almacena los mensajes pendientes para usuarios desconectados.
-- Cuando el usuario se conecta, los mensajes se extraen y se BORRAN.
-- Límite: 100 mensajes por usuario (controlado por GestorBD.java).
-- El contenido se almacena CIFRADO con AES.
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

-- ===== TABLA DE HISTORIAL DE MENSAJES (PERMANENTE) =====
-- Almacena TODOS los mensajes que pasan por el servidor de forma permanente.
-- NO se borran nunca. Se usan para restaurar conversaciones al iniciar sesión.
-- El contenido se almacena CIFRADO con AES.
CREATE TABLE IF NOT EXISTS historial_mensajes (
    id INT AUTO_INCREMENT PRIMARY KEY,
    emisor VARCHAR(50) NOT NULL,
    receptor VARCHAR(50) NOT NULL,
    tipo VARCHAR(20) NOT NULL,
    contenido_cifrado TEXT,
    datos_adjuntos LONGBLOB,
    nombre_archivo VARCHAR(255),
    timestamp BIGINT NOT NULL,
    fecha_guardado TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_emisor (emisor),
    INDEX idx_receptor (receptor),
    INDEX idx_timestamp (timestamp)
) ENGINE=InnoDB;

-- ===== TABLA DE LOG DE EVENTOS DISTRIBUIDOS (Entrega 2) =====
-- Registra eventos del sistema distribuido con marcas de reloj vectorial.
-- Sirve como evidencia de los algoritmos de coordinación y ordenamiento.
-- Categorías: MENSAJE, MUTEX, ELECCION, HEARTBEAT, FALLO, CONSENSO, NODO
CREATE TABLE IF NOT EXISTS log_eventos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nodo_id INT NOT NULL,
    categoria VARCHAR(20) NOT NULL,
    reloj_vectorial VARCHAR(100),
    descripcion TEXT NOT NULL,
    timestamp_evento BIGINT NOT NULL,
    fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_nodo (nodo_id),
    INDEX idx_categoria (categoria),
    INDEX idx_timestamp (timestamp_evento)
) ENGINE=InnoDB;
