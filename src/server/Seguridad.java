package server;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.security.MessageDigest;
import java.util.Base64;

/**
 * =====================================================================
 * SEGURIDAD — Cifrado AES para mensajes + Hash SHA-256 para contraseñas
 * =====================================================================
 * 
 * CIFRADO DE MENSAJES (AES - Simétrico):
 * Los mensajes se cifran con AES antes de guardarlos en la BD.
 * Si alguien abre phpMyAdmin, solo verá texto incomprensible.
 * La clave es simétrica (el servidor cifra y descifra con la misma clave).
 * 
 * HASH DE CONTRASEÑAS (SHA-256 - Irreversible):
 * Las contraseñas NUNCA se guardan en texto plano. Se convierten en un
 * hash de 64 caracteres hexadecimales usando SHA-256. Ni siquiera el
 * administrador del servidor puede saber la contraseña original.
 * Para autenticar, se hashea la contraseña ingresada y se compara
 * con el hash almacenado en la BD.
 * =====================================================================
 */
public class Seguridad {

    // ===== CONFIGURACIÓN AES =====
    // Clave secreta de 16 bytes (128 bits) para AES.
    // En producción, esta clave debería leerse de un archivo externo
    // o variable de entorno, nunca hardcodeada. Para este proyecto
    // académico es suficiente.
    private static final String CLAVE_AES = "ChatDistribuido!"; // Exactamente 16 caracteres
    private static final String ALGORITMO_AES = "AES";

    // =====================================================================
    // CIFRADO AES PARA MENSAJES
    // =====================================================================

    /**
     * Cifra un texto plano usando AES-128.
     * El resultado se codifica en Base64 para poder almacenarlo como
     * String en la base de datos.
     * 
     * @param textoPlano Texto original del mensaje
     * @return Texto cifrado en Base64, o el texto original si falla
     */
    public static String encriptar(String textoPlano) {
        if (textoPlano == null || textoPlano.isEmpty()) {
            return textoPlano;
        }
        try {
            SecretKeySpec clave = new SecretKeySpec(CLAVE_AES.getBytes("UTF-8"), ALGORITMO_AES);
            Cipher cipher = Cipher.getInstance(ALGORITMO_AES);
            cipher.init(Cipher.ENCRYPT_MODE, clave);
            byte[] bytesCifrados = cipher.doFinal(textoPlano.getBytes("UTF-8"));
            return Base64.getEncoder().encodeToString(bytesCifrados);
        } catch (Exception e) {
            System.err.println("[SEGURIDAD] Error cifrando: " + e.getMessage());
            return textoPlano; // Fallback: guardar sin cifrar
        }
    }

    /**
     * Descifra un texto cifrado en Base64 usando AES-128.
     * 
     * @param textoCifrado Texto cifrado en Base64
     * @return Texto original descifrado, o el texto cifrado si falla
     */
    public static String desencriptar(String textoCifrado) {
        if (textoCifrado == null || textoCifrado.isEmpty()) {
            return textoCifrado;
        }
        try {
            SecretKeySpec clave = new SecretKeySpec(CLAVE_AES.getBytes("UTF-8"), ALGORITMO_AES);
            Cipher cipher = Cipher.getInstance(ALGORITMO_AES);
            cipher.init(Cipher.DECRYPT_MODE, clave);
            byte[] bytesDescifrados = cipher.doFinal(Base64.getDecoder().decode(textoCifrado));
            return new String(bytesDescifrados, "UTF-8");
        } catch (Exception e) {
            System.err.println("[SEGURIDAD] Error descifrando: " + e.getMessage());
            return textoCifrado; // Fallback: devolver tal cual
        }
    }

    // =====================================================================
    // HASH SHA-256 PARA CONTRASEÑAS
    // =====================================================================

    /**
     * Genera un hash SHA-256 irreversible de la contraseña.
     * 
     * El resultado es un String hexadecimal de 64 caracteres.
     * Es IMPOSIBLE obtener la contraseña original a partir del hash.
     * Para autenticar, se hashea la contraseña ingresada por el usuario
     * y se compara con el hash almacenado en la BD.
     * 
     * @param password Contraseña en texto plano
     * @return Hash SHA-256 en formato hexadecimal (64 caracteres)
     */
    public static String hashPassword(String password) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(password.getBytes("UTF-8"));

            // Convertir bytes a hexadecimal
            StringBuilder hexString = new StringBuilder();
            for (byte b : hashBytes) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }
            return hexString.toString();
        } catch (Exception e) {
            System.err.println("[SEGURIDAD] Error hasheando contraseña: " + e.getMessage());
            return password; // Fallback (no debería ocurrir)
        }
    }
}
