package server;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.security.MessageDigest;
import java.util.Base64;

/**
 * Proporciona métodos de seguridad para la aplicación.
 * Implementa cifrado AES para los mensajes y hash SHA-256 para las contraseñas.
 */
public class Seguridad {

    // Clave secreta para AES
    private static final String CLAVE_AES = "ChatDistribuido!"; 
    private static final String ALGORITMO_AES = "AES";

    /**
     * Cifra un texto usando AES-128 y devuelve Base64.
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
