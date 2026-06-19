package node;

import java.util.Arrays;

/**
 * Implementación de Relojes Vectoriales para el sistema distribuido.
 * 
 * Un reloj vectorial es un arreglo de N contadores (uno por nodo) que permite
 * determinar la relación causal entre eventos en un sistema distribuido sin
 * depender de un reloj global sincronizado.
 * 
 * Reglas:
 * - Evento local:   V[miId]++
 * - Enviar mensaje:  V[miId]++, adjuntar V al mensaje
 * - Recibir mensaje: V[i] = max(V[i], Vrecibido[i]) ∀i, luego V[miId]++
 * 
 * Relaciones causales:
 * - a → b (a causalmente precede a b): V(a) < V(b)
 *   es decir, V(a)[i] ≤ V(b)[i] ∀i, y ∃j tal que V(a)[j] < V(b)[j]
 * - a ∥ b (a y b son concurrentes): ni V(a) < V(b) ni V(b) < V(a)
 */
public class RelojVectorial {

    private final int[] vector;        // Un slot por nodo del sistema
    private final int miId;            // ID de este nodo
    private final int numNodos;        // Número total de nodos
    private final Object lock = new Object();

    /**
     * Crea un reloj vectorial inicializado en ceros.
     * 
     * @param miId     ID de este nodo (0-indexed)
     * @param numNodos Número total de nodos en el sistema
     */
    public RelojVectorial(int miId, int numNodos) {
        this.miId = miId;
        this.numNodos = numNodos;
        this.vector = new int[numNodos];
    }

    /**
     * Registra un evento local: incrementa el contador de este nodo.
     * 
     * @return Copia del reloj vectorial tras el incremento
     */
    public int[] eventoLocal() {
        synchronized (lock) {
            vector[miId]++;
            return Arrays.copyOf(vector, numNodos);
        }
    }

    /**
     * Prepara el reloj para adjuntarlo a un mensaje saliente.
     * Incrementa el contador local y retorna una copia del vector.
     * 
     * @return Copia del reloj vectorial para adjuntar al mensaje
     */
    public int[] prepararEnvio() {
        synchronized (lock) {
            vector[miId]++;
            return Arrays.copyOf(vector, numNodos);
        }
    }

    /**
     * Actualiza el reloj al recibir un mensaje.
     * Aplica la regla: V[i] = max(V[i], Vrecibido[i]) ∀i, luego V[miId]++
     * 
     * @param relojRecibido Reloj vectorial adjunto al mensaje recibido
     * @return Copia del reloj vectorial actualizado
     */
    public int[] recibirMensaje(int[] relojRecibido) {
        synchronized (lock) {
            if (relojRecibido != null) {
                int longitud = Math.min(vector.length, relojRecibido.length);
                for (int i = 0; i < longitud; i++) {
                    vector[i] = Math.max(vector[i], relojRecibido[i]);
                }
            }
            vector[miId]++;
            return Arrays.copyOf(vector, numNodos);
        }
    }

    /**
     * Obtiene una copia del estado actual del reloj sin modificarlo.
     * 
     * @return Copia del reloj vectorial actual
     */
    public int[] obtenerActual() {
        synchronized (lock) {
            return Arrays.copyOf(vector, numNodos);
        }
    }

    /**
     * Determina si el evento 'a' causalmente precede al evento 'b'.
     * a → b si y solo si: a[i] ≤ b[i] ∀i, y ∃j tal que a[j] < b[j]
     * 
     * @param a Reloj vectorial del evento a
     * @param b Reloj vectorial del evento b
     * @return true si a causalmente precede a b
     */
    public static boolean esCausal(int[] a, int[] b) {
        if (a == null || b == null) return false;
        int longitud = Math.min(a.length, b.length);
        boolean alMenosUnMenor = false;

        for (int i = 0; i < longitud; i++) {
            if (a[i] > b[i]) {
                return false; // a no puede preceder a b si algún componente es mayor
            }
            if (a[i] < b[i]) {
                alMenosUnMenor = true;
            }
        }
        return alMenosUnMenor;
    }

    /**
     * Determina si dos eventos son concurrentes (ni a→b ni b→a).
     * 
     * @param a Reloj vectorial del evento a
     * @param b Reloj vectorial del evento b
     * @return true si a y b son concurrentes
     */
    public static boolean esConcurrente(int[] a, int[] b) {
        return !esCausal(a, b) && !esCausal(b, a) && !Arrays.equals(a, b);
    }

    /**
     * Compara dos relojes vectoriales para determinar prioridad en Ricart-Agrawala.
     * Retorna negativo si a tiene prioridad, positivo si b tiene prioridad.
     * Usa el timestamp del reloj vectorial (suma de componentes) y desempata por nodoId.
     * 
     * @param a       Reloj vectorial del evento a
     * @param nodoIdA ID del nodo que generó a
     * @param b       Reloj vectorial del evento b
     * @param nodoIdB ID del nodo que generó b
     * @return < 0 si a tiene prioridad, > 0 si b tiene prioridad, 0 si iguales
     */
    public static int compararPrioridad(int[] a, int nodoIdA, int[] b, int nodoIdB) {
        // Usar la suma de componentes como timestamp lógico total
        int sumaA = 0, sumaB = 0;
        for (int v : a) sumaA += v;
        for (int v : b) sumaB += v;

        if (sumaA != sumaB) {
            return Integer.compare(sumaA, sumaB); // Menor timestamp = mayor prioridad
        }
        return Integer.compare(nodoIdA, nodoIdB); // Desempate por ID de nodo
    }

    /**
     * Formatea el reloj vectorial como cadena legible.
     */
    @Override
    public String toString() {
        synchronized (lock) {
            return Arrays.toString(vector);
        }
    }
}
