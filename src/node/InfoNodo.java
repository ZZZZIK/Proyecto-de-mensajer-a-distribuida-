package node;

import java.io.Serializable;

/**
 * Información de un nodo en la lista de membresía del sistema distribuido.
 * Cada nodo mantiene una copia de esta información para todos los nodos conocidos.
 */
public class InfoNodo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int id;               // Identificador único del nodo (0, 1, 2, ...)
    private final String host;          // Dirección IP o hostname
    private final int puerto;           // Puerto TCP del nodo
    private volatile boolean activo;    // ¿Está respondiendo a heartbeats?
    private volatile long ultimoHeartbeat;  // Timestamp del último heartbeat recibido

    public InfoNodo(int id, String host, int puerto) {
        this.id = id;
        this.host = host;
        this.puerto = puerto;
        this.activo = false;
        this.ultimoHeartbeat = 0;
    }

    // ===== GETTERS =====
    public int getId()                  { return id; }
    public String getHost()             { return host; }
    public int getPuerto()              { return puerto; }
    public boolean isActivo()           { return activo; }
    public long getUltimoHeartbeat()    { return ultimoHeartbeat; }

    // ===== SETTERS =====
    public void setActivo(boolean activo) {
        this.activo = activo;
    }

    public void registrarHeartbeat() {
        this.ultimoHeartbeat = System.currentTimeMillis();
        this.activo = true;
    }

    /**
     * Dirección completa del nodo (host:puerto).
     */
    public String getDireccion() {
        return host + ":" + puerto;
    }

    @Override
    public String toString() {
        return "Nodo-" + id + "[" + getDireccion() + ", " +
               (activo ? "ACTIVO" : "INACTIVO") + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InfoNodo infoNodo = (InfoNodo) o;
        return id == infoNodo.id;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(id);
    }
}
