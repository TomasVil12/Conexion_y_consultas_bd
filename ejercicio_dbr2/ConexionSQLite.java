package ejercicio_db_ddr_2;


public class ConexionSQLite extends ConexionDB {
    
    public ConexionSQLite(String ruta) {
        super("org.sqlite.JDBC", "jdbc:sqlite" + ruta);
    }
    
    
    
}
