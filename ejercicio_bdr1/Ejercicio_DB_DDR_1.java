package ejercicio_db_ddr_1;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Ejercicio_DB_DDR_1 {

    public static void main(String[] args) {
        
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection conexion = DriverManager.getConnection("jdbc:mysql://localhost/empleadoss_departamentoss", "root", "");
            
            Statement sentencia = conexion.createStatement();
            ResultSet resultSet = sentencia.executeQuery("select * from empleados");
            
            while(resultSet.next()) {
                
                System.out.println(resultSet.getString("nomEmp"));
                System.out.println(resultSet.getString("salEmp"));
                
            }
            
            resultSet.close();
            sentencia.close();
            
            conexion.close();
            
        } catch(ClassNotFoundException | SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }
    
}
