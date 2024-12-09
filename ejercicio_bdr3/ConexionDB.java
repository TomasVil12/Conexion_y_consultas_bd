package ejercicio_bd_ddr_3;

import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.DefaultListCellRenderer;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.table.DefaultTableModel;


public abstract class ConexionDB {

    protected Connection conexion;
    protected Statement sentencia;
    protected ResultSet resultSet;


    public ConexionDB(String claseNombre, String cadenaConexion) {
        try {
            Class.forName(claseNombre);
            conexion = DriverManager.getConnection(cadenaConexion);
            conexion.setAutoCommit(false);
        } catch (ClassNotFoundException | SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    public ConexionDB(String claseNombre, String cadenaConexion, String usuario, String pass) {
        try {
            Class.forName(claseNombre);
            conexion = DriverManager.getConnection(cadenaConexion, usuario, pass);
            conexion.setAutoCommit(false);
        } catch (ClassNotFoundException | SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }


    public Statement getSentencia() {
        return sentencia;
    }


    public Connection getconexion() {
        return conexion;
    }


    public ResultSet getResultSet() {
        return resultSet;
    }

 
    public void commit() {

        try {
            conexion.commit();
        } catch (SQLException ex) {
            Logger.getLogger(ConexionDB.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void rollback() {

        try {
            conexion.rollback();
        } catch (SQLException ex) {
            Logger.getLogger(ConexionDB.class.getName()).log(Level.SEVERE, null, ex);
        }
    }


    public void cerrarResult() {
        try {
            resultSet.close();
        } catch (SQLException ex) {
            Logger.getLogger(ConexionDB.class.getName()).log(Level.SEVERE, null, ex);
        }
    }


    public void cerrarSentencia() {
        try {
            sentencia.close();
        } catch (SQLException ex) {
            Logger.getLogger(ConexionDB.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void cerrarConexion() {
        try {
            if (resultSet != null) {
                cerrarResult();
            }
            if (sentencia != null) {
                cerrarSentencia();
            }
            conexion.close();
        } catch (SQLException ex) {
            Logger.getLogger(ConexionDB.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

 
    public void ejecutarConsulta(String consulta) {
        try {
            sentencia = conexion.createStatement();
            resultSet = sentencia.executeQuery(consulta);

        } catch (SQLException ex) {
            Logger.getLogger(ConexionDB.class.getName()).log(Level.SEVERE, null, ex);
        }

    }


    public int ejecutarInstruccion(String instruccion) {

        int filas = 0;

        try {
            sentencia = conexion.createStatement();
            filas = sentencia.executeUpdate(instruccion);
        } catch (SQLException ex) {
            Logger.getLogger(ConexionDB.class.getName()).log(Level.SEVERE, null, ex);
        }

        return filas;
    }


    public int ejecutarInstruccionCommit(String instruccion, boolean commit) {

        int filas = 0;

        try {
            sentencia = conexion.createStatement();
            filas = sentencia.executeUpdate(instruccion);

            if (commit) {
                commit();
            }

        } catch (SQLException ex) {
            Logger.getLogger(ConexionDB.class.getName()).log(Level.SEVERE, null, ex);
        }

        return filas;
    }


    public boolean existeValor(String valor, String columna, String tabla) {

        boolean existe = false;

        Statement sentenciaAux;
        try {
            sentenciaAux = conexion.createStatement();

            ResultSet aux = sentenciaAux.executeQuery("select count(*) from " + tabla + " where upper(" + columna + ")='" + valor.toUpperCase() + "'");

            aux.next();

            if (aux.getInt(1) >= 1) {
                existe = true;
            }

            aux.close();
            sentenciaAux.close();

        } catch (SQLException ex) {
            Logger.getLogger(ConexionDB.class.getName()).log(Level.SEVERE, null, ex);
        }

        return existe;
    }


    public boolean existeValor(int valor, String tabla, String columna) {

        boolean existe = false;

        Statement sentenciaAux;

        try {
            sentenciaAux = conexion.createStatement();

            ResultSet aux = sentenciaAux.executeQuery("select count(*) from " + tabla + " where " + columna + "=" + valor + "");

            aux.next();

            if (aux.getInt(1) >= 1) {
                existe = true;
            }

            aux.close();
            sentenciaAux.close();

        } catch (SQLException ex) {
            Logger.getLogger(ConexionDB.class.getName()).log(Level.SEVERE, null, ex);
        }

        return existe;
    }


    public boolean masOIgualQueUno(String query) {

        boolean vacio = false;

        Statement sentenciaAux;
        try {
            sentenciaAux = conexion.createStatement();

            ResultSet aux = sentenciaAux.executeQuery(query);

            aux.next();

            if (aux.getInt(1) >= 1) {
                vacio = true;
            }

            aux.close();
            sentenciaAux.close();

        } catch (SQLException ex) {
            Logger.getLogger(ConexionDB.class.getName()).log(Level.SEVERE, null, ex);
        }

        return vacio;

    }


    public int devolverValorInt(String columna, String tabla, String condicion) {

        try (Statement sentenciaAux = conexion.createStatement();
                ResultSet aux = sentenciaAux.executeQuery("select " + columna + " from " + tabla + " where " + condicion);) {

            aux.next();
            return aux.getInt(1);

        } catch (SQLException ex) {
            Logger.getLogger(ConexionDB.class.getName()).log(Level.SEVERE, null, ex);
            return -1;
        }

    }


    public String devolverValorString(String columna, String tabla, String condicion) {

        try (Statement sentenciaAux = conexion.createStatement();
                ResultSet aux = sentenciaAux.executeQuery("select " + columna + " from " + tabla + " where " + condicion);) {

            if (consultaVacia("select " + columna + " from " + tabla + " where " + condicion)) {
                JOptionPane.showMessageDialog(null, "Error, consulta vacia");
                return null;
            } else {
                aux.next();

                return aux.getString(1);

            }

        } catch (SQLException ex) {
            Logger.getLogger(ConexionDB.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }

    }

 
    public int[] devolverValoresInt(String columna, String tabla, String condicion) {

        try (Statement sentenciaAux = conexion.createStatement();
                ResultSet aux = sentenciaAux.executeQuery("select " + columna + " from " + tabla + " where " + condicion);) {

            if (consultaVacia("select count(" + columna + ") from " + tabla + " where " + condicion)) {
                JOptionPane.showMessageDialog(null, "Error, consulta vacia");
                return null;
            } else {

                int total = cuentaRegistrosConsulta(tabla, condicion);

                int valores[] = new int[total];

                for (int i = 0; aux.next(); i++) {
                    valores[i] = aux.getInt(1);
                }

                return valores;

            }

        } catch (SQLException ex) {
            Logger.getLogger(ConexionDB.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }

    }


    public String[] devolverValoresString(String columna, String tabla, String condicion) {

        try (Statement sentenciaAux = conexion.createStatement();
                ResultSet aux = sentenciaAux.executeQuery("select " + columna + " from " + tabla + " where " + condicion);) {

            if (consultaVacia("select count(" + columna + ") from " + tabla + " where " + condicion)) {
                JOptionPane.showMessageDialog(null, "Error, consulta vacia");
                return null;
            } else {

                int total = cuentaRegistrosConsulta(tabla, condicion);

                String valores[] = new String[total];

                for (int i = 0; aux.next(); i++) {
                    valores[i] = aux.getString(1);
                }

                return valores;

            }

        } catch (SQLException ex) {
            Logger.getLogger(ConexionDB.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }

    }


    public int cuentaRegistrosConsulta(String tabla, String condicion) {

        String consulta;

        if (condicion.equals("")) {
            consulta = "select count(*) from " + tabla;
        } else {
            consulta = "select count(*) from " + tabla + " where " + condicion;
        }

        try (Statement sentenciaAux = conexion.createStatement();
                ResultSet aux = sentenciaAux.executeQuery(consulta);) {

            return aux.getInt(0);

        } catch (SQLException ex) {
            Logger.getLogger(ConexionDB.class.getName()).log(Level.SEVERE, null, ex);
            return -1;
        }

    }


    public boolean consultaVacia(String query) {

        boolean vacio = false;

        Statement sentenciaAux;
        try {
            sentenciaAux = conexion.createStatement();

            ResultSet aux = sentenciaAux.executeQuery(query);

            aux.next();

            if (aux.getInt(1) == 0) {
                vacio = true;
            }

            aux.close();
            sentenciaAux.close();

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
            vacio = true;
        }

        return vacio;

    }


    public int ultimoID(String columnaID, String tabla) {

        int IDMaximo = -1;
        Statement sm;
        try {
            sm = conexion.createStatement();
            ResultSet rs = sm.executeQuery("select max(" + columnaID + ") as " + columnaID + " from " + tabla + "");
            rs.next();
            IDMaximo = rs.getInt(columnaID);

            rs.close();
            sm.close();
        } catch (SQLException ex) {
            Logger.getLogger(ConexionDB.class.getName()).log(Level.SEVERE, null, ex);
        }

        return IDMaximo;

    }


    public int proximoIDDisponible(String columnaID, String tabla) {

        int id = ultimoID(columnaID, tabla);

        if (id == -1) {
            return 1;
        } else {
            return id + 1;
        }

    }


    public int ultimoIDSinEliminar(String columnaEliminado, String columnaID, String tabla) {

        int IDMaximo = -1;
        Statement sm;
        try {
            sm = conexion.createStatement();
            ResultSet rs = sm.executeQuery("select max(" + columnaID + ") as " + columnaID + " from " + tabla + " where " + columnaEliminado + "=0");
            rs.next();
            IDMaximo = rs.getInt(columnaID);

            rs.close();
            sm.close();
        } catch (SQLException ex) {
            Logger.getLogger(ConexionDB.class.getName()).log(Level.SEVERE, null, ex);
        }

        return IDMaximo;

    }


    public int primerID(String columnaID, String tabla) {

        int IDMaximo = -1;
        Statement sm;
        try {
            sm = conexion.createStatement();
            ResultSet rs = sm.executeQuery("select min(" + columnaID + ") as " + columnaID + " from " + tabla + "");
            rs.next();
            IDMaximo = rs.getInt(columnaID);

            rs.close();
            sm.close();
        } catch (SQLException ex) {
            Logger.getLogger(ConexionDB.class.getName()).log(Level.SEVERE, null, ex);
        }

        return IDMaximo;

    }


    public int primerIDSinEliminar(String columnaEliminado, String columnaID, String tabla) {

        int IDMaximo = -1;
        Statement sm;
        try {
            sm = conexion.createStatement();
            ResultSet rs = sm.executeQuery("select min(" + columnaID + ") as " + columnaID + " from " + tabla + " where " + columnaEliminado + "=0");
            rs.next();
            IDMaximo = rs.getInt(columnaID);

            rs.close();
            sm.close();
        } catch (SQLException ex) {
            Logger.getLogger(ConexionDB.class.getName()).log(Level.SEVERE, null, ex);
        }

        return IDMaximo;

    }


    public String minimoDe(String columna, String tabla, String condicion) {

        String resultado = "";

        Statement sm;
        try {
            sm = conexion.createStatement();

            ResultSet rs;
            if (condicion.equals("")) {
                rs = sm.executeQuery("select min(" + columna + ") as " + columna + " from " + tabla + "");
            } else {
                rs = sm.executeQuery("select min(" + columna + ") as " + columna + " from " + tabla + " where " + condicion);
            }

            rs.next();
            resultado = rs.getString(columna);

            rs.close();
            sm.close();
        } catch (SQLException ex) {
            Logger.getLogger(ConexionDB.class.getName()).log(Level.SEVERE, null, ex);
        }

        return resultado;

    }


    public String maximoDe(String columna, String tabla, String condicion) {

        String resultado = "";

        Statement sm;
        try {
            sm = conexion.createStatement();

            ResultSet rs;
            if (condicion.equals("")) {
                rs = sm.executeQuery("select max(" + columna + ") as " + columna + " from " + tabla + "");
            } else {
                rs = sm.executeQuery("select max(" + columna + ") as " + columna + " from " + tabla + " where " + condicion);
            }

            rs.next();
            resultado = rs.getString(columna);

            rs.close();
            sm.close();

        } catch (SQLException ex) {
            Logger.getLogger(ConexionDB.class.getName()).log(Level.SEVERE, null, ex);
        }

        return resultado;

    }


    public int sumaDeInt(String columna, String tabla, String condicion) {

        String resultado = "";

        Statement sm;
        try {
            sm = conexion.createStatement();

            ResultSet rs;
            if (condicion.equals("")) {
                rs = sm.executeQuery("select sum(" + columna + ") as " + columna + " from " + tabla + "");
            } else {
                rs = sm.executeQuery("select sum(" + columna + ") as " + columna + " from " + tabla + " where " + condicion);
            }

            rs.next();
            resultado = rs.getString(columna);

            rs.close();
            sm.close();

        } catch (SQLException ex) {
            Logger.getLogger(ConexionDB.class.getName()).log(Level.SEVERE, null, ex);
        }

        return Integer.parseInt(resultado);

    }


    public double sumaDeDouble(String columna, String tabla, String condicion) {

        String resultado = "";

        Statement sm;
        try {
            sm = conexion.createStatement();

            ResultSet rs;
            if (condicion.equals("")) {
                rs = sm.executeQuery("select sum(" + columna + ") as " + columna + " from " + tabla + "");
            } else {
                rs = sm.executeQuery("select sum(" + columna + ") as " + columna + " from " + tabla + " where " + condicion);
            }

            rs.next();
            resultado = rs.getString(columna);

            rs.close();
            sm.close();

        } catch (SQLException ex) {
            Logger.getLogger(ConexionDB.class.getName()).log(Level.SEVERE, null, ex);
        }

        return Double.parseDouble(resultado);

    }


    public void rellenaComboBoxBDString(JComboBox cmb, String columna, String tabla, String condicion) {

        cmb.removeAllItems();

        Statement sm;
        try {
            sm = conexion.createStatement();

            ResultSet consulta = sm.executeQuery("select distinct " + columna + " from " + tabla);

            ResultSet correspondiente = null;

            if (!condicion.equals("")) {

                Statement smAux = conexion.createStatement();

                correspondiente = sm.executeQuery("select distinct " + columna + " from " + tabla + " where " + condicion);
                correspondiente.next();

                while (consulta.next()) {

                    cmb.addItem(consulta.getString(columna));
                    if (correspondiente.getString(columna).equals(consulta.getString(columna))) {
                        cmb.setSelectedItem(correspondiente.getString(columna));
                    }
                }

                correspondiente.close();
                smAux.close();
            } else {

                while (consulta.next()) {

                    cmb.addItem(consulta.getString(columna));

                }

            }

            consulta.close();
            sm.close();

        } catch (SQLException ex) {
            Logger.getLogger(ConexionDB.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

 
    public void rellenaComboBoxBDInt(JComboBox cmb, String tabla, String columna, String condicion) {

        cmb.removeAllItems();

        Statement sm;
        try {
            sm = conexion.createStatement();

            ResultSet consulta = sm.executeQuery("select distinct " + columna + " from " + tabla);

            ResultSet correspondiente = null;

            if (!condicion.equals("")) {

                Statement smAux = conexion.createStatement();

                correspondiente = smAux.executeQuery("select distinct " + columna + " from " + tabla + " where " + condicion);
                correspondiente.next();

                while (consulta.next()) {

                    cmb.addItem(consulta.getInt(columna));
                    if (correspondiente.getInt(columna) == consulta.getInt(columna)) {
                        cmb.setSelectedItem(correspondiente.getInt(columna));
                    }
                }

                correspondiente.close();
                smAux.close();
            } else {

                while (consulta.next()) {

                    cmb.addItem(consulta.getInt(columna));

                }

            }

            consulta.close();

            sm.close();

        } catch (SQLException ex) {
            Logger.getLogger(ConexionDB.class.getName()).log(Level.SEVERE, null, ex);
        }
    }


    public void rellenaComboBox2Columnas(JComboBox cmb, String consulta, String inicio, String columnaNoVisible, String columnaVisible) {

        String datos[] = new String[2];
        try {
            cmb.removeAllItems(); //Borra todos los Items
            Statement aux = conexion.createStatement();
            ResultSet resultado = aux.executeQuery(consulta);

            //Dato inicial
            if (!inicio.equals("")) {
                datos[0] = Integer.toString(-1);
                datos[1] = inicio;
                cmb.addItem(new String[]{datos[0], datos[1],});
            }

            while (resultado.next()) {
                datos[0] = Integer.toString(resultado.getInt(columnaNoVisible));
                datos[1] = resultado.getString(columnaVisible);
                cmb.addItem(new String[]{datos[0], datos[1],});
            }

            cmb.setRenderer(new DefaultListCellRenderer() {
                @Override
                public java.awt.Component getListCellRendererComponent(
                        JList l, Object o, int i, boolean s, boolean f) {
                    return new JLabel(((String[]) o)[1]);
                }
            });

        } catch (SQLException e) {
            System.out.println(e.getStackTrace());
        }

    }


    public void rellenaJTableBD(DefaultTableModel tabla) {
        try {

            //Cabecera
            ResultSetMetaData metadatos = resultSet.getMetaData();

            tabla.setColumnCount(metadatos.getColumnCount());

            int numeroColumnas = tabla.getColumnCount();

            String[] etiquetas = new String[numeroColumnas];

            for (int i = 0; i < numeroColumnas; i++) {
                etiquetas[i] = metadatos.getColumnLabel(i + 1);
            }

            tabla.setColumnIdentifiers(etiquetas);

            //Contenido
            while (resultSet.next()) {
                Object[] datosFila = new Object[tabla.getColumnCount()];
                for (int i = 0; i < tabla.getColumnCount(); i++) {
                    datosFila[i] = resultSet.getObject(i + 1);
                }
                tabla.addRow(datosFila);
            }

            cerrarResult();

        } catch (SQLException ex) {
            Logger.getLogger(ConexionDB.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

}
