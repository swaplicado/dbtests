/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package javaBase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 *
 * @author Alfredo Pérez
 */
public class ImpItemsRevToEtla {

    /**
     *
     */
    private static Connection connOriRev = openConnectionRevuelta("192.168.1.33", "2638", "Revuleta", "usuario", "revuelta");
    private static Connection connDest = MyConn.openConnectionDestino();

    /**
     * @param args the command line arguments
     * @throws java.lang.ClassNotFoundException
     * @throws java.sql.SQLException
     * @throws java.lang.InstantiationException
     * @throws java.lang.IllegalAccessException
     */
    public static void main(String[] args) throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        
    }


        /**
         * Create a Revuelta Connection.
         * 
         * Default Params (Uppercase sensitivity):
         *
         * @param rev_host 192.168.1.33 AETH
         * #@param rev_host 10.83.42.218 CARTRO
         * @param rev_port 2638
         * @param rev_name Revuelta
         * @param rev_user usuario
         * @param rev_pswd revuelta
         * 
         * @return Connection
         */
    public static Connection openConnectionRevuelta(final String rev_host, final String rev_port, final String rev_name, final String rev_user, final String rev_pswd) {
        
        Connection connection = null;
        try {
            Class.forName("com.sybase.jdbc3.jdbc.SybDriver");

            String url = "jdbc:sybase:Tds:" + rev_host + ":" + rev_port + "/" + rev_name;
            Properties prop = new Properties();
            prop.put("user", rev_user);
            prop.put("password", rev_pswd);
            connection = DriverManager.getConnection(url, prop);
        } catch (ClassNotFoundException | SQLException e) {
            System.out.println(e);
        }

        return connection;
    }

}
