/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package SqlServer;

import java.sql.DriverManager;
import java.sql.SQLException;

/**
 *
 * @author Alfredo Pérez
 */
public abstract class MyConn {

    public static java.sql.Connection openConnectionDestino() {
        String DB = "etla_com";
//        String url = "jdbc:mysql://10.83.32.129:3306/" + DB;
//        String url = "jdbc:mysql://10.83.40.123:3306/" + DB;
        String url = "jdbc:mysql://localhost:3306/" + DB;
        String username = "root";
        String password = "msroot";
        System.out.println("Connecting database..." + DB);
        java.sql.Connection connection;
        try {
            connection = DriverManager.getConnection(url, username, password);
            System.out.println("Database " + DB + " connected!");
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot connect the database!", e);
        }
        return connection;
    }

}
