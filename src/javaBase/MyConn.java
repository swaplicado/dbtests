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
public abstract class MyConn {

    public static java.sql.Connection openConnectionOrigen() {
        String DB = "erp_cartro";
//        String url = "jdbc:mysql://localhost:3306/" + DB;
        String url = "jdbc:mysql://10.83.32.129:3306/" + DB;
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

    public static Connection openConnectionRevuelta() {
        Connection connectionRevuelta = null;
        try{
            Class.forName("com.sybase.jdbc3.jdbc.SybDriver").newInstance();
        }catch(ClassNotFoundException | InstantiationException | IllegalAccessException e){
            System.out.println(e);
        }
        String url="jdbc:sybase:Tds:192.168.1.33:2638/Revuelta";  // AETH
//            String url="jdbc:sybase:Tds:10.83.42.218:2638/Revuelta";    // CARTRO
        Properties prop = new Properties();
        prop.put("user", "usuario");
        prop.put("password", "revuelta");
        try {
             connectionRevuelta = DriverManager.getConnection(url, prop);
        }
        catch (Exception e){
            System.out.println(e);
        }
        return connectionRevuelta;
        
    }
    
    public static java.sql.Connection openConnectionDestino() {
//        String DB = "etla_com";
        String DB = "som_com";
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
