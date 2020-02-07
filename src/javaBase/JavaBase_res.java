/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package javaBase;

import com.mysql.jdbc.Statement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 *
 * @author Alphalapz
 */
public class JavaBase_res {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {

        Connection conn = conectarDB();
        Statement stmt1 = (Statement) conn.createStatement();
        String sqlComparative, tableOrigen, tableDestino, columNames = "";
        tableOrigen = "dbtest.origen";
        tableDestino = "dbtest.destino";
        ArrayList<String> fieldsToCompare1 = new ArrayList<String>();
        ArrayList<String> fieldsToCompare2 = new ArrayList<String>();
        ArrayList<String> fieldsToUpdateDest = new ArrayList<String>();
        ArrayList<String> fieldsToUpdateOri = new ArrayList<String>();

        fieldsToCompare1.add("id");
        fieldsToCompare1.add("valor1");
        fieldsToCompare1.add("valor2");
        fieldsToCompare1.add("valor3");
        fieldsToCompare1.add("doc_upd");

        fieldsToCompare2.add("id");
        fieldsToCompare2.add("valor1");
        fieldsToCompare2.add("valor2");
        fieldsToCompare2.add("valor3");
        fieldsToCompare2.add("doc_upd");

        fieldsToUpdateDest.add("valor1");
        fieldsToUpdateDest.add("valor2");
        fieldsToUpdateDest.add("valor3");
        fieldsToUpdateDest.add("doc_upd");

        fieldsToUpdateOri.add("valor1");
        fieldsToUpdateOri.add("valor2");
        fieldsToUpdateOri.add("valor3");
        fieldsToUpdateOri.add("doc_upd");

        for (int i = 0; i < fieldsToCompare1.size(); i++) {
            columNames += tableDestino + "." + fieldsToCompare1.get(i) + " = " + tableOrigen + "." + fieldsToCompare2.get(i) + (i == fieldsToCompare1.size() - 1 ? " " : " AND ");
        }
        String filterOR;
        filterOR = tableDestino + "." + fieldsToCompare1.get(0) + " = " + tableOrigen + "." + fieldsToCompare2.get(0) + " AND (";
        for (int i = 1; i < fieldsToCompare1.size(); i++) {
            filterOR += tableDestino + "." + fieldsToCompare1.get(i) + " <> " + tableOrigen + "." + fieldsToCompare2.get(i) + (i == fieldsToCompare1.size() - 1 ? " " : " OR ");
        }
        filterOR += ")";
        /*
         TP_STATUS:
         CAMBIO  =   0   NEED UPDATE ON DESTINY TABLE
         SOBRA   =   1   NEED DELETE ON DESTINY TABLE AND VALIDATE IF LINK TABLE EXIST :: IF EXIST LINK FISICAL DELET NEED
         FALTA   =   2   NEED INSERT ON DESTINY TABLE
         IGUAL   =   3   NEED NOTHING
         */
        sqlComparative = "SELECT " + tableDestino + ".*, '1' estatus "
                + "FROM " + tableDestino
                + " INNER JOIN " + tableOrigen + " ON "
                + filterOR + " "
                + "UNION "
                + "SELECT " + tableDestino + ".*, '0' estatus "
                + "FROM " + tableDestino + " "
                + "WHERE NOT EXISTS ( SELECT * FROM " + tableOrigen + " "
                + "WHERE "
                + columNames + " "
                + ") "
                + "UNION "
                + "SELECT " + tableOrigen + ".*, '2' estatus "
                + "FROM " + tableOrigen + " "
                + "WHERE NOT EXISTS ( SELECT * FROM " + tableDestino + " "
                + "WHERE "
                + columNames
                + " ) ORDER BY id;";

        // found changues
        ResultSet resultSet1 = stmt1.executeQuery(sqlComparative);
        ResultSetMetaData rsmd = resultSet1.getMetaData();
        int comparativeValue = 0;
        int arraySize = 0;
        while (resultSet1.next()) {
            arraySize++;
        }
        resultSet1.first();
        int[][] anArray = new int[arraySize + 1][2];

        if (resultSet1.next()) {
            comparativeValue = resultSet1.getInt("id");
            resultSet1.previous();
        }

        int cont = 0;

        while (resultSet1.next()) {

            if (comparativeValue != resultSet1.getInt("id")) {

                comparativeValue = resultSet1.getInt("id");
                resultSet1.previous();
                System.out.println("ID: " + resultSet1.getInt("id") + " Estatus: " + resultSet1.getInt("estatus"));
                if (resultSet1.getInt("estatus") != 3) {
                    anArray[++cont][0] = resultSet1.getInt("id");
                    anArray[cont][1] = resultSet1.getInt("estatus");
                }

                resultSet1.next();
            }
        }

        resultSet1.last();

        try {
            System.out.println("ID: " + resultSet1.getInt("id") + " Estatus: " + resultSet1.getInt("estatus"));
            if (resultSet1.getInt("estatus") != 3) {
                anArray[++cont][0] = resultSet1.getInt("id");
                anArray[cont][1] = resultSet1.getInt("estatus");
            }
        } catch (Exception e) {
            System.out.println("NO HAY CAMBIOS A REALIZAR");
            System.exit(0);
        }

        for (int i = 1; i < anArray.length; i++) {
            switch (anArray[i][1]) {
                case 0:
                    if (anArray[i][0] != 0) {
                        needDelete(tableDestino, fieldsToCompare2.get(0), anArray[i][0]);
                    }
                    break;
                case 1:
                    if (anArray[i][0] != 0) {
                        needUpdate(tableOrigen, tableDestino, fieldsToCompare2.get(0), anArray[i][0], fieldsToUpdateDest, fieldsToUpdateOri);
                    }
                    break;
                case 2:
                    if (anArray[i][0] != 0) {
                        needInsert(tableOrigen, tableDestino, fieldsToCompare2.get(0), anArray[i][0], fieldsToUpdateDest, fieldsToUpdateOri);
                    }
                    break;
                case 3:
                    break;
                default:
                    break;
            }
//            System.out.println(anArray[i][0] + " " + anArray[i][1]);
        }
    }

    private static void needUpdate(String origen, String destino, String id_column, int id, ArrayList<String> fieldsToUpdateDest, ArrayList<String> fieldsToUpdateOri) throws SQLException {
        Connection conn = conectarDB();
        Statement stmt1 = (Statement) conn.createStatement();
        String sql, fields = "";
        for (int i = 0; i < fieldsToUpdateDest.size(); i++) {
            fields += destino + "." + fieldsToUpdateDest.get(i) + " = (SELECT " + origen + "." + fieldsToUpdateOri.get(i) + " FROM " + origen + " WHERE " + origen + "." + id_column + " = " + id + ")" + (i == fieldsToUpdateDest.size() - 1 ? " " : " , ");
        }
        sql = "UPDATE "
                + destino + " "
                + "SET "
                + fields
                + "WHERE "
                + destino + "." + id_column + " = " + id;
        int resultSet1 = stmt1.executeUpdate(sql);
        if (resultSet1 == 1) {
            System.out.println("SE HA ACTUALIZADO DE MANERA CORRECTA EL ID: " + id);
        } else {
            System.out.println((char) 27 + "[31m" + "NO FUE POSIBLE ACCTUALIZAR EL ID:" + id);
        }
    }

    private static void needDelete(String destino, String id_column, int id) throws SQLException {
        Connection conn = conectarDB();
        Statement stmt1 = (Statement) conn.createStatement();
        boolean resultSet1 = stmt1.execute("DELETE FROM " + destino + " WHERE " + id_column + " = " + id);
        if (!resultSet1) {
            System.out.println("SE HA ELIMINADO DE MANERA CORRECTA EL ID: " + id);
        } else {
            System.out.println((char) 27 + "[31m" + "NO SE HA ELIMINADO EL ID: " + id + " de la tabla destino");
        }
        //Pendiente validar las relaciones posiblemente creadas en la base de destino que hagan referencia al ID del registro elimanado
    }

    private static void needInsert(String origen, String destino, String id_column, int id, ArrayList<String> fieldsToInsertDest, ArrayList<String> fieldsToInsertOri) throws SQLException {
        Connection conn = conectarDB();
        Statement stmt1 = (Statement) conn.createStatement();
        String sql, fieldsDest = id_column + ", ", fieldsOri = id_column + ", ";
        for (int i = 0; i < fieldsToInsertDest.size(); i++) {
            fieldsDest += fieldsToInsertDest.get(i) + (i == fieldsToInsertDest.size() - 1 ? " " : " , ");
        }
        for (int i = 0; i < fieldsToInsertDest.size(); i++) {
            fieldsOri += fieldsToInsertOri.get(i) + (i == fieldsToInsertOri.size() - 1 ? " " : " , ");
        }
        sql = "INSERT INTO "
                + destino + "("
                + fieldsDest
                + ") "
                + "SELECT " + fieldsOri + " FROM " + origen + " "
                + "WHERE "
                + origen + "." + id_column + " = " + id;
        boolean resultSet1 = stmt1.execute(sql);
        if (!resultSet1) {
            System.out.println("SE HA ACTUALIZADO DE MANERA CORRECTA EL ID: " + id);
        } else {
            System.out.println((char) 27 + "[31m" + "NO FUE POSIBLE ACCTUALIZAR EL ID:" + id);
        }
    }

    private static Connection conectarDB() {
        String url = "jdbc:mysql://localhost:3306/dbtest";
        String username = "root";
        String password = "msroot";
        System.out.println("Connecting database...");
        Connection connection;
        try {
            connection = DriverManager.getConnection(url, username, password);
            System.out.println("Database connected!");
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot connect the database!", e);
        }
        return connection;
    }
}
