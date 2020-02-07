/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sybase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 * @author Alphalapz
 */
public class axiom {

    private static Connection connectionAxiom;

    /**
     * @param args the command line arguments
     * @throws java.sql.SQLException
     * @throws java.lang.ClassNotFoundException
     * @throws java.lang.InstantiationException
     * @throws java.lang.IllegalAccessException
     */
    public static void main(String[] args) throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        run();
    }

    private static void run() throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        Class.forName("com.sybase.jdbc3.jdbc.SybDriver").newInstance();
        String userName = "adm_siie";
        String password = "S113Adm";

        String url = "jdbc:sqlserver://10.83.32.17;databaseName=pltfloor_qb";

        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

        connectionAxiom = DriverManager.getConnection(url, userName, password);
        Connection connectionAxiomAux = DriverManager.getConnection(url, userName, password);

        Statement statementAxiom = connectionAxiom.createStatement();
        Statement statementAxiomAux = connectionAxiomAux.createStatement();

        String sqlAxiom = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME;";
        String sqlAxiomAux = "";

        ResultSet resultSetAxiom = statementAxiom.executeQuery(sqlAxiom);

        ResultSetMetaData resultSetMetaDataAxiom;
        resultSetMetaDataAxiom = resultSetAxiom.getMetaData();
        int n = resultSetMetaDataAxiom.getColumnCount();
        int cont = 0;
        int count = 0;
        System.out.println("*****************************");
        while (resultSetAxiom.next()) {
            for (int i = 1; i <= n; i++) {
                sqlAxiomAux = "SELECT COUNT(*) TOTAL FROM [" + resultSetAxiom.getString(i) + "];";
                ResultSet resultSetAxiomAux = statementAxiomAux.executeQuery(sqlAxiomAux);
                if (resultSetAxiomAux.next()) {
                    count = resultSetAxiomAux.getInt(1);
                } else {
                    count = 0;
                }
                if (count != 0) {
                    //System.out.println("→ " + ++cont + " ↓");
                    cont++;
                    System.out.println(resultSetMetaDataAxiom.getColumnName(i) + ":\t\t" + resultSetAxiom.getString(i) + "\t\t" + count);
                    //System.out.println("→ " + cont + " ↑");
                }
            }
        }
        System.out.println("*****************************");
        System.out.println("Total rows:\t" + cont);
        System.out.println("*****************************");
    }
}
