/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package SqlServer;

import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author Alpha
 */
public class main {

    private static Connection connEtla = javaBase.MyConn.openConnectionDestino();

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        updateAdd();
    }

    private static void updateAdd() throws ClassNotFoundException, SQLException{
        int idEtlaDestin = 0;
        String add1 = "";
        String add2 = "";
        String sql = "";
        String sql2 = "";
        Statement statementEtla = connEtla.createStatement();
        Statement statementEtlaUpdate = connEtla.createStatement();
        Statement statementAxiom = createConnAxiom().createStatement();
        ResultSet resultSetAxiom = null;
        ResultSet resultSetEtla = statementEtla.executeQuery("SELECT site_loc_id FROM SU_DESTIN order by site_loc_id;");

        while(resultSetEtla.next()){
            idEtlaDestin = resultSetEtla.getInt("site_loc_id");
            sql = "SELECT SiteLocation, Address1, Address2 FROM dbo.ShipTo WHERE SiteLocation = '" + idEtlaDestin + "';";
            resultSetAxiom = statementAxiom.executeQuery(sql);
            while (resultSetAxiom.next()){
                add1 = resultSetAxiom.getString("Address1");
                add2 = resultSetAxiom.getString("Address2");
                sql2 = "UPDATE etla_com.SU_DESTIN SET Address1 = '" + add1 + "', Address2 = '" + add2 + "' WHERE site_loc_id = " + resultSetAxiom.getInt("SiteLocation") + ";";
                statementEtlaUpdate.execute(sql2);
            }
        }
    }

    private static void showMedata(ResultSet rst) throws SQLException {
        ResultSetMetaData resultSetMetaData;
        resultSetMetaData = rst.getMetaData();
        int n = resultSetMetaData.getColumnCount();
        int cont = 0;
        System.out.println("*****************************");
        while (rst.next()) {
            System.out.println("→ " + ++cont + " ↓");
            for (int i = 1; i <= n; i++) {
                System.out.println(resultSetMetaData.getColumnName(i) + ":\t\t\t" + rst.getString(i));
            }

            System.out.println("→ " + cont + " ↑\n");
        }
        System.out.println("*****************************");
        System.out.println("Total rows:\t" + cont);
        System.out.println("*****************************");
    }

    private static void saveAllRows(ResultSet rst) {

    }

    private static void searchRows() {

    }

    private static ResultSet executeSqlAxiom(final String sql) throws SQLException, ClassNotFoundException {
        Connection connection = createConnAxiom();

        Statement stm = connection.createStatement();
        ResultSet rst;
        stm.execute(sql);
        rst = stm.getResultSet();

        return rst;
    }

    private static int nTpSelect = 1;
    private static int nTpUpdate = 2;

    /**
     *
     * @param sqlTemp
     * @param type 1 for SELECT || 2 FOR UPDATE
     * @return
     * @throws SQLException
     */
    private static ResultSet executeSqlEtl(final String sqlTemp, final int type) throws SQLException {
        Statement statementEtla = null;
        statementEtla = (Statement) connEtla.createStatement();
        ResultSet rstEtl = null;
        switch (type) {
            case 1:
                rstEtl = statementEtla.executeQuery(sqlTemp);
                break;
            case 2:
                statementEtla.executeUpdate(sqlTemp);
            default:
        }

        return rstEtl;
    }

    private static Connection createConnAxiom() throws ClassNotFoundException, SQLException {
        Connection connection = null;
        String user = "ats";
        String pswd = "ats001";
        String host = "10.83.30.17";
        int port = 1433;
        String name = "pltfloor";

        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        connection = DriverManager.getConnection("jdbc:sqlserver://" + host + (port == 0 ? "" : ":" + port) + ";databaseName=" + name + ";user=" + user + ";password=" + pswd);

        return connection;
    }

    
    private static void run() throws SQLException, ClassNotFoundException {
        //Extraer Tabla de AXIOM

        //poblar tabla en ETL (quiza temporal)
        String sSqlCreate = "";
        String sSql = "";
        String auxVar = "";
        String sSqlInsert = "";
        String sqlColumnNames = "";
        String sType = "";
        String sTableName = "tempAxiom";
        sSqlCreate = "CREATE TABLE IF NOT EXISTS " + sTableName + " (";
        //sSql = "SELECT TOP (1) CustomerInvoiceKey, InvoiceNumber, Created, Description FROM dbo.CustomerInvoices WHERE Created BETWEEN '2018-12-30' AND '2019-01-01'";
        sSql = "SELECT TOP (1) CustomerInvoiceKey, InvoiceNumber, Created, Description FROM dbo.CustomerInvoices;";
        ResultSet result = executeSqlAxiom(sSql);
        for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) {
            sSqlCreate += ", ";
            switch (result.getMetaData().getColumnType(i)) {
                case (-7):
                    sType = "BIT";
                    break;
                case (-6):
                    sType = "TINYINT";
                    break;
                case (-5):
                    sType = "BIGINT";
                    break;
                case (-4):
                    sType = "LONGVARBINARY";
                    break;
                case (-3):
                    sType = "VARBINARY";
                    break;
                case (-2):
                    sType = "BINARY";
                    break;
                case (-1):
                    sType = "LONGVARCHAR";
                    break;
                case (0):
                    sType = "NULL";
                    break;
                case (1):
                    sType = "CHAR";
                    break;
                case (2):
                    sType = "NUMERIC";
                    break;
                case (3):
                    sType = "DECIMAL";
                    break;
                case (4):
                    sType = "INTEGER";
                    break;
                case (5):
                    sType = "SMALLINT";
                    break;
                case (6):
                    sType = "FLOAT";
                    break;
                case (7):
                    sType = "REAL";
                    break;
                case (8):
                    sType = "DOUBLE";
                    break;
                case (12):
                    sType = "VARCHAR(50)";
                    break;
                case (91):
                    sType = "DATE";
                    break;
                case (92):
                    sType = "TIME";
                    break;
                case (93):
                    sType = "TIMESTAMP";
                    break;
                case (1111):
                    sType = "OTHER";
                    break;
                default:
            }
            sSqlCreate += result.getMetaData().getColumnName(i)
                    + " "
                    + sType + (i == 1 ? " UNSIGNED AUTO_INCREMENT PRIMARY KEY " : "");
            sqlColumnNames += result.getMetaData().getColumnName(i) + (i == result.getMetaData().getColumnCount() ? " " : ", ");
        }
        sSqlCreate += ");";

        executeSqlEtl(sSqlCreate, nTpUpdate);
        result = executeSqlEtl("SELECT CustomerInvoiceKey FROM tempaxiom AS t ORDER BY t.CustomerInvoiceKey DESC LIMIT 1;", nTpSelect);
        String lastRowId = "";
        if (result.next()) {
            lastRowId = result.getString(1);
        } else {
            lastRowId = "19635";
        }
        System.out.println(sSqlCreate);
        //sSql = "SELECT CustomerInvoiceKey, InvoiceNumber, Created, Description FROM dbo.CustomerInvoices WHERE Created BETWEEN '2016-01-01' AND '2019-02-20'";
        sSql = "SELECT CustomerInvoiceKey, InvoiceNumber, Created, Description FROM dbo.CustomerInvoices WHERE CustomerInvoiceKey > " + (lastRowId.isEmpty() ? "19635" : lastRowId + " ORDER BY CustomerInvoiceKey DESC");
        result = executeSqlAxiom(sSql);
        while (result.next()) {
            auxVar = "";
            sSqlInsert = "INSERT INTO " + sTableName + "(" + sqlColumnNames + ")" + " VALUES ( ";
            for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) {
                auxVar += "'" + result.getString(i) + "'" + (i == result.getMetaData().getColumnCount() ? ") " : ",");
            }
            sSqlInsert += auxVar;
            System.out.println(sSqlInsert);
            executeSqlEtl(sSqlInsert, nTpUpdate);
        }

    }

}
