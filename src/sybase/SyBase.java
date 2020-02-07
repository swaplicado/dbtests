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
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author Alphalapz
 */
public class SyBase {

    private static Connection connectionRevuelta;
    public static int contador;

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
//        printTicket(getRevTicketInfo(131186));
        run();
    }

    private static void run() throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        contador = 0;
        Class.forName("com.sybase.jdbc3.jdbc.SybDriver").newInstance();
        String url = "jdbc:sybase:Tds:192.168.1.33:2638/Revuelta";  // AETH
        //String url="jdbc:sybase:Tds:10.83.42.218:2638/Revuelta";    // CARTRO
        Properties prop = new Properties();
        prop.put("user", "usuario");
        prop.put("password", "revuelta");
        connectionRevuelta = DriverManager.getConnection(url, prop);
        Statement statementRevuelta = connectionRevuelta.createStatement();

        String sqlRevuelta = "SELECT  * "
                + //"Pes_PedID, " +
                //"Ord_ID, " +
                //"Pes_BasPri, " +
                //"Pes_OpeNomPri, " +
                //"Pes_FecHorPri, " +
                //"Pes_ObsPri " +
                //"Pes_PesoPri, " +
                //"Pes_Mod2Pri, " +
                //"Pes_BasSeg, " +
                //"Pes_OpeNomSeg, " +
                //"Pes_FecHorSeg, " +
                //"Pes_ObsSeg, " +
                //"Pes_PesoSeg, " +
                //"Pes_FecHor, " +
                //"Pes_Bruto, " +
                //"Pes_Tara, " +
                //"Pes_Neto, " +
                //"Pes_NetoMerma, " +
                //"Usb_ID, " +
                //"Usb_Nombre, " +
                //"pe.Pro_ID, " +
                //"pe.Pro_Nombre, " +
                //"Emp_ID, " +
                //"Emp_Nombre, " +
                //"Emp_Categoria, " +
                //"Pes_Chofer, " +
                //"Pes_Placas " +
                //"pro.ProID, pro.Pro_Nombre " +
                " FROM dba.Pesadas AS pe WHERE pes_id = 129936 "
                + "ORDER BY Pes_ID";

        sqlRevuelta = "SELECT Usu_Clave, Pes_ID, Pro_ID, Emp_ID, Pes_Placas, Pes_Chofer, Pes_Completo, Pes_FecHorPri, Pes_FecHorSeg, Usb_Nombre, Emp_Nombre, Pro_Nombre, Pes_BasPri, Pes_OpeNomPri, Pes_ObsPri, Pes_PesoPri, Pes_PesoSeg, Pes_FecHorSeg, Pes_BasSeg, Pes_Bruto, Pes_Tara, Pes_Neto, Pes_OpeNomSeg, Pes_ObsSeg, Pes_Neto "
                + "FROM dba.Pesadas  as P INNER JOIN dba.Usuarios as U ON U.Usu_Nombre = P.Pes_OpeNomPri "
                + "WHERE Pes_FecHorPri BETWEEN '2019-01-04' AND '2019-01-05' AND Usb_ID = 'ACTH' AND Pro_ID IN ('0010','0011','0012','1458','1459','1460','2005','4145','AG2A','AJ12','AMO','BAGA','GOMA','HYCA','S11','SGIO') ";
        

        ResultSet resultSetRevuelta = statementRevuelta.executeQuery(sqlRevuelta);
        ResultSetMetaData resultSetMetaDataRevuelta;
        resultSetMetaDataRevuelta = resultSetRevuelta.getMetaData();
        int n = resultSetMetaDataRevuelta.getColumnCount();
        int cont = 0;
        System.out.println("*****************************");
        while (resultSetRevuelta.next()) {
            System.out.println("→ " + ++cont + " ↓");
            for (int i = 1; i <= n; i++) {
                System.out.println(resultSetMetaDataRevuelta.getColumnName(i) + ":\t\t" + resultSetRevuelta.getString(i));
                //System.out.println(("".equals(getContainerPlates("jaula;tq;tolva",resultSetRevuelta.getString(i))) ? "XXX::>" + resultSetRevuelta.getString(i) : ""));
                //System.out.println(getContainerPlates("jaula;tq;tolva", resultSetRevuelta.getString(i)));
            }
            System.out.println("→ " + cont + " ↑");
        }
        System.out.println("*****************************");
        System.out.println("Total rows:\t" + cont);
        System.out.println("Total FOUNDs:\t" + contador);
        System.out.println("*****************************");
    }

    private static String getContainerPlates(String obs) {
        if (obs == null) {
            return "";
        }
        String valuesToSearch = "jaula;tq;tolva";
        String[] valuesSplit = valuesToSearch.split(";");
        String placas = "";
        for (String value : valuesSplit) {

            Pattern pattern = Pattern.compile(value + "([\\s:]*[\\w]*)", Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(obs);

            if (matcher.find()) {
                placas = matcher.group(1).replaceAll("[\\s:]", "");
            }
            if (!"".equals(placas)) {
                return placas;
            }
        }
        return "";
    }

    /**
     *
     * @param aBuscar Palabras a buscar separadas por ; (punto y coma).
     * @param cadena Cadena de texto en donde se van a buscar las palabras.
     * @return Regresa la siguiente palabra encontrada despues de la palabra
     * buscada. ejemplo: aBuscar = "jaula;tolva;tq"; cadena = "PESO TEORICO:
     * 37990 KG JAULA: 293XS3 pedido 0068049 carga completa"; return = "293XS3";
     */
    private static String getContainerPlates(String aBuscar, String cadena) {
        String[] valuesSplit = aBuscar.split(";");
        String placas = "";
        for (String value : valuesSplit) {

            Pattern pattern = Pattern.compile(value + "([\\s:]*[\\w]*)", Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(cadena);

            if (matcher.find()) {
                placas = matcher.group(1).replaceAll("[\\s:]", "");
            }

            if (!"".equals(placas)) {
                return placas;
            }
        }
        return "";
    }

    private static ResultSet getRevTicketInfo(int idTicket) throws Exception {
        Class.forName("com.sybase.jdbc3.jdbc.SybDriver").newInstance();
        String url = "jdbc:sybase:Tds:192.168.1.33:2638/Revuelta";  // AETH
        //String url="jdbc:sybase:Tds:10.83.42.218:2638/Revuelta";    // CARTRO
        Properties prop = new Properties();
        prop.put("user", "usuario");
        prop.put("password", "revuelta");
        connectionRevuelta = DriverManager.getConnection(url, prop);
        Statement statementRevuelta = connectionRevuelta.createStatement();

        String sqlRevuelta = "SELECT  * FROM dba.pesadas WHERE Pes_Id = '" + idTicket + "'";
        ResultSet resultSetRevuelta = statementRevuelta.executeQuery(sqlRevuelta);
        ResultSetMetaData resultSetMetaDataRevuelta;
        resultSetMetaDataRevuelta = resultSetRevuelta.getMetaData();
        return resultSetRevuelta;
    }

    private static void printTicket(ResultSet resultSetRevuelta) throws Exception {
        ResultSetMetaData resultSetMetaDataRevuelta;
        resultSetMetaDataRevuelta = resultSetRevuelta.getMetaData();
        int n = resultSetMetaDataRevuelta.getColumnCount();
        int cont = 0;
        System.out.println("*****************************");
        while (resultSetRevuelta.next()) {
            System.out.println("→ " + ++cont + " ↓");
            for (int i = 1; i <= n; i++) {
                System.out.println(resultSetMetaDataRevuelta.getColumnName(i) + ":\t\t" + resultSetRevuelta.getString(i));
                //System.out.println(("".equals(getContainerPlates("jaula;tq;tolva",resultSetRevuelta.getString(i))) ? "XXX::>" + resultSetRevuelta.getString(i) : ""));
                //System.out.println(getContainerPlates("jaula;tq;tolva", resultSetRevuelta.getString(i)));
            }
            System.out.println("→ " + cont + " ↑");
        }
        System.out.println("*****************************");
        System.out.println("Total rows:\t" + cont);
        System.out.println("Total FOUNDs:\t" + contador);
        System.out.println("*****************************");
    }
}
