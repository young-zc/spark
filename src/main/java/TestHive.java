import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class TestHive {

    private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws SQLException {
            try {
                Class.forName(driverName);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                System.exit(1);
            }

            Connection con = DriverManager.getConnection(
                    "jdbc:hive://139.217.218.95:10000/default", "admin", "1qaz1qaz!QAZ");
            Statement stmt = con.createStatement();
            String tableName = "o_upc_admin";
            /*stmt.execute("drop table if exists " + tableName);
            stmt.execute("create table " + tableName +
                    " (key int, value string)");
            System.out.println("Create table success!");*/
            // show tables
            String sql = "use jr_dev_raw"; //+ tableName + "'";
            System.out.println("Running: " + sql);
            ResultSet res = stmt.executeQuery(sql);
            sql = "show tables '" + tableName + "'";
            res = stmt.executeQuery(sql);
            if (res.next()) {
                System.out.println(res.getString(1));
            }

            // describe table
            sql = "describe " + tableName;
            System.out.println("Running: " + sql);
            res = stmt.executeQuery(sql);
            while (res.next()) {
                System.out.println(res.getString(1) + "\t" + res.getString(2));
            }


            sql = "select * from " + tableName;
            res = stmt.executeQuery(sql);
            while (res.next()) {
                System.out.println(String.valueOf(res.getInt(1)) + "\t"
                        + res.getString(2));
            }

            sql = "select count(1) from " + tableName;
            System.out.println("Running: " + sql);
            res = stmt.executeQuery(sql);
            while (res.next()) {
                System.out.println(res.getString(1));
            }

    }
}
