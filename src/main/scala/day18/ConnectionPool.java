package day18;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;

/**
 * creat by newforesee 2018/10/16
 * 简易版的连接池
 */
public class ConnectionPool {
    //静态的Connection队列
    private static LinkedList<Connection> connectionLinkedList;

    /**
     * 加载驱动
     */
    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }


    /**
     * 获取连接,多线程访问并发控制
     * @return poll
     */
    public synchronized static Connection getConnection() {
        if (connectionLinkedList == null) {
            connectionLinkedList = new LinkedList<Connection>();
            for (int i = 0; i < 10; i++) {

                try {
                    Connection conn = DriverManager.getConnection(
                            "jdbc:mysql://slave1:3306/gp1809",
                            "root",
                            "123456"
                    );
                    connectionLinkedList.push(conn);
                } catch (SQLException e) {
                    e.printStackTrace();
                }

            }
        }
        return connectionLinkedList.poll();
    }

    /**
     * 归还一个连接
     */
    public static void returnConnection(Connection conn) {
        connectionLinkedList.push(conn);
    }
}
