package Databases.MySQL;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by christina on 7/7/2016.
 */
public class CreateMySQLDatabaseScript {

    private static String JDBC_DRIVER="com.mysql.jdbc.Driver";
    private static String DB_NAME="twitterDB";
    private static String USERNAME="root";
    private static String PASSWORD="root";

    public static void main(String[]args) throws ClassNotFoundException,SQLException,Exception{
        Class.forName("com.mysql.jdbc.Driver");
        Connection connection= DriverManager.getConnection("jdbc:mysql://localhost/?user="+USERNAME+"&password="+PASSWORD);

        String sqlQuery="CREATE DATABASE IF NOT EXISTS "+DB_NAME;
        Statement statement=connection.createStatement();
        int result=statement.executeUpdate(sqlQuery);



    }

}
