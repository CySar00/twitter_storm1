package Databases.MySQL;

import Databases.MySQL.Functions.MySQLScriptRunner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by christina on 8/7/2016.
 */
public class RunMySQLScript {
    public static final String PATH_TO_MYSQL_FILE="/home/christina/twitter_storm_NEW/twitter.sql";

    public static final String USERNAME="root";
    public static final String PASSWORD="root";
    public static final String DB_NAME="twitterDB";

    public static void main(String[]args) throws ClassNotFoundException,SQLException,Exception{
        FileWriter fileWriter;

        //create mysql connection
        Class.forName("com.mysql.jdbc.Driver");
        Connection connection= DriverManager.getConnection("jdbc:mysql://localhost:3306/twitterDB",USERNAME,PASSWORD);
        Statement statement=null;

        try {
            //initliaze object for MySQLScriptRunner
            MySQLScriptRunner mySQLScriptRunner=new MySQLScriptRunner(connection,false,false);
            //give the input file to the reader
            Reader reader=new BufferedReader(new FileReader(PATH_TO_MYSQL_FILE));
            //execute the script
            mySQLScriptRunner.runScript(reader);
        }catch (Exception e){
            System.err.println("Failed to execute "+PATH_TO_MYSQL_FILE+". The error is "+e.getMessage());
        }


    }

}
