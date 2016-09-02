package Databases.MySQL;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by christina on 8/7/2016.
 */
public class ExportMySQLResultsToCSVFiles {
    private static final String URL="jdbc:mysql://localhost:3306";
    private static final String DRIVER="com.mysql.jdbc.Driver";
    private static final String dbName="twitterDB";

    private static final String USERNAME="root";
    private static final String PASSWORD="root";

    private static final String PATH="/home/christina/twitter_storm_NEW/CSV/";

    public static void main(String[]args) throws IOException {
        Connection connection=null;
        FileWriter fileWriter;
        BufferedReader bufferedReader;

        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection= DriverManager.getConnection("jdbc:mysql://localhost:3306/twitterDB",USERNAME,PASSWORD);

            Statement statement=connection.createStatement();
            String query1="SELECT  TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE table_schema='"+dbName+"'";
            ResultSet resultSet=statement.executeQuery(query1);

            ArrayList<String> listOfTableNames=new ArrayList<String>();
            while (resultSet.next()){
                listOfTableNames.add(resultSet.getString(1));
                //System.out.println(resultSet.getString(1));
            }

            for(String tableName:listOfTableNames){
                System.out.println(tableName);
                int k=0;int j=1;

                List<String> listOfColumnNames=new ArrayList<String>();
                String query2="SELECT *FROM "+tableName;
                resultSet=statement.executeQuery(query2);

                int numberOfColumns=getNumberOfColumns(resultSet);
                try {
                    fileWriter=new FileWriter(PATH+""+tableName+".csv");
                    for(int i=1;i<=numberOfColumns;i++){
                        fileWriter.append(resultSet.getMetaData().getColumnName(i));
                        fileWriter.append(",");
                    }
                    fileWriter.append(System.getProperty("line.separator"));
                    while (resultSet.next()){
                        for(int i=1;i<=numberOfColumns;i++){
                            if(resultSet.getObject(i)!=null){
                                String data=resultSet.getObject(i).toString();
                                fileWriter.append(data);
                                fileWriter.append(",");
                            }else{
                                String data="null";
                                fileWriter.append(data);
                                fileWriter.append(",");
                            }
                        }
                        fileWriter.append(System.getProperty("line.separator"));
                    }
                    fileWriter.flush();
                    fileWriter.close();
                }catch (IOException e){
                    e.printStackTrace();
                }

            }



        }catch (ClassNotFoundException e){
            System.out.println("Unable to load JDBC driver");
        }catch (SQLException e){
            System.out.println("SQLException information");
        }

    }

    public static int getNumberOfRows(ResultSet resultSet)throws SQLException{
        resultSet.last();
        int numberOfRows=resultSet.getRow();
        resultSet.beforeFirst();
        return  numberOfRows;
    }

    public static int getNumberOfColumns(ResultSet resultSet )throws SQLException{
        return resultSet.getMetaData().getColumnCount();
    }
}
