package Databases.MySQL.Functions;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.sql.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by christina on 7/7/2016.
 */
public class MySQLScriptRunner {
    private static final String DEFAULT_DELIMITER=";";
    private static final String DELIMITER_LINE_REGEX="(?i)DELIMITER.+";
    private static final String DELIMITER_LINE_REGEX_SPLIT="(?i)DELIMITER";

    private final Connection connection;
    private final boolean stopOnError;
    private final boolean autoCommit;

    private PrintWriter logWriter=new PrintWriter(System.out);
    private PrintWriter errorLogWriter=new PrintWriter(System.err);

    private String delimiter=DEFAULT_DELIMITER;
    private boolean fullLineDelimiter=false;

    public MySQLScriptRunner(Connection connection, boolean autoCommit, boolean stopOnError){
        this.connection=connection;
        this.autoCommit=autoCommit;
        this.stopOnError=stopOnError;
    }

    public void setDelimiter(String delimiter,boolean fullLineDelimiter) {
        this.delimiter = delimiter;
        this.fullLineDelimiter=fullLineDelimiter;
    }

    public void setLogWriter(PrintWriter logWriter) {
        this.logWriter = logWriter;
    }

    public void setErrorLogWriter(PrintWriter errorLogWriter) {
        this.errorLogWriter = errorLogWriter;
    }


    public void runScript(Reader reader)throws IOException,SQLException{
        try {
            boolean originalAutoCommit=connection.getAutoCommit();
            try {
                if(originalAutoCommit!=autoCommit){
                    connection.setAutoCommit(autoCommit);
                }
                runScript(connection,reader);
            }finally {
                connection.setAutoCommit(originalAutoCommit);
            }
        }catch (IOException e){
            throw e;
        }catch (SQLException e){
            throw e;
        }catch (Exception e){
            throw new RuntimeException("Error running script. Cause: "+e,e);
        }

    }

    private void runScript(Connection connection,Reader reader) throws IOException,SQLException{
        StringBuffer command=null;
        try {
            LineNumberReader lineNumberReader=new LineNumberReader(reader);
            String line=null;

            while ((line=lineNumberReader.readLine())!=null){
                if(command==null){
                    command=new StringBuffer();
                }
                String trimmedLine=line.trim();
                if(trimmedLine.startsWith("--")){
                    println(trimmedLine);
                }else if(trimmedLine.length()<1 || trimmedLine.startsWith("//")){
                    // do nothing
                }else if(trimmedLine.length()<1 || trimmedLine.startsWith("--")){
                    // do nothing
                }else if(!fullLineDelimiter && trimmedLine.endsWith(getDelimiter()) || fullLineDelimiter && trimmedLine.equals(getDelimiter() )){
                    Pattern pattern=Pattern.compile(DELIMITER_LINE_REGEX);
                    Matcher matcher=pattern.matcher(trimmedLine);
                    if(matcher.matches()){
                        setDelimiter(trimmedLine.split(DELIMITER_LINE_REGEX)[1].trim(), fullLineDelimiter);
                        line=lineNumberReader.readLine();
                        if(line==null){
                            break;
                        }
                        trimmedLine=line.trim();
                    }
                    command.append(line.substring(0,line.lastIndexOf(getDelimiter())));
                    command.append(" ");
                    Statement statement=connection.createStatement();
                    println(command);
                    boolean hasResults=false;
                    if(stopOnError){
                        hasResults=statement.execute(command.toString());
                    }else{
                        try {
                            statement.execute(command.toString());
                        }catch (SQLException e){
                            e.fillInStackTrace();
                            printlnError("Error executing "+command);
                            printlnError(e);
                        }
                    }

                    if(autoCommit && !connection.getAutoCommit()){
                        connection.commit();
                    }

                    ResultSet resultSet=statement.getResultSet();
                    if(hasResults && resultSet!=null){
                        ResultSetMetaData metaData=resultSet.getMetaData();
                        int columns=metaData.getColumnCount();
                        for(int i=0; i<columns;i++){
                            String columnName =metaData.getColumnName(i);
                            print(columnName+"\t");
                        }
                        println("");
                        while (resultSet.next()){
                            for(int i=1;i<=columns;i++){
                                String value=resultSet.getString(i);
                                print(value+"\t");
                            }
                            println("");

                        }
                    }
                    command=null;
                    try {
                        if(resultSet!=null){
                            resultSet.close();
                        }
                    }catch (Exception e){
                        e.printStackTrace();
                    }

                    try {
                        if(statement!=null){
                            statement.close();
                        }
                    }catch (Exception e){
                        e.printStackTrace();
                    }

                }else{
                    Pattern pattern=Pattern.compile(DELIMITER_LINE_REGEX);
                    Matcher matcher=pattern.matcher(trimmedLine);
                    if(matcher.matches()){
                        setDelimiter(trimmedLine.split(DELIMITER_LINE_REGEX_SPLIT)[1].trim(),fullLineDelimiter);
                        line=lineNumberReader.readLine();
                        if(line==null){
                            break;
                        }
                        trimmedLine=line.trim();
                    }
                    command.append(line);
                    command.append(" ");

                }
            }
            if(!autoCommit){
                connection.commit();
            }

        }catch (SQLException e){
            e.fillInStackTrace();
            printlnError("Error executing: "+command);
            printlnError(e);
            throw e;
        }catch (IOException e){
            e.fillInStackTrace();
            printlnError("Error executing: "+command);
            printlnError(e);
            throw e;
        }finally {
            connection.rollback();
            flush();
        }
    }




    private String getDelimiter(){
        return delimiter;
    }

    private void print(Object object){
        if(logWriter!=null){
            logWriter.print(object);
        }
    }

    private void println(Object object){
        if(errorLogWriter!=null){
            errorLogWriter.println(object);
        }
    }

    private  void printlnError(Object object){
        if(errorLogWriter!=null){
            errorLogWriter.println(object);
        }
    }

    private void flush(){
        if(logWriter!=null){
            logWriter.flush();
        }
        if(errorLogWriter!=null){
            errorLogWriter.flush();
        }
    }

}
