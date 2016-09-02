package Databases.NoSQL.CassandraDB.Functions;

import com.google.appengine.repackaged.com.google.common.primitives.Doubles;
import com.google.appengine.repackaged.com.google.common.primitives.Floats;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.json.DataObjectFactory;

import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.*;

/**
 * Created by christina on 8/8/2016.
 */
public class SerializeAndDeserializeJavaObjects {

    public static final String NEXT_ELEMENT=", ";

    public static String serializeJavaDoublesVector(double[]vector){
        StringBuilder stringBuilder=new StringBuilder();
        if(vector==null){
            return null;
        }

        if(vector.length>0){
            stringBuilder.append(String.valueOf(vector[0]));
            for(int i=0;i<vector.length;i++){
                stringBuilder.append(NEXT_ELEMENT).append(String.valueOf(vector[i]));
            }
        }
        return stringBuilder.toString();
    }

    public static String serializeJavaStringList(List<String>list){
        StringBuilder stringBuilder=new StringBuilder();
        stringBuilder.append("[");
        Iterator<String>iterator=list.iterator();
        stringBuilder.append(list.get(0));
        for(int i=1;i<list.size();i++){
            stringBuilder.append(",").append(list.get(i));
        }
        stringBuilder.append("]");
        return stringBuilder.toString();
    }

    public static String serializeJavaStringAndSerializedStatusHashmap(Map<String,String> map){
        StringBuilder stringBuilder=new StringBuilder();
        stringBuilder.append("{{");
        for(String key:map.keySet()){
            if(stringBuilder.length()>2){
                stringBuilder.append("&&&&&");
            }
            stringBuilder.append(key);
            stringBuilder.append("&&&");
            stringBuilder.append(map.get(key));
        }
        stringBuilder.append("}}");

        return stringBuilder.toString();
    }

    public static Map<String, Status>deserializeJavaStringAndStatusHashmap(String string) throws TwitterException {
        Map<String,Status>map=new HashMap<String, Status>();

        int firstIndex=string.indexOf("{{");
        int lastIndex=string.indexOf("}}}}");

        String substring=string.substring(firstIndex+1,lastIndex+2);
        String[] splittedSubstring=substring.split("&&&&&");
        for(int i=0;i<splittedSubstring.length;i++){
            String mapEntry=splittedSubstring[i];
            String [] splittedMapEntry=mapEntry.split("&&&");
            String username=splittedMapEntry[0];
            String serializedStatus=splittedMapEntry[1];
            Status status= DataObjectFactory.createStatus(serializedStatus);
            map.put(username,status);
        }

        return map;
    }


    public static String serializeJavaStringAndLongHashmap(Map<String,Long>map){
        StringBuilder stringBuilder=new StringBuilder();
        stringBuilder.append("{");
        for(String key:map.keySet()){
            if(stringBuilder.length()>1){
                stringBuilder.append(",");
            }
            stringBuilder.append(key);
            stringBuilder.append("=");
            stringBuilder.append(String.valueOf(map.get(key)));
        }
        stringBuilder.append("}");

        return stringBuilder.toString();
    }

    public static String serializeJavaLongsList(List<Long> list){
        StringBuilder stringBuilder=new StringBuilder();
        stringBuilder.append("[");
        stringBuilder.append(list.get(0));
        for(int i=1;i<list.size();i++){
            stringBuilder.append(",").append(list.get(i));
        }
        stringBuilder.append("]");
        return stringBuilder.toString();
    }


    public static String serializeJavaDoublesList(List<Double>list){
        StringBuilder stringBuilder=new StringBuilder();
        stringBuilder.append("[");
        stringBuilder.append(list.get(0));
        for(int i=1;i<list.size();i++){
            stringBuilder.append(",").append(list.get(i));
        }
        stringBuilder.append("]");

        return stringBuilder.toString();
    }

    public static String serializeJavaStringAndJavaLongsListHashmap(Map<String,List<Long>>map){
        StringBuilder stringBuilder=new StringBuilder();

        stringBuilder.append("{");
        for(String key:map.keySet()){
            if(stringBuilder.length()>1){
                stringBuilder.append(",");
            }

            stringBuilder.append(key);
            stringBuilder.append("=");
            stringBuilder.append(serializeJavaLongsList(map.get(key)));
        }
        stringBuilder.append("}");
        return stringBuilder.toString();
    }

    public static String serializeJavaStringAndJavaDoublesListHashmap(Map<String,List<Double>>map){
        StringBuilder stringBuilder=new StringBuilder();
        stringBuilder.append("{");
        for(String key:map.keySet()){
            if(stringBuilder.length()>1){
                stringBuilder.append(",");
            }
            stringBuilder.append(key);
            stringBuilder.append("=");
            stringBuilder.append(serializeJavaDoublesList(map.get(key)));
        }
        stringBuilder.append("}");
        return stringBuilder.toString();
    }


    public static Map<String,Long>deserializeJavaStringAndLongHashmap(String string){
        Map<String,Long>map=new HashMap<String, Long>();

        int firstIndex=string.indexOf("{");
        int lastIndex=string.indexOf("}");

        String subString=string.substring(firstIndex+1,lastIndex);
        String[] splittedString=subString.split(",");
        for(int i=0;i<splittedString.length;i++){
            String mapEntry=splittedString[i];
            String[] splittedMapEntry=splittedString[i].split("=");

            String mapKey=splittedMapEntry[0];
            String mapValue=splittedMapEntry[1];
            Long mapValue1=Long.valueOf(mapValue);
            map.put(mapKey,mapValue1);
        }
        return map;
    }

    public static List<Long> deserializeJavaLongsList(String string){
        List<Long>list=new ArrayList<Long>();

        int firstIndex=string.indexOf("[");
        int lastIndex=string.indexOf("]");

        String subString=string.substring(firstIndex+1,lastIndex);
        String[] splittedString=subString.split(",");
        for(int i=0;i<splittedString.length;i++){
            list.add(Long.valueOf(splittedString[i]));
        }
        return list;
    }

    public static List<Double>deserializeJavaDoublesList(String string){
        List<Double>list=new ArrayList<Double>();

        int firstIndex=string.indexOf("[");
        int lastIndex=string.indexOf("]");

        String substring=string.substring(firstIndex+1,lastIndex);
        String[] splittedString=substring.split(",");
        for(int i=0;i<splittedString.length;i++){
            list.add(Double.valueOf(splittedString[i]));
        }
        return list;
    }

    public static Map<String,List<Long>>deserializeJavaStringAndLongsListHashmap(String string){
        Map<String,List<Long>>map=new HashMap<String, List<Long>>();

        int firstIndex=string.indexOf("{");
        int lastIndex=string.indexOf("}");

        String subString=string.substring(firstIndex+1,lastIndex);
        String [] splittedString=subString.split(",");
        for(int i=0;i<splittedString.length;i++){
            String mapEntry=splittedString[i];
            String [] splittedMapEntry=mapEntry.split("=");

            String key=splittedMapEntry[0];
            String value=splittedMapEntry[1];

            List<Long>list=deserializeJavaLongsList(value);
            map.put(key,list);
        }
        return map;
    }

    public static Map<String,List<Double>>deserializeJavaStringAndDoublesListHashmap(String string){
        Map<String,List<Double>>map=new HashMap<String, List<Double>>();

        int firstIndex=string.indexOf("{");
        int lastIndex=string.indexOf("}");

        String subString=string.substring(firstIndex,lastIndex);
        String[] splittedSubstring=subString.split(",");
        for(int i=0;i<splittedSubstring.length;i++){
            String mapEntry=splittedSubstring[i];
            String [] splittedMapEntry=mapEntry.split("=");

            String key=splittedMapEntry[0];
            String value=splittedMapEntry[1];

            List<Double>list=deserializeJavaDoublesList(value);
            map.put(key,list);
        }
        return map;
    }


    public static Map<String,double[]>deserializeJavaStringAndDoublesVectorHashmap(String string){
        Map<String,double[]>map=new HashMap<String, double[]>();
        int firstIndex=string.indexOf("{");
        int lastIndex=string.indexOf("}");
        String subString=string.substring(firstIndex+1,lastIndex-1);
        String[] splittedSubString=subString.split("],");
        for(int i=0;i<splittedSubString.length;i++){
            String serializedMapEntry=splittedSubString[i];
            String [] splittedSerializedMapEntry=serializedMapEntry.split("=\\[");
            String key=splittedSerializedMapEntry[0];
            String serializedValues=splittedSerializedMapEntry[1];
            String[] splittedSerializedValues=serializedValues.split(",");
            List<Double>valuesAsList=new ArrayList<Double>();
            for(int j=0;j<splittedSerializedValues.length;j++){
                valuesAsList.add(Double.valueOf(splittedSerializedValues[j]));
            }
            double[] valuesAsVector= Doubles.toArray(valuesAsList);
            map.put(key,valuesAsVector);
        }
        return map;
    }

    public static Map<String,float[]>deserializeJavaStringAndFloatsVectorHashmap(String string){
        Map<String,float[]>map=new HashMap<String, float[]>();
        int firstIndex=string.indexOf("{");
        int lastIndex=string.indexOf("}");
        String subString=string.substring(firstIndex+1,lastIndex-1);
        String [] splittedSubString=subString.split("],");
        for(int i=0; i<splittedSubString.length;i++){
            String serializedMapEntry=splittedSubString[i];
            String [] splittedSerializedMapEntry=serializedMapEntry.split("=\\[");
            String key=splittedSerializedMapEntry[0];
            String serializedValues=splittedSerializedMapEntry[1];
            String[] splittedSerializedValues=serializedValues.split(",");
            List<Float>valuesAsList=new ArrayList<Float>();
            for(int j=0;j<splittedSerializedValues.length;j++){
                valuesAsList.add(Float.parseFloat(splittedSerializedValues[j]));
            }
            float[]valuesAsVector= Floats.toArray(valuesAsList);
            map.put(key,valuesAsVector);
        }
        return map;
    }




    public static double[]deserializeJavaDoublesVector(String string){
        StreamTokenizer streamTokenizer=new StreamTokenizer(new StringReader(string));
        streamTokenizer.resetSyntax();
        streamTokenizer.wordChars('0','9');
        streamTokenizer.whitespaceChars(' ',' ');
        streamTokenizer.parseNumbers();

        int length=(int)streamTokenizer.nval;
        double[]vector=new double[length];
        for(int i=0;i<vector.length;i++){
            vector[i]=streamTokenizer.nval;
        }
        return vector;
    }
}
