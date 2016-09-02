package Storm.Spouts.Preprocessing.OfAKeyword;

import Databases.NoSQL.CassandraDB.Functions.SerializeAndDeserializeJavaObjects;
import Databases.NoSQL.CassandraDB.PreprocessingAuthorsAndTweetData.CassandraSchemaForAuthorsAndFeatures;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by christina on 30/7/2016.
 */
public class ProcessFeaturesOfAKeywordSpout extends BaseRichSpout {
    private String filename;

    private SpoutOutputCollector spoutOutputCollector;
    private BufferedReader bufferedReader;
    private AtomicLong linesRead;
    private Map<String,List<Double>>map;

    public ProcessFeaturesOfAKeywordSpout(String filename){
        this.filename=filename;
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector=spoutOutputCollector;
        try {
            bufferedReader=new BufferedReader(new FileReader(filename));
        }catch (IOException e){
            throw new RuntimeException(e);
        }catch (Exception e){
            throw new RuntimeException(e);
        }
        linesRead=new AtomicLong(0);
        this.map=new HashMap<String, List<Double>>();

    }

    public void nextTuple() {
        try {
            String line=bufferedReader.readLine();
            if(line!=null){
                long ID=linesRead.incrementAndGet();

                int firstIndex=line.indexOf("[");
                int lastIndex=line.indexOf("]");

                String subString=line.substring(firstIndex+1,lastIndex);
                String[] splittedSubString=subString.split(",");

                String username=splittedSubString[0];
                String serializedFeatures=splittedSubString[1];
                List<Double>features=new ArrayList<Double>();
                String[] splittedSerializedFeatures=serializedFeatures.split(";");
                double tempFeature;
                for(int i=0;i<splittedSerializedFeatures.length;i++){
                    if(splittedSerializedFeatures[i].equals("NaN")){
                        tempFeature=0;
                    }else if(splittedSerializedFeatures[i].equals("Infinity")){
                        tempFeature=Double.MAX_VALUE;
                    }else if(splittedSerializedFeatures[i].equals("-Infinity")){
                        tempFeature=Double.MIN_VALUE;
                    }else{
                        tempFeature=Double.parseDouble(splittedSerializedFeatures[i]);
                    }
                    features.add(tempFeature);
                }

                if(!map.containsKey(username)){
                    map.put(username,features);
                }



            }else{
                CassandraSchemaForAuthorsAndFeatures.writeFeaturesToCassandraDB(SerializeAndDeserializeJavaObjects.serializeJavaStringAndJavaDoublesListHashmap(map));
                Thread.sleep(60*60*60*1000);
            }

        }catch (Exception e){
            throw new RuntimeException(e);
        }

    }

    @Override
    public void ack(Object msgId) {

    }

    @Override
    public void deactivate() {
        try {
            bufferedReader.close();
        }catch (IOException e){
            throw new RuntimeException(e);
        }catch (Exception e){
            throw new RuntimeException(e);
        }

    }

    @Override
    public void fail(Object msgId) {
        System.out.println("failed line number: "+msgId);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}

