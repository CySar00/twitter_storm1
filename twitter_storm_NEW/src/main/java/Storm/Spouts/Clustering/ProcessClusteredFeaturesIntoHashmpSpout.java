package Storm.Spouts.Clustering;

import Databases.NoSQL.CassandraDB.Clustering.CassandraSchemaForClusteredFeatures;
import Databases.NoSQL.CassandraDB.Functions.SerializeAndDeserializeJavaObjects;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by christina on 8/8/2016.
 */
public class ProcessClusteredFeaturesIntoHashmpSpout extends BaseRichSpout {
    private String filename1;
    private String filename2;

    SpoutOutputCollector spoutOutputCollector;
    BufferedReader bufferedReader1;
    BufferedReader bufferedReader2;
    AtomicLong linesRead1;
    AtomicLong linesRead2;

    Map<String,List<Double>>clusteredFeaturesHashmap0;
    Map<String,List<Double>>clusteredFeaturesHashmap1;


    public ProcessClusteredFeaturesIntoHashmpSpout(String filename1,String filename2){
        this.filename1=filename1;
        this.filename2=filename2;

    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector=spoutOutputCollector;
        try {
            bufferedReader1=new BufferedReader(new FileReader(filename1));
            bufferedReader2=new BufferedReader(new FileReader(filename2));
        }catch (IOException e){
            e.printStackTrace();
        }catch (Exception e){
            e.printStackTrace();
        }
        linesRead1=new AtomicLong(0);
        linesRead2=new AtomicLong(0);
        clusteredFeaturesHashmap0=new HashMap<String, List<Double>>();
        clusteredFeaturesHashmap1=new HashMap<String, List<Double>>();
    }

    public void nextTuple() {
        try {
            String line1=bufferedReader1.readLine();
            String line2=bufferedReader2.readLine();

            if(line1!=null) {
                long ID1 = linesRead1.incrementAndGet();

                String[] splittedLine = line1.split("\\[");
                String username = splittedLine[1].replace(",,", "");
                String serializedFeatures = splittedLine[2].replace("]]", "");
                String[] splittedSerializedFeatures = serializedFeatures.split(",");
                List<Double> features =new ArrayList<Double>();
                for (int i = 0; i < splittedSerializedFeatures.length; i++) {
                    features.add(Double.valueOf(splittedSerializedFeatures[i]));
                }
                clusteredFeaturesHashmap0.put(username, features);

            }else if(line2!=null){
                long ID2 = linesRead2.incrementAndGet();

                String [] splittedLine=line2.split("\\[");
                String username=splittedLine[1].replace(",,","");
                String serializedFeatures=splittedLine[2].replace("]]","");
                String[]splittedSerializedFeatures=serializedFeatures.split(",");
                List<Double>features=new ArrayList<Double>();
                for(int i=0;i<splittedSerializedFeatures.length;i++){
                    features.add(Double.valueOf(splittedSerializedFeatures[i]));
                }
                clusteredFeaturesHashmap1.put(username,features);

            }else{
                CassandraSchemaForClusteredFeatures.writeClusteredFeaturesHashmapToCassandraDB(0, SerializeAndDeserializeJavaObjects.serializeJavaStringAndJavaDoublesListHashmap(clusteredFeaturesHashmap0));
                CassandraSchemaForClusteredFeatures.writeClusteredFeaturesHashmapToCassandraDB(1,SerializeAndDeserializeJavaObjects.serializeJavaStringAndJavaDoublesListHashmap(clusteredFeaturesHashmap1));
                spoutOutputCollector.emit(new Values(clusteredFeaturesHashmap0,clusteredFeaturesHashmap1));
                Thread.sleep(60*60*60*60*10000);
            }
        }catch (Exception e){
            throw new RuntimeException(e);
        }

    }

    @Override
    public void ack(Object msgId) {

    }

    @Override
    public void fail(Object msgId) {
        System.out.println("Failed line number "+msgId);
    }

    @Override
    public void deactivate() {
        try{
            bufferedReader1.close();
            bufferedReader2.close();
        }catch (IOException e) {
            e.printStackTrace();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("MAP_1","MAP_2"));

    }
}
