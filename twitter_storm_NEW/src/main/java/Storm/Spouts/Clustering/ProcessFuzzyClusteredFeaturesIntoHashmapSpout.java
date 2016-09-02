package Storm.Spouts.Clustering;

import Databases.NoSQL.CassandraDB.Clustering.CassandraSchemaForClusteredFeatures;
import Databases.NoSQL.CassandraDB.Clustering.CassandraSchemaForFuzzyClusteredFeatures;
import Databases.NoSQL.CassandraDB.Functions.SerializeAndDeserializeJavaObjects;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import clojure.lang.IFn;
import com.google.common.primitives.Doubles;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by christina on 30/8/2016.
 */
public class ProcessFuzzyClusteredFeaturesIntoHashmapSpout extends BaseRichSpout {
    private String filename1;
    private String filename2;
    private String filename3;
    private String filename4;

    SpoutOutputCollector spoutOutputCollector;
    BufferedReader bufferedReader1;
    BufferedReader bufferedReader2;
    BufferedReader bufferedReader3;
    BufferedReader bufferedReader4;

    AtomicLong linesRead1;
    AtomicLong linesRead2;
    AtomicLong linesRead3;
    AtomicLong linesRead4;

    Map<String,double[]>map1;
    Map<String,double[]>map2;
    Map<String,double[]>map3;
    Map<String,double[]>map4;

    Map<Integer,Map<String,double[]>>map;

    public ProcessFuzzyClusteredFeaturesIntoHashmapSpout(String filename1,String filename2,String filename3,String filename4){
        this.filename1=filename1;
        this.filename2=filename2;
        this.filename3=filename3;
        this.filename4=filename4;
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("CLUSTER_INDEX","MAP"));

    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector=spoutOutputCollector;
        try {
            bufferedReader1=new BufferedReader(new FileReader(filename1));
            bufferedReader2=new BufferedReader(new FileReader(filename2));
            bufferedReader3=new BufferedReader(new FileReader(filename3));
            bufferedReader4=new BufferedReader(new FileReader(filename4));
        }catch (IOException e){
            throw new RuntimeException(e);
        }catch (Exception e){
            throw new RuntimeException(e);
        }
        linesRead1=new AtomicLong(0);
        linesRead2=new AtomicLong(0);
        linesRead3=new AtomicLong(0);
        linesRead4=new AtomicLong(0);

        this.map=new HashMap<Integer, Map<String, double[]>>();

        map1=new HashMap<String, double[]>();
        map2=new HashMap<String, double[]>();
        map3=new HashMap<String, double[]>();
        map4=new HashMap<String, double[]>();
    }

    public void nextTuple() {
        try {
            String line1=bufferedReader1.readLine();
            String line2=bufferedReader2.readLine();
            String line3=bufferedReader3.readLine();
            String line4=bufferedReader4.readLine();

            if(line3!=null){
                long ID3=linesRead3.incrementAndGet();
                String [] splittedLine=line3.split("\\[");
                String username=splittedLine[1].replace(",,","");
                String serializedFeatures=splittedLine[2].replace("]]","");
                String [] splittedSerializedFeatures=serializedFeatures.split(",");
                List<Double>features=new ArrayList<Double>();
                for(int i=0;i<splittedSerializedFeatures.length;i++){
                    features.add(Double.valueOf(splittedSerializedFeatures[i]));
                }
                double[]featuresAsVector= Doubles.toArray(features);
                map3.put(username,featuresAsVector);
            }

            if(line4!=null){
                long ID4=linesRead4.incrementAndGet();
                String [] splittedLine=line4.split("\\[");
                String username=splittedLine[1].replace(",,","");
                String serializedFeatures=splittedLine[2].replace("]]","");
                String [] splittedSerializedFeatures=serializedFeatures.split(",");
                List<Double>features=new ArrayList<Double>();
                for(int i=0;i<splittedSerializedFeatures.length;i++){
                    features.add(Double.valueOf(splittedSerializedFeatures[i]));
                }

                double[]featuresAsVector= Doubles.toArray(features);
                map4.put(username,featuresAsVector);

            }
            if(line1!=null){
                long ID1=linesRead1.incrementAndGet();
                String[] splittedLine=line1.split("\\[");
                String username=splittedLine[1].replace(",,,","");
                String serializedFeatures=splittedLine[2].replace("]]","");
                String [] splittedSerializedFeatures=serializedFeatures.split(",");
                List<Double>features=new ArrayList<Double>();
                for(int i=0;i<splittedSerializedFeatures.length;i++){
                    features.add(Double.valueOf(splittedSerializedFeatures[i]));
                }

                double[]featuresAsVector= Doubles.toArray(features);
                map1.put(username,featuresAsVector);
            }else if(line2!=null){
                long ID2=linesRead2.incrementAndGet();
                String [] splittedLine=line2.split("\\[");
                String username=splittedLine[1].replace(",,,","");
                String serializedFeatures=splittedLine[2].replace("]]","");
                String[] splittedSerializedFeatures=serializedFeatures.split(",");
                List<Double>features=new ArrayList<Double>();
                for(int i=0;i<splittedSerializedFeatures.length;i++){
                    features.add(Double.valueOf(splittedSerializedFeatures[i]));
                }

                double[]featuresAsVector= Doubles.toArray(features);
                map2.put(username,featuresAsVector);

            }else{
                map.put(0,map1);
                map.put(1,map2);
                map.put(2,map3);
                map.put(3,map4);
                System.out.println(map);
                for(Integer clusterIndex:map.keySet()){
                    spoutOutputCollector.emit(new Values(clusterIndex,map.get(clusterIndex)));
                }


                Thread.sleep(60*60*60*1000000000);
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
            bufferedReader1.close();
            bufferedReader2.close();
            bufferedReader3.close();
            bufferedReader4.close();
        }catch (IOException e){
            throw new RuntimeException(e);
        }catch (Exception e){
            throw new RuntimeException(e);
        }

    }

    @Override
    public void fail(Object msgId) {

    }
}
