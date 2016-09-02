package Storm.Spouts.Rank;

import Databases.NoSQL.CassandraDB.Clustering.CassandraSchemaForClusteredFeatures;
import Databases.NoSQL.CassandraDB.Clustering.CassandraSchemaForGaussianProbabilitiesOfClusteredFeatures;
import Databases.NoSQL.CassandraDB.Functions.SerializeAndDeserializeJavaObjects;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.google.appengine.repackaged.com.google.common.primitives.Floats;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by christina on 11/8/2016.
 */
public class ProcessMapsWithGaussianProbabilitiesSpout extends BaseRichSpout {
    private String filename;

    SpoutOutputCollector spoutOutputCollector;
    BufferedReader bufferedReader;
    AtomicLong linesRead;


    public ProcessMapsWithGaussianProbabilitiesSpout(String filename){
        this.filename=filename;
    }


    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector=spoutOutputCollector;
        try {
            bufferedReader=new BufferedReader(new FileReader(filename));
        }catch (IOException e){
            e.printStackTrace();
        }catch (Exception e){
            e.printStackTrace();
        }
        linesRead=new AtomicLong();


    }

    public void nextTuple() {
        try {
            String line=bufferedReader.readLine();
            if(line!=null){
                long ID=linesRead.incrementAndGet();
                int firstIndex=line.indexOf("{}");
                int lastIndex=line.indexOf("]}]");
                String subLine=line.substring(firstIndex+5,lastIndex);
                String [] splittedSubLine=subLine.split(", \\{");
                Integer clusterIndex=Integer.parseInt(splittedSubLine[0]);
                String serializedMapEntries=splittedSubLine[1];
                StringBuilder stringBuilder=new StringBuilder();
                stringBuilder.append("{").append(serializedMapEntries).append("]}");
                String serializedMap=stringBuilder.toString();

                Map<String,float[]>map=SerializeAndDeserializeJavaObjects.deserializeJavaStringAndFloatsVectorHashmap(serializedMap);
                for(String username:map.keySet()){
                    float [] values=map.get(username);
                    spoutOutputCollector.emit(new Values(clusterIndex,username,values));
                }
            }else{



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
    public void fail(Object msgId) {
        System.out.println("Failed line number: "+msgId);

    }

    @Override
    public void deactivate() {
        try {
            bufferedReader.close();
        }catch (IOException e){
            e.printStackTrace();
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("CLUSTER_INDEX","USERNAME","GAUSSIAN_PROBABILITIES"));

    }
}
