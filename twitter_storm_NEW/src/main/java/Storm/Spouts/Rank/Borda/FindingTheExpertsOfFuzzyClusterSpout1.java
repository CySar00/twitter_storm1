package Storm.Spouts.Rank.Borda;

import Databases.NoSQL.CassandraDB.CassandraSchemaOfAuthors;
import Databases.NoSQL.CassandraDB.Functions.SerializeAndDeserializeJavaObjects;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by christina on 1/9/2016.
 */
public class FindingTheExpertsOfFuzzyClusterSpout1 extends BaseRichSpout {
    private String filename;
    private int cluster0;
    private int cluster1;

    SpoutOutputCollector spoutOutputCollector;
    BufferedReader bufferedReader;
    AtomicLong atomicLong;
    Map<String,List<Double>>map;
    List<String>experts;

    public FindingTheExpertsOfFuzzyClusterSpout1(String filename,int cluster0,int cluster1){
        this.filename=filename;
        this.cluster0=cluster0;
        this.cluster1=cluster1;
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector=spoutOutputCollector;
        try {
            bufferedReader=new BufferedReader(new FileReader(filename));
        }catch (Exception e){
            throw new RuntimeException(e);
        }
        atomicLong=new AtomicLong(0);
        this.map=new HashMap<String, List<Double>>();
        experts=new ArrayList<String>();
    }

    public void nextTuple() {
        try {
            String line=bufferedReader.readLine();
            if(line!=null){
                int firstIndex=line.indexOf("[");
                int lastIndex=line.indexOf("]}]");
                String subLine=line.substring(firstIndex+1,lastIndex);
                String [] splittedSubLine=subLine.split(",");
                Integer clusterIndex=Integer.valueOf(splittedSubLine[0]);
                if(clusterIndex==cluster0 || clusterIndex==cluster1) {
                    StringBuilder stringBuilder = new StringBuilder();
                    stringBuilder.append(splittedSubLine[1]);
                    for(int i=2;i<splittedSubLine.length;i++){
                        stringBuilder.append(",").append(splittedSubLine[i]);
                    }
                    String serializedMap=stringBuilder.toString().replace("{","");
                    if(!serializedMap.contains("NaN")) {
                        String[] mapEntries = serializedMap.split("],");
                        for (int i = 0; i < mapEntries.length; i++) {
                            String mapEntry = mapEntries[i];
                            String[] splittedMapEntry = mapEntry.split("=\\[");
                            String username = splittedMapEntry[0];
                            String serializedGaussianProbabilities = splittedMapEntry[1];
                            String [] splittedSerializedGaussianProbabilities=serializedGaussianProbabilities.split(",");
                            List<Double>gaussianProbabilities=new ArrayList<Double>();
                            for(int j=0;j<splittedSerializedGaussianProbabilities.length;j++){
                                gaussianProbabilities.add(Double.valueOf(splittedSerializedGaussianProbabilities[j]));
                            }
                            map.put(username,gaussianProbabilities);
                            experts.add(username);
                        }
                    }
                }
            }else{
                CassandraSchemaOfAuthors.writeTheAuthorsToCassandraDatabase(SerializeAndDeserializeJavaObjects.serializeJavaStringList(experts));
                Thread.sleep(60*60*1000);
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

    }

    @Override
    public void deactivate() {
        try {
            bufferedReader.close();
        }catch (Exception e){
            throw new RuntimeException(e);
        }

    }
}
