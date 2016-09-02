package Storm.Spouts.Rank;

import Databases.NoSQL.CassandraDB.CassandraSchemaOfAuthors;
import Databases.NoSQL.CassandraDB.Functions.SerializeAndDeserializeJavaObjects;
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
 * Created by christina on 29/8/2016.
 */
public class ProcessMapsWithGaussianProbabilitiesWithNaNValuesSpout extends BaseRichSpout {
    private String filename;

    private SpoutOutputCollector spoutOutputCollector;
    private AtomicLong linesRead;
    private BufferedReader bufferedReader;
    private Map<String,List<Double>>map;
    private List<String>list;


    public ProcessMapsWithGaussianProbabilitiesWithNaNValuesSpout(String filename){
        this.filename=filename;
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

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
        list=new ArrayList<String>();

    }

    public void nextTuple() {
        try {
            String line=bufferedReader.readLine();
            if(line!=null){
                long ID=linesRead.incrementAndGet();

                int firstIndex=line.indexOf("{}");
                int lastIndex=line.indexOf("]}]");
                String subLine=line.substring(firstIndex+5,lastIndex);
                String[] splittedSubLine=subLine.split(", \\{");
                Integer clusterIndex=Integer.valueOf(splittedSubLine[0]);
                String serializedMap=splittedSubLine[1];
                String [] serializedMapEntries=serializedMap.split("],");
                for(int i=0;i<serializedMapEntries.length;i++){
                    String[]splittedSerializedMapEntries=serializedMapEntries[i].split("=\\[");
                    String username=splittedSerializedMapEntries[0];
                    String serializedFeatures=splittedSerializedMapEntries[1];
                    if(!serializedFeatures.contains("NaN")){
                        List<Double>features=new ArrayList<Double>();
                        String[]splittedSerializedFeatures=serializedFeatures.split(",");
                        for(int j=0;j<splittedSerializedFeatures.length;j++){
                            features.add(Double.valueOf(splittedSerializedFeatures[j]));
                        }

                        if(!map.containsKey(username)){
                            map.put(username,features);
                        }
                    }
                }

            }else {
             //   System.out.println(map);
                for(String username:map.keySet()){
                    if(!list.contains(username)){
                        list.add(username);
                    }
                }
                System.out.println(list);
                CassandraSchemaOfAuthors.writeTheAuthorsToCassandraDatabase(SerializeAndDeserializeJavaObjects.serializeJavaStringList(list));
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

    }
}
