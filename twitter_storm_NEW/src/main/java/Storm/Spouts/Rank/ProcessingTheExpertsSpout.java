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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by christina on 12/8/2016.
 */
public class ProcessingTheExpertsSpout extends BaseRichSpout {
    private String filename;

    SpoutOutputCollector spoutOutputCollector;
    BufferedReader bufferedReader;
    AtomicLong linesRead;
    List<String>theExperts;

    public ProcessingTheExpertsSpout(String filename){
        this.filename=filename;
    }


    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector=spoutOutputCollector;
        try {
            bufferedReader=new BufferedReader(new FileReader(filename));
        }catch (Exception e){
            e.printStackTrace();
        }
        linesRead=new AtomicLong(0);
        theExperts=new ArrayList<String>();
    }

    public void nextTuple() {
        try {
            String line=bufferedReader.readLine();

            if(line!=null){
                long ID=linesRead.incrementAndGet();
                String [] splittedLine=line.split(",");
                String username=splittedLine[3].replace("[","");
                System.out.println(username);
                if(!theExperts.contains(username)){
                    theExperts.add(username);
                }

            }else{
                if(theExperts.size()>0) {
                    CassandraSchemaOfAuthors.writeTheAuthorsToCassandraDatabase(SerializeAndDeserializeJavaObjects.serializeJavaStringList(theExperts));
                }
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

    }
}
