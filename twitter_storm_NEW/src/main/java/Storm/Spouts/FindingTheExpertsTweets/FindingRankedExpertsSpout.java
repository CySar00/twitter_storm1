package Storm.Spouts.FindingTheExpertsTweets;

import Databases.NoSQL.CassandraDB.CassandraSchemaOfAuthors;
import Databases.NoSQL.CassandraDB.Functions.SerializeAndDeserializeJavaObjects;
import Storm.Spouts.Rank.Borda.FindingTheExpertsOfFuzzyClusterSpout1;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by christina on 1/9/2016.
 */
public class FindingRankedExpertsSpout extends BaseRichSpout {
    private String filename;

    SpoutOutputCollector spoutOutputCollector;
    BufferedReader bufferedReader;
    AtomicLong linesRead;
    List<String>experts;

    public FindingRankedExpertsSpout(String filename){
        this.filename=filename;
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector=spoutOutputCollector;
        try {
            bufferedReader=new BufferedReader(new FileReader(filename));
        }catch (Exception e){
            throw new RuntimeException(e);
        }
        linesRead=new AtomicLong(0);
        experts=new ArrayList<String>();

    }

    public void nextTuple() {
        try {
            String line=bufferedReader.readLine();
            if(line!=null){
                int firstIndex=line.indexOf("[");
                int lastIndex=line.indexOf("]");
                String subLine=line.substring(firstIndex+1,lastIndex);
                String[] splittedSubLine=subLine.split(", ");
                String expertsAndBordaScore=splittedSubLine[1];
                String[] splittedExpertsAndBordaScore=expertsAndBordaScore.split("  ,");
                String username=splittedExpertsAndBordaScore[0];
               // System.out.println(username);
                experts.add(username);


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
    public void deactivate() {
        try {
            bufferedReader.close();
        }catch (Exception e){
            throw new RuntimeException(e);
        }

    }

    @Override
    public void fail(Object msgId) {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
