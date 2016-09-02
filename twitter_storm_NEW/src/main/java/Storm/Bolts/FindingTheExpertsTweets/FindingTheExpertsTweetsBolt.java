package Storm.Bolts.FindingTheExpertsTweets;

import Databases.NoSQL.CassandraDB.CassandraSchemaOfAuthors;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.cassandra.thrift.Cassandra;
import twitter4j.Status;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by christina on 1/9/2016.
 */
public class FindingTheExpertsTweetsBolt extends BaseRichBolt {
    OutputCollector outputCollector;
    Map<String,List<String>>tweets;

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("USERNAME","TWEETS"));

    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
        tweets=new HashMap<String, List<String>>();

    }

    public void execute(Tuple tuple) {
        String username=tuple.getString(0);
        Status tweet=(Status)tuple.getValue(1);

        String experts=CassandraSchemaOfAuthors.readTheAuthorsFromCassandraDatabase();
        if(experts.contains(username)){
            System.out.println(username);

            String tweet1=tweet.getText();
            List<String>tweets=this.tweets.get(username);
            if(tweets==null){
                tweets=new ArrayList<String>();
            }
            tweets.add(tweet1);
            this.tweets.put(username,tweets);
            outputCollector.emit(new Values(username,tweets));

        }

    }
}
