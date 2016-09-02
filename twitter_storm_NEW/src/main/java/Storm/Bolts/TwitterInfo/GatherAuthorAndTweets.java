package Storm.Bolts.TwitterInfo;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.Status;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by christina on 22/7/2016.
 */
public class GatherAuthorAndTweets extends BaseRichBolt {
    OutputCollector outputCollector;
    Map<String,List<Status>>tweets;

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("MAP"));

    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
        tweets=new HashMap<String, List<Status>>();
    }

    public void execute(Tuple tuple) {
        String username=tuple.getString(0);
        Status tweet=(Status)tuple.getValue(1);
        String hashtag=tuple.getString(2);

        List<Status>tweets=this.tweets.get(username);
        if(tweets==null){
            tweets=new ArrayList<Status>();
        }
        tweets.add(tweet);
        this.tweets.put(username,tweets);
        System.out.println(this.tweets);
        outputCollector.emit(tuple,new Values(this.tweets));

    }
}
