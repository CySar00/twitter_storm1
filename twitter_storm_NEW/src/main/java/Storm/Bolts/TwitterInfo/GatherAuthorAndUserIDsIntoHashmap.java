package Storm.Bolts.TwitterInfo;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by christina on 22/7/2016.
 */
public class GatherAuthorAndUserIDsIntoHashmap extends BaseRichBolt {

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    public void execute(Tuple tuple) {

    }
}
