package Storm.Bolts.Rank;

import Storm.Bolts.Rank.Functions.GaussRank;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by christina on 29/8/2016.
 */
public class GaussRankTheExpertsBolt extends BaseRichBolt {
    OutputCollector outputCollector;


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("INDEX","USERNAME","SCORE"));
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;

    }

    public void execute(Tuple tuple) {
        Map<String,double[]>CDFs=(Map<String,double[]>)tuple.getValue(0);
        GaussRank.rankExpertsUsingGaussianRankMethod(outputCollector,CDFs);

    }
}
