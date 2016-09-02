package Storm.Bolts.Rank;

import Storm.Bolts.Rank.Functions.BordaRank;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by christina on 1/9/2016.
 */
public class ReRankTheExpertsUsingBordaScoresBolt extends BaseRichBolt{
    private OutputCollector outputCollector;


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("INDEX","USERNAME","SCORE"));

    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;

    }

    public void execute(Tuple tuple) {
        Map<String,Double>map=(Map<String,Double>)tuple.getValue(0);
        Map<String,Double>reRankMap= BordaRank.sortExpertsUsingBordaScoreValues(map);
        int count=0;
        for(String expert:reRankMap.keySet()){
            outputCollector.emit(new Values(count,expert,reRankMap.get(expert)));
            count+=1;
        }
    }
}
