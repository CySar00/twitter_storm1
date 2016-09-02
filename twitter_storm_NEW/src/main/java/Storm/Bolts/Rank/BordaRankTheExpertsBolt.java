package Storm.Bolts.Rank;

import Storm.Bolts.Rank.Functions.BordaRank;
import Storm.Bolts.Rank.Functions.RankAggregation;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by christina on 31/8/2016.
 */
public class BordaRankTheExpertsBolt extends BaseRichBolt {
    OutputCollector outputCollector;
    Map<String,Map<String,Double>>scores;
    RankAggregation<String>rankAggregation;

    Map<String,Double>map;
    Map<String,Double>sortedMap;


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("INDEX","USERNAME","SCORE"));

    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;

        scores=new HashMap<String, Map<String, Double>>();
        rankAggregation=new RankAggregation<String>();
        this.map=new HashMap<String, Double>();
        sortedMap=new HashMap<String, Double>();
    }

    public void execute(Tuple tuple) {
        Map<String,double[]>map=(Map<String,double[]>)tuple.getValue(0);
        int noOfExperts=map.keySet().size();
        int sum=0;
        for(Map.Entry<String,double[]>mapEntry:map.entrySet()){
            double[]values=mapEntry.getValue();
            sum+=values.length;
        }
        int noOfFeautures=(int)sum/noOfExperts;

        for(String username:map.keySet()){
            scores.put(username,new HashMap<String, Double>());
            String str=null;

            for(int i=0;i<map.get(username).length;i++){
                str="Metric".concat(String.valueOf(i));
                scores.get(username).put(str,map.get(username)[i]);
            }
        }
        Map<String,Double>[]aux=rankAggregation.processMap(scores);
        Map<String,Double>borda=rankAggregation.bordaFusion(aux);
        for(String username:borda.keySet()){
            this.map.put(username,borda.get(username));
        }
        sortedMap=BordaRank.sortExpertsUsingBordaScoreValues(this.map);
        int count=0;
        for(String username:sortedMap.keySet()){
            System.out.println(username+" ---->>>> "+sortedMap.get(username));
            outputCollector.emit(new Values(count,username,sortedMap.get(username)));
            count+=1;
        }











    }
}
