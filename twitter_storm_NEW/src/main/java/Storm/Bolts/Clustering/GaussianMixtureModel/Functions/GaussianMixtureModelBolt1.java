package Storm.Bolts.Clustering.GaussianMixtureModel.Functions;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by christina on 31/8/2016.
 */
public class GaussianMixtureModelBolt1 extends BaseRichBolt {
    OutputCollector outputCollector;

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("CLUSTER_INDEX","GAUSSIAN_PROBABILITIES"));

    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
    }

    public void execute(Tuple tuple) {
        int clusterIndex=(int)tuple.getInteger(0);
        Map<String,double[]>map=(Map<String,double[]>)tuple.getValue(1);

        int noOfUsers=map.keySet().size();
        int sum=0;
        for(Map.Entry<String,double[]>mapEntry:map.entrySet()){
            sum+=mapEntry.getValue().length;
        }
        int noOfFeatures=(int)sum/noOfUsers;
        GaussianMixtureModel.calculateTheGaussianMixtureModelOfAHashmap(outputCollector,clusterIndex,map);

    }
}
