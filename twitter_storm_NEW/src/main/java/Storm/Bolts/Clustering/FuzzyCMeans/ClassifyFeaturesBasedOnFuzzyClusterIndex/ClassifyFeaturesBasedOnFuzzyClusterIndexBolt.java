package Storm.Bolts.Clustering.FuzzyCMeans.ClassifyFeaturesBasedOnFuzzyClusterIndex;

import Storm.Bolts.Clustering.KMeans.ClassifyBasedOnClusterIndex.ClassifyBasedOnClusterIndexBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.List;
import java.util.Map;

/**
 * Created by christina on 7/8/2016.
 */
public class ClassifyFeaturesBasedOnFuzzyClusterIndexBolt extends BaseRichBolt {
    private int clusterIndex;

    OutputCollector outputCollector;

    public ClassifyFeaturesBasedOnFuzzyClusterIndexBolt(int clusterIndex){
        this.clusterIndex=clusterIndex;
    }


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("USERNAME","FEATURES_AS_LIST"));
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
    }

    public void execute(Tuple tuple) {
        int clusterIndex=tuple.getInteger(0);
        String username=tuple.getString(1);
        List<Double>featuresAsList=(List<Double>)tuple.getValue(2);
        double[]featuresAsVector=(double[])tuple.getValue(3);

        if(clusterIndex==this.clusterIndex){
            outputCollector.emit(new Values(username,featuresAsList));
        }
    }
}
