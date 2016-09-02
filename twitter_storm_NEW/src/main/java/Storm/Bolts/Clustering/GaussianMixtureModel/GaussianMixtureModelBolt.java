package Storm.Bolts.Clustering.GaussianMixtureModel;

import Storm.Bolts.Clustering.GaussianMixtureModel.Functions.CalcluateKeySizeAndValuesSizeOfHashmap;
import Storm.Bolts.Clustering.GaussianMixtureModel.Functions.CalculateMeansAndVariance;
import Storm.Bolts.Clustering.GaussianMixtureModel.Functions.Gaussian;
import Storm.Bolts.Clustering.GaussianMixtureModel.Functions.GaussianMixtureModel;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.Collections;
import java.util.Map;

/**
 * Created by christina on 8/8/2016.
 */
public class GaussianMixtureModelBolt extends BaseRichBolt {
    OutputCollector outputCollector;



    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("CLUSTER_INDEX","GAUSSIAN_PROBABILITIES"));

    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;

    }

    public void execute(Tuple tuple) {
        int clusterIndex=(int)tuple.getInteger(0);
        Map<String,double[]>featuresOfTheCluster=(Map<String,double[]>)tuple.getValue(1);
        int noOfUsers= CalcluateKeySizeAndValuesSizeOfHashmap.calculateTheKeySizeOfAHashmap(featuresOfTheCluster);
        int noOfFeatures=CalcluateKeySizeAndValuesSizeOfHashmap.calculateTheValuesSizeOfAHashmap(featuresOfTheCluster);

        GaussianMixtureModel.calculateTheGaussianMixtureModelOfAHashmap(outputCollector,clusterIndex,featuresOfTheCluster);




    }
}
