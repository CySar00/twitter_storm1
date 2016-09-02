package Storm.Bolts.Clustering.FuzzyCMeans;

import Databases.NoSQL.CassandraDB.Clustering.CassandraSchemaForFuzzyCCentroids;
import Storm.Bolts.Clustering.FuzzyCMeans.Functions.EmitFeaturesIntoClosestFuzzyCluster;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by christina on 7/8/2016.
 */
public class EmitFeaturesIntoClosestFuzzyClusterBolt extends BaseRichBolt {
    private double fuzzy;

    OutputCollector outputCollector;
    Map<String,List<Double>>featuresAsList;
    Map<String,double[]>featuresAsVector;
    double[][]membership;

    public EmitFeaturesIntoClosestFuzzyClusterBolt(double fuzzy){
        this.fuzzy=fuzzy;
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("CLUSTER_INDEX","USERNAME","FEATURES_AS_LIST","FEATURES_AS_VECTOR","MEMBERSHIP"));

    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
        featuresAsList=new HashMap<String, List<Double>>();
        featuresAsVector=new HashMap<String, double[]>();

    }

    public void execute(Tuple tuple) {
        String username=tuple.getString(0);
        List<Double>featuresAsList1=(List<Double>)tuple.getValue(1);
        double[]featuresAsVector1=(double[])tuple.getValue(2);
        membership=(double[][])tuple.getValue(3);

        List<Double>featuresAsList=this.featuresAsList.get(username);
        if(featuresAsList==null){
            featuresAsList=new ArrayList<Double>();
        }
        featuresAsList=featuresAsList1;
        this.featuresAsList.put(username,featuresAsList);

        double[]featuresAsVector=this.featuresAsVector.get(username);
        if(featuresAsVector==null){
            featuresAsVector=new double[featuresAsVector1.length];
        }
        featuresAsVector = featuresAsVector1;
        this.featuresAsVector.put(username,featuresAsVector);
        String[]serializedFuzzyCentroids= CassandraSchemaForFuzzyCCentroids.getCentroidsForFuzzyMeansClustering();
        EmitFeaturesIntoClosestFuzzyCluster.emitFeaturesIntoClosestFuzzyCluster(outputCollector,serializedFuzzyCentroids,username,featuresAsList1,featuresAsVector1,membership,fuzzy);


    }
}
