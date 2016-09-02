package Storm.Bolts.Clustering.KMeans;

import Databases.NoSQL.CassandraDB.Clustering.CassandraSchemaForKCentroids;
import Storm.Bolts.Clustering.KMeans.Functions.EmitFeaturesIntoClosestKClusters;
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
 * Created by christina on 4/8/2016.
 */
public class EmitFeaturesIntoClosestKClustersBolt extends BaseRichBolt {
    OutputCollector outputCollector;
    Map<String,List<Double>>featuresAsList;
    Map<String,double[]>featuresAsVector;


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("CLUSTER_INDEX","USERNAME","VECTOR_OF_FEATURES","LIST_OF_FEATURES"));


    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
        featuresAsList=new HashMap<String, List<Double>>();
        featuresAsVector=new HashMap<String, double[]>();

    }

    public void execute(Tuple tuple) {
        String username=tuple.getString(0);
        List<Double> listOfFeatures=(List<Double>)tuple.getValue(1);
        double[] vectorOfFeatures=(double[])tuple.getValue(2);

        List<Double>featureAsList=this.featuresAsList.get(username);
        if(featureAsList==null){
            featureAsList=new ArrayList<Double>();
        }
        featureAsList=listOfFeatures;
        this.featuresAsList.put(username,featureAsList);

        double[]featuresAsVector=this.featuresAsVector.get(username);
        if(featuresAsVector==null){
            featuresAsVector=new double[vectorOfFeatures.length];
        }
        featuresAsVector=vectorOfFeatures;
        this.featuresAsVector.put(username,featuresAsVector);


        String[] serializedCentroids=CassandraSchemaForKCentroids.getCentroidsForKMeansClustering();
        EmitFeaturesIntoClosestKClusters.emitFeaturesIntoClusterWithClosestCentroid(outputCollector,serializedCentroids,username,vectorOfFeatures,listOfFeatures);
      }
}
