package Storm.Bolts.Clustering.KMeans;

import Databases.NoSQL.CassandraDB.Clustering.CassandraSchemaForKCentroids;
import Databases.NoSQL.CassandraDB.Functions.SerializeAndDeserializeJavaObjects;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by christina on 4/8/2016.
 */
public class KClusteringBolt extends BaseRichBolt {
    OutputCollector outputCollector;

    Map<Integer,Map<String,double[]>>clusteredAuthorsAndFeatures;
    Map<Integer,double[]>clusteredFeatures;
    Map<String,Integer>counts;
    int []n;

    public void getInitialCentroidsFromCassandraDB(){
        String[]serializedCentroidsFromDatabase= CassandraSchemaForKCentroids.getCentroidsForKMeansClustering();
        for(int i=0;i<serializedCentroidsFromDatabase.length;i++){
            try {
                clusteredFeatures.put(i, SerializeAndDeserializeJavaObjects.deserializeJavaDoublesVector(serializedCentroidsFromDatabase[i]));
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public void updateCetnroidsToCassandraDB(int index,double[]vectorToBeUpdated){
        String serializedVectorOfCentroid=SerializeAndDeserializeJavaObjects.serializeJavaDoublesVector(vectorToBeUpdated);
        CassandraSchemaForKCentroids.setCentroidsForKMeansClustering(index,serializedVectorOfCentroid);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("CLUSTER_INDEX","USERNAME","VECTOR_OF_FEATURES","FEATURES_AS_LIST"));

    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
        clusteredAuthorsAndFeatures=new HashMap<Integer, Map<String, double[]>>();
        clusteredFeatures=new HashMap<Integer, double[]>();
        n=new int[30];
        getInitialCentroidsFromCassandraDB();

    }

    public void execute(Tuple tuple) {
        String[]serializedCentroids=CassandraSchemaForKCentroids.getCentroidsForKMeansClustering();
        System.out.println(serializedCentroids[0]);
        System.out.println(serializedCentroids[1]);

        int clusterIndex=(int)tuple.getInteger(0);
        String username=tuple.getString(1);
        double[]featuresAsVector=(double[])tuple.getValue(2);
        List<Double>featuresAsList=(List<Double>)tuple.getValue(3);

        double[]centroidsOfClusters=clusteredFeatures.get(clusterIndex);
        double[]result=new double[featuresAsVector.length];
        if(centroidsOfClusters!=null && featuresAsVector!=null){
            n[clusterIndex]+=1;
            for(int i=0;i<centroidsOfClusters.length;i++){
                result[i]=(n[clusterIndex]*centroidsOfClusters[i]+featuresAsVector[i]/(n[clusterIndex]+1));
            }
            clusteredFeatures.put(clusterIndex,result);
            updateCetnroidsToCassandraDB(clusterIndex,result);
        }else{
            result=featuresAsVector;
            clusteredFeatures.put(clusterIndex,result);
            updateCetnroidsToCassandraDB(clusterIndex,result);

        }
        System.out.println(clusterIndex);
        outputCollector.emit(new Values(clusterIndex,username,featuresAsVector,featuresAsList));
    }
}
