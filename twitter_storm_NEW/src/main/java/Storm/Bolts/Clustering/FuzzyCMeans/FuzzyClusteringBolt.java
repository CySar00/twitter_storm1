package Storm.Bolts.Clustering.FuzzyCMeans;

import Databases.NoSQL.CassandraDB.Clustering.CassandraSchemaForFuzzyCCentroids;
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
 * Created by christina on 7/8/2016.
 */
public class FuzzyClusteringBolt extends BaseRichBolt {
    private double fuzzy;

    OutputCollector outputCollector;
    Map<Integer,double[]>fuzzyCentroids;
    int[]n;


    public FuzzyClusteringBolt(double fuzzy){
        this.fuzzy=fuzzy;
    }

    public void getFuzzyCentroidsFromCassandraDB(){
        String[] serializedFuzzyCentroids= CassandraSchemaForFuzzyCCentroids.getCentroidsForFuzzyMeansClustering();
        try {
            for (int i = 0; i < serializedFuzzyCentroids.length; i++) {
                fuzzyCentroids.put(i, SerializeAndDeserializeJavaObjects.deserializeJavaDoublesVector(serializedFuzzyCentroids[i]));
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void updateFuzzyCentroidsToCassandraDB(int index,double[]fuzzyCentroidAsVector){
        String serializedFuzzyCentroid=SerializeAndDeserializeJavaObjects.serializeJavaDoublesVector(fuzzyCentroidAsVector);
        CassandraSchemaForFuzzyCCentroids.setCentroidsForFuzzyMeansClustering(index,serializedFuzzyCentroid);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("CLUSTER_INDEX","USERNAME","FEATURES_AS_LIST","FEATURES_AS_VECTOR"));

    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
        fuzzyCentroids=new HashMap<Integer, double[]>();
        n=new int[30];
        getFuzzyCentroidsFromCassandraDB();

    }

    public void execute(Tuple tuple) {
        int clusterIndex=(int)tuple.getInteger(0);
        String username=tuple.getString(1);
        List<Double> featuresAsList=(List<Double>)tuple.getValue(2);
        double[]featuresAsVector=(double[])tuple.getValue(3);
        double[][]memberships=(double[][])tuple.getValue(4);

        double[]fuzzyCentroid=fuzzyCentroids.get(clusterIndex);
        double[] results=new double[featuresAsVector.length];
        if(fuzzyCentroid!=null && featuresAsVector!=null && memberships!=null){
            n[clusterIndex]+=1;

            double total=0;
            for(int i=0;i<memberships.length;i++){
                double[] membership=memberships[i];
                for(int j=0;j<membership.length;j++){
                    total+=Math.pow(membership[i],fuzzy);
                }
            }

            for(int i=0;i<memberships.length;i++){
                double[]membership=memberships[i];
                double sum=0;
                for(int j=0;j<membership.length;j++){
                    sum+=Math.pow(membership[i],fuzzy)*featuresAsVector[j];
                    results[j]=sum/total;
                }
            }
            fuzzyCentroids.put(clusterIndex,results);
            updateFuzzyCentroidsToCassandraDB(clusterIndex,results);
        }else{
            double total=0;
            for(int i=0;i<memberships.length;i++){
                double[]membership=memberships[i];
                for(int j=0;j<membership.length;j++){
                    total+=Math.pow(membership[i],fuzzy);
                }
            }
            for(int i=0;i<memberships.length;i++){
                double sum=0;
                double[]membership=memberships[i];
                for(int j=0;j<membership.length;j++){
                    sum+=Math.pow(membership[j],fuzzy)*featuresAsVector[j];
                    results[j]=sum/total;
                }
            }
            fuzzyCentroids.put(clusterIndex,results);
            updateFuzzyCentroidsToCassandraDB(clusterIndex,results);
        }
        System.out.println(clusterIndex+" "+username);
        outputCollector.emit(tuple,new Values(clusterIndex,username,featuresAsList,featuresAsVector));

    }
}
