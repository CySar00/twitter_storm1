package Storm.Spouts.Rank.Borda;

import Databases.NoSQL.CassandraDB.CassandraSchemaOfAuthors;
import Databases.NoSQL.CassandraDB.Functions.SerializeAndDeserializeJavaObjects;
import Storm.Bolts.Rank.Functions.CalculateMeansAndCovariance;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import clojure.lang.IFn;
import com.google.common.primitives.Doubles;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by christina on 1/9/2016.
 */
public class FindingTheTargetClusterOfFuzzyClusterSpout extends BaseRichSpout {
    private String filename;
    private int cluster0;
    private int cluster1;

    SpoutOutputCollector spoutOutputCollector;
    BufferedReader bufferedReader;
    AtomicLong linesRead;
    Map<Integer, Map<String,double[]>>map;
    Map<Integer,double[]>mu;


    public FindingTheTargetClusterOfFuzzyClusterSpout(String filename,int cluster0,int cluster1){
        this.filename=filename;
        this.cluster0=cluster0;
        this.cluster1=cluster1;
    }


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector=spoutOutputCollector;
        try {
            bufferedReader=new BufferedReader(new FileReader(filename));
        }catch (Exception e){
            throw new RuntimeException(e);
        }
        linesRead=new AtomicLong(0);
        this.map=new HashMap<Integer, Map<String, double[]>>();
        mu=new HashMap<Integer, double[]>();
    }

    public void nextTuple() {
        try {
            String line=bufferedReader.readLine();
            if(line!=null){
                Map<String,double[]>map=new HashMap<String, double[]>();
                int firstIndex=line.indexOf("[");
                int lastIndex=line.indexOf("]}]");
                String subLine=line.substring(firstIndex+1,lastIndex);
                String [] splittedSubLine=subLine.split(", \\{");
                Integer clusterIndex=Integer.valueOf(splittedSubLine[0]);
                if(clusterIndex==cluster0 || clusterIndex==cluster1){
                    String serializedMap=splittedSubLine[1];
                    String[] serializedMapEntry=serializedMap.split("],");
                    for(int i=0;i<serializedMapEntry.length;i++){
                        String  mapEntry=serializedMapEntry[i];
                        String [] splittedMapEntry=mapEntry.split("=\\[");
                        String username=splittedMapEntry[0];
                        String serializedGaussianProbabilities=splittedMapEntry[1];
                        String[] splittedSerializedGaussianProbabilities=serializedGaussianProbabilities.split(",");
                        List<Double> gaussianProbabilities=new ArrayList<Double>();
                        for(int j=0;j<splittedSerializedGaussianProbabilities.length;j++){
                            gaussianProbabilities.add(Double.valueOf(splittedSerializedGaussianProbabilities[j]));
                        }
                        double[]gaussianProbabilities1= Doubles.toArray(gaussianProbabilities);
                        for(int j=0;j<gaussianProbabilities1.length;j++){
                            if(gaussianProbabilities1[j]>0.9){
                                map.put(username,gaussianProbabilities1);
                            }
                        }
                    }
                    double[]mu=CalculateMeansAndCovariance.calculateTheMeansOfAHashmap(map);
                    if(clusterIndex==cluster0){
                        this.map.put(cluster0,map);
                        this.mu.put(cluster0,mu);
                    }
                    if(clusterIndex==cluster1){
                        this.map.put(cluster1,map);
                        this.mu.put(cluster1,mu);
                    }
                }
            }else {
                double[] mu0=mu.get(cluster0);
                double[] mu1=mu.get(cluster1);

                double[] clusterIndices=new double[mu0.length];
                for(int i=0;i<mu0.length;i++){
                    if(mu0[i]>mu1[i]){
                        clusterIndices[i]=0;
                    }else if(mu1[i]>mu0[i]){
                        clusterIndices[i]=1;
                    }
                }
                double sumOfClusterIndices=0;
                for(int i=0;i<clusterIndices.length;i++){
                    sumOfClusterIndices+=clusterIndices[i];
                }
                double muOfSum=sumOfClusterIndices/clusterIndices.length;
                if(muOfSum<0.5){
                    Map<String,double[]>map=this.map.get(cluster0);
                    Map<String,List<Double>>map1=new HashMap<String, List<Double>>();
                    List<String>expertsOfFuzzyCluster=new ArrayList<String>();
                    for(Map.Entry<String,double[]>mapEntry:map.entrySet()){
                        String username=mapEntry.getKey();
                        double []gaussianProbabilitiesAsVector=mapEntry.getValue();
                        List<Double>gaussianProbabilitiesAsList=Doubles.asList(gaussianProbabilitiesAsVector);
                        map1.put(username,gaussianProbabilitiesAsList);
                        expertsOfFuzzyCluster.add(username);
                    }
                    CassandraSchemaOfAuthors.writeTheAuthorsToCassandraDatabase(SerializeAndDeserializeJavaObjects.serializeJavaStringList(expertsOfFuzzyCluster));

                }else if(muOfSum>=0.5){
                    Map<String,double[]>map=this.map.get(cluster1);
                    Map<String,List<Double>>map1=new HashMap<String, List<Double>>();
                    List<String>expertsOfFuzzyCluster=new ArrayList<String>();
                    for(Map.Entry<String,double[]>mapEntry:map.entrySet()){
                        String username=mapEntry.getKey();
                        double[] gaussianProbabilitiesAsVector=mapEntry.getValue();
                        List<Double>gaussianProbablitiesAsList=Doubles.asList(gaussianProbabilitiesAsVector);
                        map1.put(username,gaussianProbablitiesAsList);
                        expertsOfFuzzyCluster.add(username);
                    }
                    CassandraSchemaOfAuthors.writeTheAuthorsToCassandraDatabase(SerializeAndDeserializeJavaObjects.serializeJavaStringList(expertsOfFuzzyCluster));
                }


                System.out.println(this.map);
                Thread.sleep(60*60*10000);
            }
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public void ack(Object msgId) {

    }

    @Override
    public void fail(Object msgId) {

    }

    @Override
    public void deactivate() {

    }
}
