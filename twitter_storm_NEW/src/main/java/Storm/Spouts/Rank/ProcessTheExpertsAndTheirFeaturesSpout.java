package Storm.Spouts.Rank;

import Storm.Bolts.Rank.Functions.CalculateMeansAndCovariance;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.google.common.primitives.Doubles;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by christina on 14/8/2016.
 */
public class ProcessTheExpertsAndTheirFeaturesSpout extends BaseRichSpout {
    private String filename;

    SpoutOutputCollector spoutOutputCollector;
    BufferedReader bufferedReader;
    AtomicLong linesRead;

    Map<String,List<Double>>featuresAsList;
    Map<String,double[]>featuresAsVector;


    public ProcessTheExpertsAndTheirFeaturesSpout(String filename){
        this.filename=filename;
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector=spoutOutputCollector;
        try {
            bufferedReader=new BufferedReader(new FileReader(filename));
        }catch (IOException e){
            throw new RuntimeException(e);
        }catch (Exception e){
            throw new RuntimeException(e);
        }
        linesRead=new AtomicLong(0);
        featuresAsList=new HashMap<String, List<Double>>();
        featuresAsVector=new HashMap<String, double[]>();

    }


    public void nextTuple() {
        try {
            String line=bufferedReader.readLine();
            if(line!=null){
                String[] splittedLine=line.split("\\[");
                String username=splittedLine[1];
                String username1=username.replace(",","");
                String serializedFeatures=splittedLine[2];
                String serializedFeatures1=serializedFeatures.replace("]]","");
                String[] splittedSerializedFeatures=serializedFeatures1.split(",");
                List<Double>featuresAsList=new ArrayList<Double>();
                for(int i=0;i<splittedSerializedFeatures.length;i++){
                    featuresAsList.add(Double.valueOf(splittedSerializedFeatures[i]));
                }
                if(!this.featuresAsList.containsKey(username1)){
                    this.featuresAsList.put(username1,featuresAsList);
                }
                double[]featuresAsVector= Doubles.toArray(featuresAsList);
                if(!this.featuresAsVector.containsKey(username1)){
                    this.featuresAsVector.put(username1, featuresAsVector);
                }
            }else{


                double[]mu= CalculateMeansAndCovariance.calculateTheMeansOfAHashmap(featuresAsVector);
                double [] covariance=CalculateMeansAndCovariance.calculateTheCovarianceOfAHashmap(featuresAsVector);
                for(String key:featuresAsVector.keySet()){
                    spoutOutputCollector.emit(new Values(key,featuresAsVector.get(key),mu,covariance));
                }



                Thread.sleep(60*60*1000);
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
        System.out.println("Failed line number: "+msgId);

    }

    @Override
    public void deactivate() {
        try {
            bufferedReader.close();
        }catch (IOException e){
            throw new RuntimeException(e);
        }catch (Exception e){
            throw new RuntimeException(e);
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("USERNAME","FEATURES_AS_VECTOR","MU","SIGMA"));

    }
}
