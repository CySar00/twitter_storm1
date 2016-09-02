package Storm.Spouts.Rank.Borda;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.google.appengine.repackaged.com.google.common.primitives.Doubles;

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
public class ProcesssTheExpertsAndTheirFeaturesSpout1 extends BaseRichSpout {
    private String filename;

    SpoutOutputCollector spoutOutputCollector;
    BufferedReader bufferedReader;
    AtomicLong linesRead;

    Map<String,double[]>map;

    public ProcesssTheExpertsAndTheirFeaturesSpout1(String filename){
        this.filename=filename;
    }


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("MAP"));
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector=spoutOutputCollector;
        try {
            bufferedReader=new BufferedReader(new FileReader(filename));
        }catch (Exception e){
            throw new RuntimeException(e);
        }
        linesRead=new AtomicLong(0);
        this.map=new HashMap<String, double[]>();
    }

    public void nextTuple() {
        try {
            String line=bufferedReader.readLine();
            if(line!=null){
                long ID=linesRead.incrementAndGet();
                String [] splittedLine=line.split(",");
                String username=splittedLine[3].replace("[","");
                List<Double> featuresAsList=new ArrayList<Double>();
                Double firstFeature=Double.valueOf(splittedLine[4].replace("[",""));
                featuresAsList.add(firstFeature);
                for(int i=5;i<splittedLine.length-1;i++){
                    featuresAsList.add(Double.valueOf(splittedLine[i]));
                }
                Double lastFeature=Double.valueOf(splittedLine[splittedLine.length-1].replace("]]",""));
                featuresAsList.add(lastFeature);
                double[] featuresAsVector= Doubles.toArray(featuresAsList);
                map.put(username,featuresAsVector);
            }else{
                spoutOutputCollector.emit(new Values(map));
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
    public void deactivate() {
        try {
            bufferedReader.close();
        }catch (Exception e){
            throw new RuntimeException(e);
        }

    }

    @Override
    public void fail(Object msgId) {

    }
}
