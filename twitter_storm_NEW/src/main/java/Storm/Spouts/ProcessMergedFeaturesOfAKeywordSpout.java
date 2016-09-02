package Storm.Spouts;

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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by christina on 4/8/2016.
 */
public class ProcessMergedFeaturesOfAKeywordSpout extends BaseRichSpout {
    private String filename;

    private SpoutOutputCollector spoutOutputCollector;
    private BufferedReader bufferedReader;
    private AtomicLong linesRead;


    public ProcessMergedFeaturesOfAKeywordSpout(String filename){
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

    }

    public void nextTuple() {
        try {
            String line=bufferedReader.readLine();
            if(line!=null){
                long ID=linesRead.incrementAndGet();
                String[] splittedLine=line.split("\\[");

                String username=splittedLine[1].substring(0,splittedLine[1].length()-1);
                String serializedFeatures=splittedLine[2].substring(0,splittedLine[2].length()-2);
                String[] splittedSerializedFeatures=serializedFeatures.split(",");
                List<Double> features=new ArrayList<Double>();
                for(int i=0;i<splittedSerializedFeatures.length;i++){
                    features.add(Double.valueOf(splittedSerializedFeatures[i]));
                }
                double[] vectorizedFeatures= Doubles.toArray(features);
                if(vectorizedFeatures.length==12) {
                    spoutOutputCollector.emit(new Values(username, features, vectorizedFeatures));
                }
            }else{
                Thread.sleep(60*60*60*1000);
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
            outputFieldsDeclarer.declare(new Fields("USERNAME","LIST_OF_FEATURES","VECTOR_OF_FEATURES"));
    }
}
