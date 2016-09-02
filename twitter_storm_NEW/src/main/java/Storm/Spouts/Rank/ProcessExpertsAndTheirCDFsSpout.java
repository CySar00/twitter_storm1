package Storm.Spouts.Rank;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.google.appengine.repackaged.com.google.common.primitives.Doubles;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by christina on 29/8/2016.
 */
public class ProcessExpertsAndTheirCDFsSpout extends BaseRichSpout {
    private String filename;

    private SpoutOutputCollector spoutOutputCollector;
    private BufferedReader bufferedReader;
    private AtomicLong linesRead;

    Map<String,double[]>CDFs;

    public ProcessExpertsAndTheirCDFsSpout(String filename){
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
        CDFs=new HashMap<String, double[]>();
    }

    public void nextTuple() {
        try {
            String line=bufferedReader.readLine();
            if(line!=null){
                String[] splittedLine=line.split("\\[");
                String username=splittedLine[1].replace(",","");
                String serializedFeatures=splittedLine[2].replace("]]","");
                String []splittedSerializedCDFs=serializedFeatures.split(",");
                List<Double>cdfsAsList=new ArrayList<Double>();
                for(int i=0;i<splittedSerializedCDFs.length;i++){
                    cdfsAsList.add(Double.valueOf(splittedSerializedCDFs[i]));
                }
                double[]CDFs= Doubles.toArray(cdfsAsList);
                if(!this.CDFs.containsKey(username)) {
                    this.CDFs.put(username, CDFs);
                }
            }else{
                spoutOutputCollector.emit(new Values(CDFs));
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
        outputFieldsDeclarer.declare(new Fields("EXPERTS_AND_THEIR_CDFS"));

    }
}
