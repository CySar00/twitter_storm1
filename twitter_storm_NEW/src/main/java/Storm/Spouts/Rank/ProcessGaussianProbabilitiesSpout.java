package Storm.Spouts.Rank;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by christina on 12/8/2016.
 */
public class ProcessGaussianProbabilitiesSpout extends BaseRichSpout {
    private String filename1;
    private String filename2;

    SpoutOutputCollector spoutOutputCollector;
    BufferedReader bufferedReader1;
    BufferedReader bufferedReader2;
    AtomicLong linesRead1;
    AtomicLong linesRead2;

    Map<String,List<Float>>map1;
    Map<String,List<Float>>map2;

    public ProcessGaussianProbabilitiesSpout(String filename1,String filename2){
        this.filename1=filename1;
        this.filename2=filename2;
    }


    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector=spoutOutputCollector;
        try {
            bufferedReader1=new BufferedReader(new FileReader(filename1));
            bufferedReader2=new BufferedReader(new FileReader(filename2));
        }catch (IOException e){
            e.printStackTrace();
        }catch (Exception e){
            e.printStackTrace();
        }
        linesRead1=new AtomicLong(0);
        linesRead2=new AtomicLong(0);
        map1=new HashMap<String, List<Float>>();
        map2=new HashMap<String, List<Float>>();
    }

    public void nextTuple() {
        try {
            String line1=bufferedReader1.readLine();
            String line2=bufferedReader2.readLine();
            if(line1!=null){
                long ID=linesRead1.incrementAndGet();
                System.out.println(line1);
            }else if(line2!=null){
                long ID=linesRead2.incrementAndGet();
            }else{
                Thread.sleep(60*60*1000);
            }
        }catch (Exception e){

        }

    }

    @Override
    public void ack(Object msgId) {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void fail(Object msgId) {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
