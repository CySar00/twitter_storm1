package Storm.Spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by christina on 31/8/2016.
 */
public class ReadFileWithOneLineSpout extends BaseRichSpout {
    private String filename;
    SpoutOutputCollector spoutOutputCollector;
    BufferedReader bufferedReader;
    AtomicLong aLong;


    public ReadFileWithOneLineSpout(String filename){
        this.filename=filename;
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
        aLong=new AtomicLong(0);

    }

    public void nextTuple() {
        try {
            String line=bufferedReader.readLine();
            if(line!=null){
                System.out.println(line);
            }else{
                Thread.sleep(60*60*1000000000);
            }
        }catch (Exception e){
            throw new RuntimeException(e);
        }

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
    public void ack(Object msgId) {

    }

    @Override
    public void fail(Object msgId) {

    }
}
