package Storm.Spouts.Preprocessing.OfAKeyword;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import twitter4j.Status;
import twitter4j.json.DataObjectFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by christina on 30/7/2016.
 */
public class ProcessTweetsOfAKeywordSpout extends BaseRichSpout {
    private String filename;

    private SpoutOutputCollector spoutOutputCollector;
    private BufferedReader bufferedReader;
    private AtomicLong linesRead;

    public ProcessTweetsOfAKeywordSpout(String filename){
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

                int firstIndex=line.indexOf("{},");
                int lastIndex=line.indexOf("}}]");
                String subString=line.substring(firstIndex+5,lastIndex+2);

                String[] splittedSubString=subString.split(",");
                String username=splittedSubString[0];

                StringBuilder stringBuilder=new StringBuilder();
                stringBuilder.append(splittedSubString[1]);
                for(int i=2;i<splittedSubString.length;i++){
                    stringBuilder.append(",").append(splittedSubString[i]);
                }
                String serializedTweet=stringBuilder.toString();
                Status tweet=DataObjectFactory.createStatus(serializedTweet);

                spoutOutputCollector.emit(new Values(username,tweet));



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
    public void deactivate() {
        try {
            bufferedReader.close();
        }catch (IOException e){
            throw new RuntimeException(e);
        }catch (Exception e){
            throw new RuntimeException(e);
        }

    }

    @Override
    public void fail(Object msgId) {
        System.out.println("failed line number: "+msgId);

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("USERNAME","TWEET"));
    }
}
