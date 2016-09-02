package Storm.Spouts.Preprocessing.FromCSVFiles;

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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by christina on 30/7/2016.
 */
public class ProcessTweetsOfKeywordsSpout extends BaseRichSpout {
    private String filename;
    private String keyword;

    private SpoutOutputCollector spoutOutputCollector;
    private BufferedReader bufferedReader;
    private AtomicLong linesRead=new AtomicLong(0);

    private Map<String,String>map;

    public ProcessTweetsOfKeywordsSpout(String filename,String keyword){
        this.filename=filename;
        this.keyword=keyword;
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
        this.map=new HashMap<String, String>();

    }

    public void nextTuple() {
        try {
            String line=bufferedReader.readLine();
            long ID=linesRead.incrementAndGet();
            if(line!=null){
                if(!line.startsWith("tweet")){
                    String [] splittedLine=line.split(",");
                    if(splittedLine[splittedLine.length-1].equals(keyword)){
                        StringBuilder stringBuilder=new StringBuilder();
                        stringBuilder.append(splittedLine[0]);
                        for(int i=1;i<splittedLine.length-2;i++){
                            stringBuilder.append(",").append(splittedLine[i]);
                        }

                       if(!map.containsKey(splittedLine[splittedLine.length-2])){
                           spoutOutputCollector.emit(new Values(splittedLine[splittedLine.length-2],stringBuilder.toString()));
                           map.put(splittedLine[splittedLine.length-2],stringBuilder.toString());
                       }



                    }

                }
            }else{
                Thread.sleep(60*60*60*1000);
            }
        }catch (Exception e){

        }


    }

    @Override
    public void ack(Object msgId) {

    }

    @Override
    public void fail(Object msgId) {
        System.out.println("failed line number: "+msgId);

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
        outputFieldsDeclarer.declare(new Fields("USERNAME","TWEET"));

    }
}
