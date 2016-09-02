package Storm.Spouts.Rank.Borda;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by christina on 1/9/2016.
 */
public class JoinBordaRanksFromDifferentFuzzyClustersSpout extends BaseRichSpout {
    private String filename1;
    private String filename2;

    SpoutOutputCollector spoutOutputCollector;
    BufferedReader bufferedReader1;
    BufferedReader bufferedReader2;
    AtomicLong linesRead1;
    AtomicLong linesRead2;

    Map<String,Double>map1;
    Map<String,Double>map2;
    Map<String,Double>map;

    public JoinBordaRanksFromDifferentFuzzyClustersSpout(String filename1,String filename2){
        this.filename1=filename1;
        this.filename2=filename2;
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("MAP"));

    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector=spoutOutputCollector;
        try {
            bufferedReader1=new BufferedReader(new FileReader(filename1));
            bufferedReader2=new BufferedReader(new FileReader(filename2));
        }catch (Exception e){
            throw new RuntimeException(e);
        }
        linesRead1=new AtomicLong(0);
        linesRead2=new AtomicLong(1);
        map1=new HashMap<String, Double>();
        map2=new HashMap<String, Double>();
        this.map=new HashMap<String, Double>();
    }

    public void nextTuple() {
        try {
            String line1=bufferedReader1.readLine();
            String line2=bufferedReader2.readLine();

            if(line2!=null){
                int firstIndex=line2.indexOf("[");
                int lastIndex=line2.indexOf("]");
                String subLine=line2.substring(firstIndex+1,lastIndex);
                String[] splittedSubLine=subLine.split(",");
                String username=splittedSubLine[1];
                Double score=Double.valueOf(splittedSubLine[2]);
                map2.put(username,score);
            }

            if(line1!=null){
                System.out.println(line1);
                int firstIndex=line1.indexOf("[");
                int lastIndex=line1.indexOf("]");
                String subLine=line1.substring(firstIndex+1,lastIndex);
                String[] splittedSubLine=subLine.split(",");
                String username=splittedSubLine[1];
                Double score=Double.valueOf(splittedSubLine[2]);
                map1.put(username,score);
            }else{
                for(String username1:map1.keySet()) {
                    double score1=map1.get(username1);
                    double score;
                    for(String username2:map2.keySet()){
                        double score2=map2.get(username2);
                        if(username1.equals(username2)){
                            score=score1+score2;
                            map.put(username1,score);
                        }else{
                            map.put(username1,score1);
                            map.put(username2,score2);
                        }

                    }
                }
                System.out.println(map);
                spoutOutputCollector.emit(new Values(map));

                Thread.sleep(60*60*10000);
            }



        }catch (Exception e){

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
        try {
            bufferedReader1.close();
            bufferedReader2.close();
        }catch (Exception e){
            throw new RuntimeException(e);
        }

    }
}
