package Storm.Spouts.Clustering;

import Databases.NoSQL.CassandraDB.Clustering.CassandraSchemaForKCentroids;
import Storm.Spouts.Clustering.Functions.SelectRandomLines;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by christina on 3/8/2016.
 */
public class SelectKCentroidsSpout extends BaseRichSpout {
    private String filename;
    private int numberOfKClusters;

    SpoutOutputCollector spoutOutputCollector;
    BufferedReader bufferedReader;
    AtomicLong linesRead;

    Random random;
    List<String>lines;
    List<String> kCentroids;

    public SelectKCentroidsSpout(String filename, int numberOfKClusters){
        this.filename=filename;
        this.numberOfKClusters=numberOfKClusters;
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
        random=new Random();
        lines=new ArrayList<String>();
        kCentroids=new ArrayList<String>();
    }

    public void nextTuple() {
        try {
            String line=bufferedReader.readLine();
            if(line!=null){
                long ID=linesRead.incrementAndGet();
                String[] splittedLine=line.split("\\[");
                String serializedFeatures=splittedLine[2];
                String serializedFeatures1=serializedFeatures.substring(0,serializedFeatures.length()-2);
                String[] splittedSerializedFeatures=serializedFeatures1.split(",");

                if(splittedSerializedFeatures.length==12) {
                    if (!lines.contains(serializedFeatures1)) {
                        lines.add(serializedFeatures1);
                    }
                }

                StringBuilder stringBuilder=new StringBuilder();
                stringBuilder.append("[").append(serializedFeatures1);
                String serializedFeatures2=stringBuilder.toString();



            }else {
                kCentroids= SelectRandomLines.selectNRandomLinesFromArrayList(lines,numberOfKClusters);
                int i=0;
                Iterator<String>iterator=kCentroids.iterator();
                while (iterator.hasNext()){
                    String serializedCentroids=iterator.next();
                    CassandraSchemaForKCentroids.setCentroidsForKMeansClustering(i,serializedCentroids);
                    i+=1;
                }
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
        System.out.println("Failed line number: "+msgId);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }


}
