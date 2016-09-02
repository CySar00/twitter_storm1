package Storm.Spouts.Clustering;

import Databases.NoSQL.CassandraDB.Clustering.CassandraSchemaForFuzzyCCentroids;
import Storm.Spouts.Clustering.Functions.SelectRandomLines;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by christina on 6/8/2016.
 */
public class SelectFuzzyCCentroidsSpout extends BaseRichSpout{
    private String filename;
    private int numberOfClusters;

    private SpoutOutputCollector spoutOutputCollector;
    private BufferedReader bufferedReader;
    private AtomicLong linesRead;

    List<String> lines;
    List<String> fuzzyCentroids;

    public SelectFuzzyCCentroidsSpout(String filename,int numberOfClusters){
        this.filename=filename;
        this.numberOfClusters=numberOfClusters;
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
        lines=new ArrayList<String>();
        fuzzyCentroids=new ArrayList<String>();

    }

    public void nextTuple() {
        try {
            String line=bufferedReader.readLine();
            if(line!=null){
                long ID=linesRead.incrementAndGet();
                String[] splittedLine=line.split("\\[");
                String serializedFeatures=splittedLine[2];
                String serializedFeatures1=serializedFeatures.substring(0,serializedFeatures.length()-2);
                String[] splittesSerializedFeatures=serializedFeatures1.split(",");

                if(splittesSerializedFeatures.length==12){
                    if(!lines.contains(serializedFeatures1)){
                        lines.add(serializedFeatures1);
                    }
                }

            }else {
                fuzzyCentroids= SelectRandomLines.selectNRandomLinesFromArrayList(lines,numberOfClusters);
                int i=0;
                Iterator<String>iterator=fuzzyCentroids.iterator();
                while (iterator.hasNext()){
                    String serializedFuzzyCentroid=iterator.next();
                    CassandraSchemaForFuzzyCCentroids.setCentroidsForFuzzyMeansClustering(i,serializedFuzzyCentroid);
                    i+=1;
                }
                Thread.sleep(60*60*60*100000000);
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

    }
}
