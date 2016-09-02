package Storm.Spouts.Preprocessing;

import Databases.NoSQL.CassandraDB.Functions.SerializeAndDeserializeJavaObjects;
import Databases.NoSQL.CassandraDB.PreprocessingAuthorsAndTweetData.CassandraSchemaForAuthorsAndTwittterInformation;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import twitter4j.Status;
import twitter4j.json.DataObjectFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by christina on 1/8/2016.
 */
public class ProcessTwitterInfoFromTextAndCSVFilesSpout extends BaseRichSpout {
    private String filename1;
    private String filename2;
    private String filename3;
    private String filename4;

    private SpoutOutputCollector spoutOutputCollector;

    private BufferedReader bufferedReader1;
    private BufferedReader bufferedReader2;
    private BufferedReader bufferedReader3;
    private BufferedReader bufferedReader4;

    private AtomicLong linesRead1;
    private AtomicLong linesRead2;
    private AtomicLong linesRead3;
    private AtomicLong linesRead4;

    Map<String, Status>tweets;
    Map<String,String>serializedTweets;
    Map<String,Long>IDs;
    Map<String,List<Long>>followers;
    Map<String,List<Long>>friends;

    public ProcessTwitterInfoFromTextAndCSVFilesSpout(String filename1,String filename2,String filename3,String filename4){
        this.filename1=filename1;
        this.filename2=filename2;
        this.filename3=filename3;
        this.filename4=filename4;
    }


    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector=spoutOutputCollector;
        try{
            bufferedReader1=new BufferedReader(new FileReader(filename1));
            bufferedReader2=new BufferedReader(new FileReader(filename2));
            bufferedReader3=new BufferedReader(new FileReader(filename3));
            bufferedReader4=new BufferedReader(new FileReader(filename4));
        }catch (IOException e){
            throw new RuntimeException(e);
        }catch (Exception e){
            throw new RuntimeException(e);
        }
        linesRead1=new AtomicLong(0);
        linesRead2=new AtomicLong(0);
        linesRead3=new AtomicLong(0);
        linesRead4=new AtomicLong(0);

        tweets=new HashMap<String, Status>();
        serializedTweets=new HashMap<String, String>();
        IDs=new HashMap<String, Long>();
        followers=new HashMap<String, List<Long>>();
        friends=new HashMap<String, List<Long>>();


    }

    public void nextTuple() {
        try {
            String line1=bufferedReader1.readLine();
            String line2=bufferedReader2.readLine();
            String line3=bufferedReader3.readLine();
            String line4=bufferedReader4.readLine();

            if(line1!=null && line2!=null && line3!=null && line4!=null){
                long ID1=linesRead1.incrementAndGet();
                long ID2=linesRead2.incrementAndGet();
                long ID3=linesRead3.incrementAndGet();
                long ID4=linesRead4.incrementAndGet();


                int firstIndexOfLine1=line1.indexOf("{},");
                int lastIndexOfLine1=line1.indexOf("}}]");
                String subStringOfLine1=line1.substring(firstIndexOfLine1+5,lastIndexOfLine1+2);

                String[] splittedSubStringOfLine1=subStringOfLine1.split(",");
                String username1=splittedSubStringOfLine1[0];

                StringBuilder stringBuilder=new StringBuilder();
                stringBuilder.append(splittedSubStringOfLine1[1]);
                for(int i=2;i<splittedSubStringOfLine1.length;i++){
                    stringBuilder.append(",").append(splittedSubStringOfLine1[i]);
                }
                String serializedTweet=stringBuilder.toString();
                Status tweet= DataObjectFactory.createStatus(serializedTweet);

                if(!this.serializedTweets.containsKey(username1)){
                    this.serializedTweets.put(username1,serializedTweet);
                }
                if(!tweets.containsKey(username1)){
                    tweets.put(username1,tweet);
                }



            }else{
                CassandraSchemaForAuthorsAndTwittterInformation.writeAuthorsAndTwitterInformationToCassandraDB(0, SerializeAndDeserializeJavaObjects.serializeJavaStringAndSerializedStatusHashmap(serializedTweets));

                System.out.println(CassandraSchemaForAuthorsAndTwittterInformation.readAuthorsAndTwitterInformationFromCassandraDB()[0]);
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
            bufferedReader1.close();;
            bufferedReader2.close();;
            bufferedReader3.close();
            bufferedReader4.close();
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
        outputFieldsDeclarer.declare(new Fields("TWEETS","IDS","FOLLOWERS","FRIENDS"));
    }
}
