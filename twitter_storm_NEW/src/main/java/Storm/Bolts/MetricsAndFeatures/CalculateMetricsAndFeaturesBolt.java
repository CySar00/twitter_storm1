package Storm.Bolts.MetricsAndFeatures;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import twitter4j.Status;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by christina on 1/8/2016.
 */
public class CalculateMetricsAndFeaturesBolt extends BaseRichBolt {
    public static final double LAMBDA=0.05;
    public static final int MILLI_SECS=60*1000;

    private String sourceComponent1;
    private String sourceComponent2;

    OutputCollector outputCollector;

    Map<String, Status>tweets;
    Map<String,Long>IDs;
    Map<String,List<Long>>followers;
    Map<String,List<Long>>friends;

    Map<String,List<Status>>TWEETS;
    Map<String,Double>NUMBER_OF_TWEETS;

    Map<String,Double>OT1;
    Map<String,Double>OT2;
    Map<String,Double>OT3;
    Map<String,Double>OT4;

    Map<String,Double>CT1;
    Map<String,Double>CT2;

    Map<String,Double>RT1;
    Map<String,Double>RT2;
    Map<String,Double>RT3;

    Map<String,Double>M1;
    Map<String,Double>M2;
    Map<String,Double>M3;
    Map<String,Double>M4;

    Map<String,Double>G1;
    Map<String,Double>G2;
    Map<String,Double>G3;
    Map<String,Double>G4;

    Map<String,Double>FREQUENCY;
    Map<String,Double>MORNING_COUNT;
    Map<String,Double>NOON_COUNT;
    Map<String,Double>EVENING_COUNT;
    Map<String,Double>NIGHT_COUNT;

    Map<String,Double>TS;
    Map<String,Double>SS;
    Map<String,Double>nonCS;
    Map<String,Double>RI;
    Map<String,Double>MI;
    Map<String,Double>ID;
    Map<String,Double>ID1;
    Map<String,Double>NS;


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;


        TWEETS=new HashMap<String, List<Status>>();
        NUMBER_OF_TWEETS=new HashMap<String, Double>();

        OT1=new HashMap<String, Double>();
        OT2=new HashMap<String, Double>();
        OT3=new HashMap<String, Double>();
        OT4=new HashMap<String, Double>();

        CT1=new HashMap<String, Double>();
        CT2=new HashMap<String, Double>();

        RT1=new HashMap<String, Double>();
        RT2=new HashMap<String, Double>();
        RT3=new HashMap<String, Double>();

        M1=new HashMap<String, Double>();
        M2=new HashMap<String, Double>();
        M3=new HashMap<String, Double>();
        M4=new HashMap<String, Double>();

        G1=new HashMap<String, Double>();
        G2=new HashMap<String, Double>();
        G3=new HashMap<String, Double>();
        G4=new HashMap<String, Double>();

        FREQUENCY=new HashMap<String, Double>();
        MORNING_COUNT=new HashMap<String, Double>();
        NOON_COUNT=new HashMap<String, Double>();
        EVENING_COUNT=new HashMap<String, Double>();
        NIGHT_COUNT=new HashMap<String, Double>();

        TS=new HashMap<String, Double>();
        SS=new HashMap<String, Double>();
        nonCS=new HashMap<String, Double>();
        RI=new HashMap<String, Double>();
        MI=new HashMap<String, Double>();
        ID=new HashMap<String, Double>();
        ID1=new HashMap<String, Double>();
        NS=new HashMap<String, Double>();

    }

    public void execute(Tuple tuple) {
        String username=tuple.getString(0);
        Status tweet=(Status)tuple.getValue(1);


        List<Status>TWEETS=this.TWEETS.get(username);
        if(TWEETS==null){
            TWEETS=new ArrayList<Status>();
        }
        TWEETS.add(tweet);
        this.TWEETS.put(username,TWEETS);

        Double NUMBER_OF_TWEETS=this.NUMBER_OF_TWEETS.get(username);
        if(NUMBER_OF_TWEETS==null){
            NUMBER_OF_TWEETS=0.0;
        }
        NUMBER_OF_TWEETS+=1;
        this.NUMBER_OF_TWEETS.put(username,NUMBER_OF_TWEETS);

        Double OT1=this.OT1.get(username);
        if(OT1==null){
            OT1=0.0;
        }

        Double OT2=this.OT2.get(username);
        if(OT2==null){
            OT2=0.0;
        }

        Double OT3=this.OT3.get(username);
        if(OT3==null){
            OT3=0.0;
        }

        Double OT4=this.OT4.get(username);
        if(OT4==null){
            OT4=0.0;
        }


        if(!tweet.getText().startsWith("@") && !tweet.getText().startsWith("RT") && !tweet.getText().contains("RT")){
            OT1+=1;
        }

        OT2+=tweet.getURLEntities().length;

        OT4+=tweet.getHashtagEntities().length;



        this.OT1.put(username,OT1);
        this.OT2.put(username,OT2);
        this.OT4.put(username,OT4);









    }
}
