package Storm.Bolts.Features;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.Status;

import java.util.*;

/**
 * Created by christina on 31/7/2016.
 */
public class CalculateNewMetricsBolt extends BaseRichBolt {
    public static final int MILLI_SECS= 60*1000;

    private OutputCollector outputCollector;

    Map<String,List<Status>>tweets;

    Map<String,Double>NUMBER_OF_TWEETS;

    Map<String,Double>FREQUENCY;

    Map<String,Double>MORNING_COUNT;
    Map<String,Double>NOON_COUNT;
    Map<String,Double>EVENING_COUNT;
    Map<String,Double>NIGHT_COUNT;

    Map<String,List<Double>>FEATURES;

    Calendar calendar;

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("USERNAME","NUMBER_OF_TWEETS","FREQUENCY","MORNING_COUNT","NOON_COUNT","EVENING_COUNT","NIGHT_COUNT"));
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;

        tweets=new HashMap<String, List<Status>>();

        NUMBER_OF_TWEETS=new HashMap<String, Double>();
        FREQUENCY=new HashMap<String, Double>();

        MORNING_COUNT=new HashMap<String, Double>();
        NOON_COUNT=new HashMap<String, Double>();
        EVENING_COUNT=new HashMap<String, Double>();
        NIGHT_COUNT=new HashMap<String, Double>();

//        calendar=Calendar.getInstance();


    }

    public void execute(Tuple tuple) {
        String username=tuple.getString(0);
        Status tweet=(Status)tuple.getValue(1);


        List<Status>tweets=this.tweets.get(username);
        if(tweets==null){
            tweets=new ArrayList<Status>();
        }
        tweets.add(tweet);
        this.tweets.put(username,tweets);

        Double NUMBER_OF_TWEETS=this.NUMBER_OF_TWEETS.get(username);
        if(NUMBER_OF_TWEETS==null){
            NUMBER_OF_TWEETS=0.0;
        }
        NUMBER_OF_TWEETS+=1;
        this.NUMBER_OF_TWEETS.put(username,NUMBER_OF_TWEETS);

        Double FREQUENCY=this.FREQUENCY.get(username);
        if(FREQUENCY==null){
            FREQUENCY=0.0;
        }

        Double MORNING_COUNT=this.MORNING_COUNT.get(username);
        if(MORNING_COUNT==null){
            MORNING_COUNT=0.0;
        }

        Double NOON_COUNT=this.NOON_COUNT.get(username);
        if(NOON_COUNT==null){
            NOON_COUNT=0.0;
        }

        Double EVENING_COUNT=this.EVENING_COUNT.get(username);
        if(EVENING_COUNT==null){
            EVENING_COUNT=0.0;
        }

        Double NIGHT_COUNT=this.NIGHT_COUNT.get(username);
        if(NIGHT_COUNT==null){
            NIGHT_COUNT=0.0;
        }

        Date dateOfLastTweetInList=tweets.get(tweets.size()-1).getCreatedAt();
        Date dateOfFirstTweetInList=tweets.get(0).getCreatedAt();

        double duration= (double)(dateOfFirstTweetInList.getTime()-dateOfLastTweetInList.getTime())/MILLI_SECS;
        if(duration==0){
            FREQUENCY=0.0;
        }else{
            FREQUENCY=NUMBER_OF_TWEETS/duration;
        }
        this.FREQUENCY.put(username,FREQUENCY);

        Date dateOfCurrentTweet=tweet.getCreatedAt();
        calendar=Calendar.getInstance();
        calendar.setTime(dateOfCurrentTweet);
        if(calendar.get(Calendar.HOUR_OF_DAY)<=6){
            MORNING_COUNT+=1;
        }else if(calendar.get(Calendar.HOUR_OF_DAY)>6 && calendar.get(Calendar.HOUR_OF_DAY)<=12 ){
            NOON_COUNT+=1;
        }else if(calendar.get(Calendar.HOUR_OF_DAY)>12 && calendar.get(Calendar.HOUR_OF_DAY)<18){
            EVENING_COUNT+=1;
        }else{
            NIGHT_COUNT+=1;
        }
        this.MORNING_COUNT.put(username,MORNING_COUNT);
        this.NOON_COUNT.put(username,NOON_COUNT);
        this.EVENING_COUNT.put(username,EVENING_COUNT);
        this.NIGHT_COUNT.put(username,NIGHT_COUNT);

        outputCollector.emit(new Values(username,NUMBER_OF_TWEETS,FREQUENCY,MORNING_COUNT,NOON_COUNT,EVENING_COUNT,NIGHT_COUNT));







    }
}
