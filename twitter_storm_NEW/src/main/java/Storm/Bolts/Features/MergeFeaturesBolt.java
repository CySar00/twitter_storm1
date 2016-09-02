package Storm.Bolts.Features;

import Databases.NoSQL.CassandraDB.PreprocessingAuthorsAndTweetData.CassandraSchemaForAuthorsAndFeatures;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by christina on 31/7/2016.
 */
public class MergeFeaturesBolt extends BaseRichBolt {
    OutputCollector outputCollector;

    Map<String,List<Double>>FEATURES;
    Map<String,List<Double>>MERGED_FEATURES;

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("USERNAME","MERGED_FEATURES"));

    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
        MERGED_FEATURES=new HashMap<String, List<Double>>();

    }

    public void execute(Tuple tuple) {
        String username=tuple.getString(0);

        Double NUMBER_OF_TWEETS=tuple.getDouble(1);

        Double FREQUENCY=tuple.getDouble(2);

        Double MORNING_COUNT=tuple.getDouble(3);
        Double NOON_COUNT=tuple.getDouble(4);
        Double EVENING_COUNT=tuple.getDouble(5);
        Double NIGHT_COUNT=tuple.getDouble(6);

        FEATURES=new HashMap<String, List<Double>>();



        List<Double>MERGED_FEATURES=this.MERGED_FEATURES.get(username);
        if(MERGED_FEATURES==null){
            MERGED_FEATURES=new ArrayList<Double>();
        }
        MERGED_FEATURES.add(FREQUENCY);
        MERGED_FEATURES.add(MORNING_COUNT);
        MERGED_FEATURES.add(NOON_COUNT);
        MERGED_FEATURES.add(EVENING_COUNT);
        MERGED_FEATURES.add(NIGHT_COUNT);

        String serializedHashmapOfFeatures= CassandraSchemaForAuthorsAndFeatures.readFeaturesFromCassandraDB()[0];

        int firstIndex=serializedHashmapOfFeatures.indexOf("{");
        int lastIndex=serializedHashmapOfFeatures.indexOf("}");

        String subString=serializedHashmapOfFeatures.substring(firstIndex+1,lastIndex-1);
        String[]splittedSubString=subString.split("],");
        for(int i=0;i<splittedSubString.length;i++){
            String mapEntry=splittedSubString[i];
            if(mapEntry.contains(username)){
                System.out.println(username);
                String[]splittedMapEntry=splittedSubString[i].split("=\\[");
                String serializedFeatures=splittedMapEntry[1];
                String [] splittedSerializedFeatures=serializedFeatures.split(",");
                for(int j=0;j<splittedSerializedFeatures.length;j++){
                    MERGED_FEATURES.add(Double.valueOf(splittedSerializedFeatures[j]));
                }
            }
        }
        System.out.println(MERGED_FEATURES.size());

        this.MERGED_FEATURES.put(username,MERGED_FEATURES);
        outputCollector.emit(tuple,new Values(username,MERGED_FEATURES));

    }
}
