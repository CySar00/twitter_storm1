package Storm.Bolts.Rank;

import Databases.NoSQL.CassandraDB.CassandraSchemaOfAuthors;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.List;
import java.util.Map;

/**
 * Created by christina on 13/8/2016.
 */
public class FindTheExpertsBolt extends BaseRichBolt {
    OutputCollector outputCollector;

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("USERNAME","FEATURES"));

    }


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;

    }

    public void execute(Tuple tuple) {

        String username = tuple.getString(0);
        List<Double> features = (List<Double>) tuple.getValue(1);

        String username1 = username.replace(",", "");
        System.out.println(username1 + " " + features);
        String serializedExperts = CassandraSchemaOfAuthors.readTheAuthorsFromCassandraDatabase();
        String serializedExperts1 = serializedExperts.replace("[", "").replace("]", "");
        // System.out.println(serializedExperts);
        String[] splittedSerializedExperts = serializedExperts1.split(",");
        for (int i = 0; i < splittedSerializedExperts.length; i++) {
            String expert = splittedSerializedExperts[i].replace(" ", "");
            if (expert.contains(username1)) {
                System.out.println(username1);
                outputCollector.emit(new Values(username1, features));
            }
        }



    }
}
