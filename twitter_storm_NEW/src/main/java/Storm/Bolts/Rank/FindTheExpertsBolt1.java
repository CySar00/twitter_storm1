package Storm.Bolts.Rank;

import Databases.NoSQL.CassandraDB.CassandraSchemaOfAuthors;
import Databases.NoSQL.CassandraDB.PreprocessingAuthorsAndTweetData.CassandraSchemaForAuthorsAndFeatures;
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
 * Created by christina on 29/8/2016.
 */
public class FindTheExpertsBolt1 extends BaseRichBolt {
    OutputCollector outputCollector;

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("USERNAME","FEATURES"));
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;

    }

    public void execute(Tuple tuple) {
        String username=tuple.getString(0);
        List<Double> features=(List<Double>)tuple.getValue(1);

        String username1=username.replace(",","");
        String experts= CassandraSchemaOfAuthors.readTheAuthorsFromCassandraDatabase();
        if(experts.contains(username1)){
            outputCollector.emit(tuple,new Values(username1,features));
            System.out.println(username1+" "+features);


        }
    }
}
