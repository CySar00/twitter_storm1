package Storm.Spouts.Clustering;

import Databases.NoSQL.CassandraDB.Clustering.CassandraSchemaForClusteredFeatures;
import Databases.NoSQL.CassandraDB.Functions.SerializeAndDeserializeJavaObjects;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by christina on 8/8/2016.
 */
public class GaussianMixtureModelingSpout extends BaseRichSpout {
    SpoutOutputCollector spoutOutputCollector;
    String[] serializedHashmapsOfClusteredFeatures;

    Map<String,double[]>clusteredFeatures0;
    Map<String,double[]>clusteredFeatures1;
    Map<Integer,Map<String,double[]>>clusteredFeatures;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector=spoutOutputCollector;
        clusteredFeatures=new HashMap<Integer, Map<String, double[]>>();
    }

    public void nextTuple() {
        serializedHashmapsOfClusteredFeatures= CassandraSchemaForClusteredFeatures.readClusteredFeaturesHashmapToCassandraDB();

        String serializedHashmapOfClusteredFeatures0=serializedHashmapsOfClusteredFeatures[0];
        String serializedHashmapOfClusteredFeatures1=serializedHashmapsOfClusteredFeatures[1];

        clusteredFeatures.put(0,SerializeAndDeserializeJavaObjects.deserializeJavaStringAndDoublesVectorHashmap(serializedHashmapOfClusteredFeatures0));
        clusteredFeatures.put(1,SerializeAndDeserializeJavaObjects.deserializeJavaStringAndDoublesVectorHashmap(serializedHashmapOfClusteredFeatures1));
        for(Integer key:clusteredFeatures.keySet()){
            spoutOutputCollector.emit(new Values(key,clusteredFeatures.get(key)));
        }

        
    }

    @Override
    public void ack(Object msgId) {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void fail(Object msgId) {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("CLUSTER_INDEX","MAP_OF_CLUSTERED_FEATURES"));

    }
}
