package Storm.Bolts.Rank;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.primitives.Floats;

import java.util.Map;

/**
 * Created by christina on 11/8/2016.
 */
public class CheckGaussianProbabilitiesBolt extends BaseRichBolt {
    OutputCollector outputCollector;
    int clusterIndex;

    public CheckGaussianProbabilitiesBolt(int clusterIndex){
        this.clusterIndex=clusterIndex;
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("CLUSTER_INDEX","USERNAME","GAUSSIAN_PROBABILITIES"));

    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
    }

    public void execute(Tuple tuple) {
        int clusterIndex=(int)tuple.getInteger(0);
        String username=tuple.getString(1);
        float[]gaussianProbabilities=(float[])tuple.getValue(2);

        if(clusterIndex==this.clusterIndex) {
            for(int i=0;i<gaussianProbabilities.length;i++) {
                if(gaussianProbabilities[i]>=0.9) {

                    outputCollector.emit(new Values(clusterIndex, username, Floats.asList(gaussianProbabilities)));
                }
            }
        }
    }
}
