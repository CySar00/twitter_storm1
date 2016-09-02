package Storm.Bolts.Rank;

import Storm.Bolts.Rank.Functions.CalculateCDFs;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.primitives.Doubles;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by christina on 29/8/2016.
 */
public class CalculateCDFsOfExpertsFeaturesBolt extends BaseRichBolt {
    OutputCollector outputCollector;
    Map<String,double[]>CDFs;
    Map<String,List<Double>>CDFsAsList;


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("USERNAME","CDFS"));

    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
        CDFs=new HashMap<String, double[]>();
        CDFsAsList=new HashMap<String, List<Double>>();

    }

    public void execute(Tuple tuple) {
        String username=tuple.getString(0);
        double[]features=(double[])tuple.getValue(1);
        double[] mu=(double[])tuple.getValue(2);
        double[] covariance=(double[])tuple.getValue(3);

        double[]CDFs=this.CDFs.get(username);
        if(CDFs==null){
            CDFs=new double[features.length];
        }

        double []z=new double[features.length];
        for(int i=0;i<features.length;i++){
            if(covariance[i]!=0){
                z[i]=(features[i]-mu[i])/covariance[i];
            }else if(covariance[i]==0){
                z[i]=features[i];
            }
            CDFs[i]=CalculateCDFs.Phi(z[i]);
        }
        this.CDFs.put(username,CDFs);

        List<Double>CDFsAsList=this.CDFsAsList.get(username);
        if(CDFsAsList==null){
            CDFsAsList=new ArrayList<Double>();
        }
        CDFsAsList= Doubles.asList(CDFs);
        this.CDFsAsList.put(username,CDFsAsList);

        outputCollector.emit(new Values(username,CDFsAsList));



    }
}
