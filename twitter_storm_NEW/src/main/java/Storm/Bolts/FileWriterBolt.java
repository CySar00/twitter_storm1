package Storm.Bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * Created by christina on 22/7/2016.
 */
public class FileWriterBolt extends BaseRichBolt {
    PrintWriter printWriter;
    int counter=0;

    private OutputCollector outputCollector;
    private String filename;

    public FileWriterBolt(String filename){
        this.filename=filename;
    }



    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
        try {
            printWriter=new PrintWriter(filename,"UTF-8");
        }catch (FileNotFoundException e){
            e.printStackTrace();
        }catch (UnsupportedEncodingException e){
            e.printStackTrace();
        }

    }

    public void execute(Tuple tuple) {
        printWriter.println((counter++)+" : "+tuple);
        printWriter.flush();
        outputCollector.ack(tuple);
    }

    @Override
    public void cleanup() {
        printWriter.close();
        super.cleanup();
    }
}
