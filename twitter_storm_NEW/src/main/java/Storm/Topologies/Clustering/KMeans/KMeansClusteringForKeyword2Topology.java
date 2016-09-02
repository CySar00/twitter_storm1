package Storm.Topologies.Clustering.KMeans;

import Storm.Bolts.Clustering.KMeans.ClassifyBasedOnClusterIndex.ClassifyBasedOnClusterIndexBolt;
import Storm.Bolts.Clustering.KMeans.EmitFeaturesIntoClosestKClustersBolt;
import Storm.Bolts.Clustering.KMeans.KClusteringBolt;
import Storm.Bolts.FileWriterBolt;
import Storm.Spouts.Clustering.SelectKCentroidsSpout;
import Storm.Spouts.ProcessMergedFeaturesOfAKeywordSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 * Created by christina on 2/8/2016.
 */
public class KMeansClusteringForKeyword2Topology {
    public static final String PATH_TO_FEATURES_OF_KEYWORD2_TEXT_FILE="/home/christina/twitter_storm_NEW/Merged_Features_Of_Keywords/merged_features_of_keyword2.txt";

    public static final String PATH_TO_FEATURES_OF_KEYWORD2_THAT_HAVE_BEEN_CLUSTERED_INTO_CLUSTER_WITH_INDEX_EQUALS_0="/home/christina/twitter_storm_NEW/K_Clustered_Features/features_of_keyword2_that_belong_to_cluster_with_index_0.txt";
    public static final String PATH_TO_FEATURES_OF_KEYWORD2_THAT_HAVE_BEEN_CLUSTERED_INTO_CLUSTER_WITH_INDEX_EQUALS_1="/home/christina/twitter_storm_NEW/K_Clustered_Features/features_of_keyword2_that_belong_to_cluster_with_index_1.txt";


    public static final int CLUSTERS=2;

    public static void main(String [] args) throws Exception{
        TopologyBuilder topologyBuilder=new TopologyBuilder();
        topologyBuilder.setSpout("SELECT_CENTROIDS_FOR_K_MEANS_CLUSTERING",new SelectKCentroidsSpout(PATH_TO_FEATURES_OF_KEYWORD2_TEXT_FILE,CLUSTERS));

        topologyBuilder.setSpout("READ_MERGED_FEATURES_OF_KEYWORD2_FROM_TEXT_FILE",new ProcessMergedFeaturesOfAKeywordSpout(PATH_TO_FEATURES_OF_KEYWORD2_TEXT_FILE));
        topologyBuilder.setBolt("EMIT_FEATURES_INTO_CLOSEST_K_CENTROID",new EmitFeaturesIntoClosestKClustersBolt()).fieldsGrouping("READ_MERGED_FEATURES_OF_KEYWORD2_FROM_TEXT_FILE",new Fields("USERNAME"));
        topologyBuilder.setBolt("K-CLUSTERING_FEATURES_OF_KEYWORD2",new KClusteringBolt()).fieldsGrouping("EMIT_FEATURES_INTO_CLOSEST_K_CENTROID",new Fields("CLUSTER_INDEX"));

        topologyBuilder.setBolt("FIND_FEATURES_OF_KEYWORD2_THAT_BELONG_TO_THE_CLUSTER_WITH_INDEX_0",new ClassifyBasedOnClusterIndexBolt(0)).fieldsGrouping("K-CLUSTERING_FEATURES_OF_KEYWORD2",new Fields("CLUSTER_INDEX"));
        topologyBuilder.setBolt("WRITE_FEATURES_OF_CLUSTER_WITH_INDEX_0_TO_TEXT_FILE",new FileWriterBolt(PATH_TO_FEATURES_OF_KEYWORD2_THAT_HAVE_BEEN_CLUSTERED_INTO_CLUSTER_WITH_INDEX_EQUALS_0)).shuffleGrouping("FIND_FEATURES_OF_KEYWORD2_THAT_BELONG_TO_THE_CLUSTER_WITH_INDEX_0");

        topologyBuilder.setBolt("FIND_FEATURES_OF_KEYWORD2_THAT_BELONG_TO_THE_CLUSTER_WITH_INDEX_1",new ClassifyBasedOnClusterIndexBolt(1)).fieldsGrouping("K-CLUSTERING_FEATURES_OF_KEYWORD2",new Fields("CLUSTER_INDEX"));
        topologyBuilder.setBolt("WRITE_FEATURES_OF_CLUSTER_WITH_INDEX_1_TO_TEXT_FILE",new FileWriterBolt(PATH_TO_FEATURES_OF_KEYWORD2_THAT_HAVE_BEEN_CLUSTERED_INTO_CLUSTER_WITH_INDEX_EQUALS_1)).shuffleGrouping("FIND_FEATURES_OF_KEYWORD2_THAT_BELONG_TO_THE_CLUSTER_WITH_INDEX_1");


        Config config=new Config();
        if(args!=null && args.length>0){
            config.setNumWorkers(5);
            config.setNumAckers(10);
            config.setMaxSpoutPending(100);
            StormSubmitter.submitTopology("K-Clustering_Features_Of_Keyword2",config,topologyBuilder.createTopology());
        }else{
            LocalCluster localCluster=new LocalCluster();
            localCluster.submitTopology("K-Clustering_Features_Of_Keyword2",config,topologyBuilder.createTopology());
            Utils.sleep(2*100*1000);
            localCluster.killTopology("K-Clustering_Features_Of_Keyword2");
            localCluster.shutdown();


        }


    }
}
