package Storm.Topologies.Clustering.FuzzyC;

import Storm.Bolts.Clustering.FuzzyCMeans.CalculateMembershipForFuzzyClusteringBolt;
import Storm.Bolts.Clustering.FuzzyCMeans.ClassifyFeaturesBasedOnFuzzyClusterIndex.ClassifyFeaturesBasedOnFuzzyClusterIndexBolt;
import Storm.Bolts.Clustering.FuzzyCMeans.EmitFeaturesIntoClosestFuzzyClusterBolt;
import Storm.Bolts.Clustering.FuzzyCMeans.FuzzyClusteringBolt;
import Storm.Bolts.Clustering.KMeans.ClassifyBasedOnClusterIndex.ClassifyBasedOnClusterIndexBolt;
import Storm.Bolts.Clustering.KMeans.EmitFeaturesIntoClosestKClustersBolt;
import Storm.Bolts.Clustering.KMeans.KClusteringBolt;
import Storm.Bolts.FileWriterBolt;
import Storm.Spouts.Clustering.SelectFuzzyCCentroidsSpout;
import Storm.Spouts.Clustering.SelectKCentroidsSpout;
import Storm.Spouts.ProcessMergedFeaturesOfAKeywordSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 * Created by christina on 7/8/2016.
 */
public class FuzzyClusteringForKeyword1Topology {
    public static final String PATH_TO_FEATURES_OF_KEYWORD1_TEXT_FILE = "/home/christina/twitter_storm_NEW/Merged_Features_Of_Keywords/merged_features_of_keyword1.txt";

    public static final String PATH_TO_FEATURES_OF_KEYWORD1_THAT_HAVE_BEEN_CLUSTERED_INTO_FUZZY_CLUSTER_WITH_INDEX_EQUALS_0 = "/home/christina/twitter_storm_NEW/Fuzzy_Clustered_Features/features_of_keyword1_that_belong_to_fuzzy_cluster_with_index_0.txt";
    public static final String PATH_TO_FEATURES_OF_KEYWORD1_THAT_HAVE_BEEN_CLUSTERED_INTO_FUZZY_CLUSTER_WITH_INDEX_EQUALS_1 = "/home/christina/twitter_storm_NEW/Fuzzy_Clustered_Features/features_of_keyword1_that_belong_to_fuzzy_cluster_with_index_1.txt";
    public static final String PATH_TO_FEATURES_OF_KEYWORD1_THAT_HAVE_BEEN_CLUSTERED_INTO_FUZZY_CLUSTER_WITH_INDEX_EQUALS_2 = "/home/christina/twitter_storm_NEW/Fuzzy_Clustered_Features/features_of_keyword1_that_belong_to_fuzzy_cluster_with_index_2.txt";


    public static final int CLUSTERS = 3;
    public static final double FUZZY=1.7;

    public static void main(String[] args) throws Exception {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("SELECT_CENTROIDS_FOR_FUZZY_MEANS_CLUSTERING", new SelectFuzzyCCentroidsSpout(PATH_TO_FEATURES_OF_KEYWORD1_TEXT_FILE, CLUSTERS));

        topologyBuilder.setSpout("READ_MERGED_FEATURES_OF_KEYWORD1_FROM_TEXT_FILE", new ProcessMergedFeaturesOfAKeywordSpout(PATH_TO_FEATURES_OF_KEYWORD1_TEXT_FILE));
        topologyBuilder.setBolt("CALCULATE_MEMBERSHIPS_FOR_FUZZY_CLUSTERING",new CalculateMembershipForFuzzyClusteringBolt(FUZZY)).fieldsGrouping("READ_MERGED_FEATURES_OF_KEYWORD1_FROM_TEXT_FILE",new Fields("USERNAME"));
        topologyBuilder.setBolt("EMIT_FEATURES_INTO_CLOSEST_FUZZY_CLUSTER",new EmitFeaturesIntoClosestFuzzyClusterBolt(FUZZY)).fieldsGrouping("CALCULATE_MEMBERSHIPS_FOR_FUZZY_CLUSTERING",new Fields("USERNAME"));

        topologyBuilder.setBolt("FUZZY_CLUSTERING_FEATURES_OF_KEYWORD1",new FuzzyClusteringBolt(FUZZY)).fieldsGrouping("EMIT_FEATURES_INTO_CLOSEST_FUZZY_CLUSTER",new Fields("CLUSTER_INDEX"));

        topologyBuilder.setBolt("FIND_FEATURES_OF_KEYWORD1_THAT_BELONG_TO_THE_FUZZY_CLUSTER_WITH_INDEX_0",new ClassifyFeaturesBasedOnFuzzyClusterIndexBolt(0)).fieldsGrouping("FUZZY_CLUSTERING_FEATURES_OF_KEYWORD1",new Fields("CLUSTER_INDEX"));
        topologyBuilder.setBolt("WRITE_FEATURES_OF_FUZZY_CLUSTER_WITH_INDEX_0_TO_TEXT_FILE",new FileWriterBolt(PATH_TO_FEATURES_OF_KEYWORD1_THAT_HAVE_BEEN_CLUSTERED_INTO_FUZZY_CLUSTER_WITH_INDEX_EQUALS_0)).shuffleGrouping("FIND_FEATURES_OF_KEYWORD1_THAT_BELONG_TO_THE_FUZZY_CLUSTER_WITH_INDEX_0");


        topologyBuilder.setBolt("FIND_FEATURES_OF_KEYWORD1_THAT_BELONG_TO_THE_FUZZY_CLUSTER_WITH_INDEX_1",new ClassifyFeaturesBasedOnFuzzyClusterIndexBolt(1)).fieldsGrouping("FUZZY_CLUSTERING_FEATURES_OF_KEYWORD1",new Fields("CLUSTER_INDEX"));
        topologyBuilder.setBolt("WRITE_FEATURES_OF_FUZZY_CLUSTER_WITH_INDEX_1_TO_TEXT_FILE",new FileWriterBolt(PATH_TO_FEATURES_OF_KEYWORD1_THAT_HAVE_BEEN_CLUSTERED_INTO_FUZZY_CLUSTER_WITH_INDEX_EQUALS_1)).shuffleGrouping("FIND_FEATURES_OF_KEYWORD1_THAT_BELONG_TO_THE_FUZZY_CLUSTER_WITH_INDEX_1");

        topologyBuilder.setBolt("FIND_FEATURES_OF_KEYWORD1_THAT_BELONG_TO_THE_FUZZY_CLUSTER_WITH_INDEX_2",new ClassifyFeaturesBasedOnFuzzyClusterIndexBolt(2)).fieldsGrouping("FUZZY_CLUSTERING_FEATURES_OF_KEYWORD1",new Fields("CLUSTER_INDEX"));
        topologyBuilder.setBolt("WRITE_FEATURES_OF_FUZZY_CLUSTER_WITH_INDEX_2_TO_TEXT_FILE",new FileWriterBolt(PATH_TO_FEATURES_OF_KEYWORD1_THAT_HAVE_BEEN_CLUSTERED_INTO_FUZZY_CLUSTER_WITH_INDEX_EQUALS_2)).shuffleGrouping("FIND_FEATURES_OF_KEYWORD1_THAT_BELONG_TO_THE_FUZZY_CLUSTER_WITH_INDEX_2");



        Config config = new Config();
        if (args != null && args.length > 0) {
            config.setNumWorkers(5);
            config.setNumAckers(10);
            config.setMaxSpoutPending(100);
            StormSubmitter.submitTopology("Fuzzy_Clustering_Features_Of_Keyword1", config, topologyBuilder.createTopology());
        } else {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("Fuzzy_Clustering_Features_Of_Keyword1", config, topologyBuilder.createTopology());
            Utils.sleep(2 * 100 * 1000);
            localCluster.killTopology("Fuzzy_Clustering_Features_Of_Keyword1");
            localCluster.shutdown();


        }

    }
}