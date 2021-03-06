package Databases.NoSQL.CassandraDB.Clustering;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;
import org.apache.cassandra.db.ColumnFamily;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by christina on 2/8/2016.
 */
public class CassandraSchemaForKCentroids {

    private static StringSerializer stringSerializer=StringSerializer.get();

    public static String[] getCentroidsForKMeansClustering(){
        CassandraHostConfigurator cassandraHostConfigurator=new CassandraHostConfigurator("localhost:9160");
        Cluster cluster=HFactory.getOrCreateCluster("Test Cluster","localhost:9160");

        KeyspaceDefinition keyspaceDefinition=cluster.describeKeyspace("myKKeyspace1");
        if(keyspaceDefinition==null){
            ColumnFamilyDefinition columnFamilyDefinition=HFactory.createColumnFamilyDefinition("myKKeyspace1","clusters",ComparatorType.BYTESTYPE);
            KeyspaceDefinition keyspaceDefinition1=HFactory.createKeyspaceDefinition("myKKeyspace1",ThriftKsDef.DEF_STRATEGY_CLASS,1,Arrays.asList(columnFamilyDefinition));
            cluster.addKeyspace(keyspaceDefinition1,true);
        }
        ConfigurableConsistencyLevel configurableConsistencyLevel=new ConfigurableConsistencyLevel();
        Map<String,HConsistencyLevel>consistencyLevelMap=new HashMap<String, HConsistencyLevel>();
        consistencyLevelMap.put("clusters",HConsistencyLevel.ONE);
        configurableConsistencyLevel.setReadCfConsistencyLevels(consistencyLevelMap);
        configurableConsistencyLevel.setWriteCfConsistencyLevels(consistencyLevelMap);

        Keyspace keyspace=HFactory.createKeyspace("myKKeyspace1",cluster,configurableConsistencyLevel);
        String[] serializedCentroids=new String[2];String[] clusters={"cluster-0","cluster-1"};
        try {
            ColumnQuery<String,String,String>columnQuery=HFactory.createStringColumnQuery(keyspace);
            for(int i=0;i<clusters.length;i++){
                columnQuery.setColumnFamily("clusters").setKey("cluster").setName(clusters[i]);
                QueryResult<HColumn<String,String>>result=columnQuery.execute();
                if(result==null){
                    return null;
                }
                HColumn<String,String>column=result.get();
                if(column==null){
                    return null;
                }
                serializedCentroids[i]=column.getValue();
            }
        }catch (HectorException e){
            e.printStackTrace();
        }
        return serializedCentroids;
    }


    public static void setCentroidsForKMeansClustering(int index,String serializedCentroid){
        CassandraHostConfigurator cassandraHostConfigurator=new CassandraHostConfigurator("localhost:9160");
        Cluster cluster=HFactory.getOrCreateCluster("Test Cluster","localhost:9160");

        ConfigurableConsistencyLevel configurableConsistencyLevel=new ConfigurableConsistencyLevel();
        Map<String,HConsistencyLevel>consistencyLevelMap=new HashMap<String, HConsistencyLevel>();
        consistencyLevelMap.put("clusters",HConsistencyLevel.ONE);
        configurableConsistencyLevel.setReadCfConsistencyLevels(consistencyLevelMap);
        configurableConsistencyLevel.setWriteCfConsistencyLevels(consistencyLevelMap);

        KeyspaceDefinition keyspaceDefinition=cluster.describeKeyspace("myKKeyspace1");
        if(keyspaceDefinition==null){
            ColumnFamilyDefinition columnFamilyDefinition=HFactory.createColumnFamilyDefinition("myKKeyspace1","clusters",ComparatorType.BYTESTYPE);
            KeyspaceDefinition keyspaceDefinition1=HFactory.createKeyspaceDefinition("myKKeyspace1",ThriftKsDef.DEF_STRATEGY_CLASS,1,Arrays.asList(columnFamilyDefinition));
            cluster.addKeyspace(keyspaceDefinition1,true);
        }
        Keyspace keyspace=HFactory.createKeyspace("myKKeyspace1",cluster,configurableConsistencyLevel);
        Mutator<String>mutator=HFactory.createMutator(keyspace,stringSerializer);
        try {
            mutator.insert("cluster","clusters",HFactory.createStringColumn("cluster-"+index,serializedCentroid));
        }catch (HectorException e){
            e.printStackTrace();
        }


    }
}
