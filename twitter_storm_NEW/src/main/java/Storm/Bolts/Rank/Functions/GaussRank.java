package Storm.Bolts.Rank.Functions;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Values;

import java.util.*;

/**
 * Created by christina on 29/8/2016.
 */
public class GaussRank {

    public static void rankExpertsUsingGaussianRankMethod(OutputCollector outputCollector,Map<String, double[]>cdfs){
        Map<String,double[]>ranks=new HashMap<String, double[]>();
        HashMap rank=new HashMap();
        Map<String,Double>sortedMap=new HashMap<String, Double>();

        int noOfKeys=CalculateMeansAndCovariance.calculateNoOfKeysOfAHashmap(cdfs);
        int noOfElements=CalculateMeansAndCovariance.calculateNoOfVectorElementsOfAHashmap(cdfs);

        for(String key:cdfs.keySet()){
            double prod=1;
            for(int i=0;i<noOfElements;i++){
                prod*=cdfs.get(key)[i];
            }
            rank.put(key,prod);
        }


        sortedMap=sortRankMapUsingValues(rank);
        Iterator iterator=sortedMap.entrySet().iterator();
        int count=0;
        while (iterator.hasNext()){
            Map.Entry<String,Double>entry=(Map.Entry<String,Double>)iterator.next();
            outputCollector.emit(new Values(count,entry.getKey(),entry.getValue()));
            count+=1;
        }
    }

    private static HashMap sortRankMapUsingValues(HashMap map){
        List list=new LinkedList(map.entrySet());
        Collections.sort(list, new Comparator() {
            public int compare(Object o, Object t1) {
                return ((Comparable)((Map.Entry)(o)).getValue()).compareTo(((Map.Entry)(t1)).getValue());
            }
        });
        HashMap hashMapWithSortedValues=new LinkedHashMap();
        for(Iterator iterator=list.iterator();iterator.hasNext();){
            Map.Entry entry=(Map.Entry)iterator.next();
            hashMapWithSortedValues.put(entry.getKey(),entry.getValue());
        }
        return hashMapWithSortedValues;
    }
}
