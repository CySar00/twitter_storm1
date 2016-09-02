package Storm.Bolts.Rank.Functions;

import java.util.*;

/**
 * Created by christina on 1/9/2016.
 */
public class BordaRank {

    public static HashMap sortExpertsUsingBordaScoreValues(Map<String,Double>map){
        List list=new LinkedList(map.entrySet());
        Collections.sort(list, new Comparator() {
            public int compare(Object o, Object t1) {
                return ((Comparable)((Map.Entry)(o)).getValue()).compareTo(((Map.Entry)(t1)).getValue());
            }
        });
        HashMap hashmapWithSortedValues=new LinkedHashMap();
        for(Iterator iterator=list.iterator();iterator.hasNext();){
            Map.Entry entry=(Map.Entry)iterator.next();
            hashmapWithSortedValues.put(entry.getKey(),entry.getValue());
        }
        return hashmapWithSortedValues;
    }
}
