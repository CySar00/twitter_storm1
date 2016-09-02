package Storm.Spouts.Clustering.Functions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by christina on 4/8/2016.
 */
public class SelectRandomLines {

    public static List<String> selectNRandomLinesFromArrayList(List<String>list,int N){
        List<String>copy=new LinkedList<String>(list);
        Collections.shuffle(copy);
        return copy.subList(0,N);




    }
}
