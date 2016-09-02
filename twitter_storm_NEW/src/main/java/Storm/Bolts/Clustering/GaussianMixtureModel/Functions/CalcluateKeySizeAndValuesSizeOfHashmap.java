package Storm.Bolts.Clustering.GaussianMixtureModel.Functions;

import java.util.Map;

/**
 * Created by christina on 8/8/2016.
 */
public class CalcluateKeySizeAndValuesSizeOfHashmap {

    public static int calculateTheKeySizeOfAHashmap(Map<String,double[]> map){
        return map.keySet().size();
    }

    public static int calculateTheKeySizeOfAJavaStringAndFloatsVectorMap(Map<String,float[]>map){
        return map.keySet().size();
    }

    public static int calculateTheValuesSizeOfAHashmap(Map<String,double[]>map){
        int noOfKeys=calculateTheKeySizeOfAHashmap(map);
        int sum=0;
        for(Map.Entry<String,double[]>mapEntry:map.entrySet()){
            double [] values=mapEntry.getValue();
            sum+=values.length;
        }
        return sum/noOfKeys;
    }

    public static int calculateTheValuesSizeOfAJavaStringAndFloatsVectorMap(Map<String,float[]>map){
        int noOfKeys=calculateTheKeySizeOfAJavaStringAndFloatsVectorMap(map);
        int sum=0;
        for(Map.Entry<String,float[]>mapEntry:map.entrySet()){
            float[]values=mapEntry.getValue();
            sum+=values.length;
        }
        return sum/noOfKeys;
    }
}
