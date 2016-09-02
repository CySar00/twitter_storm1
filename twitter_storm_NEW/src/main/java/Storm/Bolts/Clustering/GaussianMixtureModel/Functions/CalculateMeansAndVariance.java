package Storm.Bolts.Clustering.GaussianMixtureModel.Functions;

import java.util.Map;

/**
 * Created by christina on 8/8/2016.
 */
public class CalculateMeansAndVariance {

    public static float calculateTheMeansOfAVector(float[] vector){
        float sum=0;
        for(int i=0;i<vector.length;i++){
            sum+=vector[i];
        }
        return sum/vector.length;
    }

    public static float calculateTheVarianceOfAVector(float[]vector){
        float sum=0;

        double meansOfVector=calculateTheMeansOfAVector(vector);
        for(int i=0;i<vector.length;i++){
            sum+=(vector[i]-meansOfVector)*(vector[i]-meansOfVector);
        }
        return sum/(vector.length-1);

    }

    public static float calculateTheStandardDeviationOfAVector(float[] vector){
        return (float)Math.sqrt(calculateTheVarianceOfAVector(vector));

    }

    public static float[] calculateTheMeansOfValuesOfAHashmap(Map<String,double[]>map){
        int noOfKeys=CalcluateKeySizeAndValuesSizeOfHashmap.calculateTheKeySizeOfAHashmap(map);
        int noOfValues=CalcluateKeySizeAndValuesSizeOfHashmap.calculateTheValuesSizeOfAHashmap(map);

        float [] mu=new float[noOfValues];
        float[] tempVector=new float[noOfKeys];

        for(int i=0;i<noOfValues;i++){
            int counter=0;
            for(String key:map.keySet()){
                tempVector[counter]=(float)map.get(key)[i];
                counter+=1;
            }
            mu[i]=calculateTheMeansOfAVector(tempVector);
        }
        return mu;
    }

    public static float[] calculateTheCovarianceOfValuesOfAHashmap(Map<String,double[]>map){
        int noOfKeys=CalcluateKeySizeAndValuesSizeOfHashmap.calculateTheKeySizeOfAHashmap(map);
        int noOfValues=CalcluateKeySizeAndValuesSizeOfHashmap.calculateTheValuesSizeOfAHashmap(map);
        float[]covariance=new float[noOfValues];
        float [] tempVector=new float[noOfKeys];

        for(int i=0;i<noOfValues;i++){
            int counter=0;
            for(String key:map.keySet()){
                tempVector[counter]=(float)map.get(key)[i];
                counter+=1;
            }
            covariance[i]=calculateTheVarianceOfAVector(tempVector);
        }
        return covariance;
    }



    public static float[] calculateProbabilities(int noOfValues){
        float [] probabilities=new float[noOfValues];
        for(int i=0;i<noOfValues;i++){
            probabilities[i]=(float)1/noOfValues;
        }
        return probabilities;
    }

}
