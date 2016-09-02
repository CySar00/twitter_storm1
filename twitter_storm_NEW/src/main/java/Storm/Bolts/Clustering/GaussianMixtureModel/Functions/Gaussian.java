package Storm.Bolts.Clustering.GaussianMixtureModel.Functions;

import com.google.appengine.repackaged.com.google.common.primitives.Doubles;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by christina on 8/8/2016.
 */
public class Gaussian {

    public static Map<String,float[]>calculateTheGaussianDistribtionOfAHashmap(Map<String,double[]>map,float[]mu,float[] covariance){
        int noOfKeys=CalcluateKeySizeAndValuesSizeOfHashmap.calculateTheKeySizeOfAHashmap(map);
        int noOfValues=CalcluateKeySizeAndValuesSizeOfHashmap.calculateTheValuesSizeOfAHashmap(map);

        Map<String,float[]>gaussianDistribution=new HashMap<String, float[]>();

        for(String key:map.keySet()){
            double[]valuesAsVector=map.get(key);
            float[] gaussian=new float[noOfValues];
            for(int i=0;i<valuesAsVector.length;i++){
                float tempValue=(float)valuesAsVector[i];

                if(covariance[i]==0){
                    gaussian[i]=(float)(1/Math.sqrt(2*Math.PI))*(float)(Math.exp(-tempValue*tempValue/2));
                }else{
                    gaussian[i]=(float)(1/Math.sqrt(2*Math.PI*covariance[i]))*(float)(Math.exp(-(tempValue-mu[i])*(tempValue-mu[i])/2*covariance[i]));
                }
            }
            gaussianDistribution.put(key,gaussian);
        }
        return gaussianDistribution;
    }

    public static Map<String,float[]>calculateTheGaussianProbabilityOfAHashmap(Map<String,float[]>map,float[]probabilities){

        Map<String,float[]>gaussianProbability=new HashMap<String, float[]>();
        int noOfValues=CalcluateKeySizeAndValuesSizeOfHashmap.calculateTheValuesSizeOfAJavaStringAndFloatsVectorMap(map);
        for(String key:map.keySet()){
            float[]valuesAsVector=map.get(key);
            float[] gaussianProbabilities=new float[noOfValues];
            float sum=0;
            for(int i=0;i<noOfValues;i++){

                sum+=valuesAsVector[i]*probabilities[i];
            }
            for(int i=0;i<noOfValues;i++){
                    gaussianProbabilities[i] = (float)valuesAsVector[i] * probabilities[i] / sum;

            }
            gaussianProbability.put(key,gaussianProbabilities);
        }
        return gaussianProbability;
     }

    public static float[] calculateTheNk(Map<String,float[]>gaussianProbabilities){
        int noOfKeys=CalcluateKeySizeAndValuesSizeOfHashmap.calculateTheKeySizeOfAJavaStringAndFloatsVectorMap(gaussianProbabilities);
        int noOfValues=CalcluateKeySizeAndValuesSizeOfHashmap.calculateTheValuesSizeOfAJavaStringAndFloatsVectorMap(gaussianProbabilities);
        float[] Nk=new float[noOfValues];
        float[] tempVector=new float[noOfKeys];
        for(int i=0;i<noOfValues;i++){
            int count=0;
            float sum=0;
            for(String key:gaussianProbabilities.keySet()){
                tempVector[count]=gaussianProbabilities.get(key)[i];
                sum+=tempVector[count];
                count+=1;
            }
            Nk[i]=sum;
        }
        for(int i=0;i<noOfValues;i++){
            if(Nk[i]==0){
                Nk[i]=1;
            }
        }
        return Nk;
    }

    public static float[] calculateTheGaussianPosteriorProbability(float[]Nk){
        float[] gaussianPosteriorProbability=new float[Nk.length];
        for(int i=0; i<gaussianPosteriorProbability.length;i++){
            gaussianPosteriorProbability[i]=Nk[i]/Nk.length;
        }
        return gaussianPosteriorProbability;
    }


    public static float[] calculateTheMuk(Map<String,double[]>map,Map<String,float[]>gaussianProbability,float[]Nk){
        float[]muk=new float[Nk.length];
        for(int i=0;i<Nk.length;i++){
            float sum=0;
            for(String key:map.keySet()){
                sum+=map.get(key)[i]*gaussianProbability.get(key)[i];
            }
            muk[i]=sum/Nk[i];
        }
        return muk;
    }

    public static float[] calculateTheSigmaK(Map<String,double[]>map,Map<String,float[]>gaussianProbability,float[]muk,float[]Nk){
        float[]sigmak=new float[Nk.length];
        for(int i=0;i<Nk.length;i++){
            float sum=0;
            for(String key:map.keySet()){
                sum+=gaussianProbability.get(key)[i]*(map.get(key)[i]-muk[i])*(map.get(key)[i]-muk[i]);
            }
            sigmak[i]=sum/Nk[i];
        }
        return sigmak;
    }

    public static Map<String,Float>calculateTheLogLikelihood(Map<String,double[]>map,float[]probability){
        Map<String,Float>logLikelihood=new HashMap<String, Float>();
        for(String key:map.keySet()){
            float sum=0;
            for(int i=0;i<map.get(key).length;i++){
                sum+=map.get(key)[i];
            }
            String serializedSum=String.valueOf(Math.log(sum+1));
            Float sum1=Float.valueOf(serializedSum);
            logLikelihood.put(key,sum1);
        }
        return logLikelihood;
    }

    public static Map<String,Float>calculateTheLogLikelihoodUsingTheGaussianDistribution(Map<String,float[]>gaussianDistribution,float[]probability){
        Map<String,Float>logLikelihood=new HashMap<String, Float>();
        for(String key:gaussianDistribution.keySet()){
            float sum=0;
            for(int i=0;i<gaussianDistribution.get(key).length;i++){
                sum+=gaussianDistribution.get(key)[i];
            }
            String serializedSum=String.valueOf(Math.log(sum+1));
            Float sum1=Float.valueOf(serializedSum);
            logLikelihood.put(key,sum1);
        }
        return logLikelihood;
    }

    public static float calculateTheTotalLogLikelihoodOfAHashmap(Map<String,Float>logLikelihood){
        float totalLogLikelihood=0;
        for(String key:logLikelihood.keySet()){
            totalLogLikelihood+=logLikelihood.get(key);
        }
        return totalLogLikelihood;
    }

}
