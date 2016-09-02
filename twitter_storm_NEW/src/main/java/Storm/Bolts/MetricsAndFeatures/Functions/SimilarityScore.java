package Storm.Bolts.MetricsAndFeatures.Functions;

/**
 * Created by christina on 1/8/2016.
 */
public class SimilarityScore {

    private static int  min(int a,int b,int c){
        return Math.min(Math.min(a,b),c);
    }

    public static int calculateLevenshteinDistance(String string1,String string2){
        int [][]distance=new int[string1.length()+1][string2.length()+1];

        for(int i=0;i<=string1.length();i++){
            distance[i][0]=i;
        }

        for(int i=0;i<=string2.length();i++){
            distance[0][i]=i;
        }

        for(int i=1; i<=string1.length();i++){
            for(int j=1;j<=string2.length();j++){
                distance[i][j]=min(
                        distance[i-1][j]+1,
                        distance[i][j-1]+1,
                        distance[i][j]+((string1.charAt(i-1)==string2.charAt(j-1)) ? 0:1 )

                );
            }
        }
        return distance[string1.length()][string2.length()];

    }
}
