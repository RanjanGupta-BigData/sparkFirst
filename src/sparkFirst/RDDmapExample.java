package sparkFirst;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
 
public class RDDmapExample {
 
    public static void main(String[] args) {
        // configure spark
        SparkConf sparkConf = new SparkConf().setAppName("Read Text to RDD")
                                        .setMaster("local[2]").set("spark.executor.memory","2g");
        // start a spark context
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        
        // provide path to input text file
        String path = "C:\\Spark-Learn\\spark-2.4.4-bin-hadoop2.6\\data1\\data.txt";
        
        // read text file to RDD
        JavaRDD<String> lines = sc.textFile(path);
        
        // map each line to number of words in the line
        JavaRDD<Integer> n_words = lines.map(x -> x.split(" ").length); 
        
        // collect RDD for printing
        for(int n:n_words.collect()){
            System.out.println(n);
        }
        sc.stop();
    }
 
}
