package sparkFirst;

import java.util.Arrays;
import java.util.List;
 
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
 
public class ParallelizeExample {
 
    public static void main(String[] args) {
        // configure spark
        SparkConf sparkConf = new SparkConf().setAppName("Print Elements of RDD")
                                        .setMaster("local[4]").set("spark.executor.memory","2g");
        // start a spark context
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        
        // sample collection
        List<Integer> collection = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        
        // parallelize the collection to two partitions
        JavaRDD<Integer> rdd = sc.parallelize(collection);
        
        System.out.println("Number of partitions : "+rdd.getNumPartitions());
        
        rdd.foreach(new VoidFunction<Integer>(){ 
              public void call(Integer number) {
                  System.out.println(number); 
        }});
    }
}
