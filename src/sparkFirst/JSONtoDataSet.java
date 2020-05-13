package sparkFirst;

import java.io.Serializable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
 
public class JSONtoDataSet {
 
    public static class Employee implements Serializable{
        public String name;
        public int salary;
    }
 
    public static void main(String[] args) {
        // configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("Read JSON File to DataSet")
                .master("local[2]")
                .getOrCreate();
 
        // Java Bean (data class) used to apply schema to JSON data
        Encoder<Employee> employeeEncoder = Encoders.bean(Employee.class);
 
        String jsonPath = "C:\\Spark-Learn\\spark-2.4.4-bin-hadoop2.6\\data1\\employees.json";
 
        // read JSON file to Dataset
        Dataset<Employee> ds = spark.read().json(jsonPath).as(employeeEncoder);
        ds.printSchema();
        ds.show();
        ds.createOrReplaceTempView("ds");
    }
}
