package top.wisely.springsparkcassandra.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.GroupedData;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.expressions.aggregate.Max;
import org.apache.spark.api.java.function.Function;

import scala.Function1;
import scala.runtime.BoxedUnit;

public class SparkJsonLog {



//
//    @Bean
//    public JavaRDD<Person> personTable(){
//        return CassandraJavaUtil.javaFunctions(javaSparkContext()).cassandraTable(cassandraKeyspace, "person",CassandraJavaUtil.mapRowTo(Person.class));
//    }
    
    public static void main(String[] args) {
    	 SparkConf conf = new SparkConf(true)
           .set("spark.cassandra.connection.host", "192.168.43.102")
         .set("spark.submit.deployMode", "client");
   //  JavaSparkContext context = new JavaSparkContext("spark://server110:7077", "SparkJsonLog", conf);
    JavaSparkContext context = new JavaSparkContext("local", "SparkJsonLog", conf);
    JavaRDD<String> textFile = context.textFile("/user/sns_catalina_20180301.out");
    //
      SQLContext sqlContext = new SQLContext(context.sc());
      DataFrame json = sqlContext.read().json(textFile);
      json.registerTempTable("log");
      DataFrame sql = json.sqlContext().sql("select * from log ");
      
      /****
       * 
       * root
 |-- @timestamp: string (nullable = true)
 |-- @version: string (nullable = true)
 |-- _corrupt_record: string (nullable = true)
 |-- host: string (nullable = true)
 |-- message: string (nullable = true)
 |-- path: string (nullable = true)
 |-- severity: string (nullable = true)
 |-- tags: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- timestamp: string (nullable = true)

       */
      json.printSchema();
    //  Row[] collect = sql.collect();
      //System.out.println(sql.groupBy("message").agg(sql.col("message")));;
      DataFrame count=  sql.groupBy("message").count();
    //  count.show();
   Row[] collect2 = count.sort(count.col("count").desc()).limit(1).collect();
      for (int i = 0; i < collect2.length; i++) {
		System.err.println(collect2[i].get(0));
	}
     
      /*.agg(sql.col("message"))*/;
     
  /*   { JavaRDD<Row> rdd = json.selectExpr("host as host","message as message ").javaRDD();
      JavaRDD<String> map = rdd.map(new Function<Row, String>() {
		private static final long serialVersionUID = 1L;
		@Override
  		public String call(Row v1) throws Exception {
			String string = v1.getString(1);
			//System.out.println(string);
  			return string;
  		}
      	  
        });
      System.out.println(map.count());
      System.out.println( map.distinct().count()); }*/
   
	}

}
