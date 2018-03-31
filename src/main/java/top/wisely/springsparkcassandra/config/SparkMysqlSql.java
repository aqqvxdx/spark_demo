package top.wisely.springsparkcassandra.config;

import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class SparkMysqlSql {



//
//    @Bean
//    public JavaRDD<Person> personTable(){
//        return CassandraJavaUtil.javaFunctions(javaSparkContext()).cassandraTable(cassandraKeyspace, "person",CassandraJavaUtil.mapRowTo(Person.class));
//    }
    
    public static void main(String[] args) {
    	 SparkConf conf = new SparkConf(true)
          .set("spark.cassandra.connection.host", "192.168.43.102")
         .set("spark.submit.deployMode", "client");
      JavaSparkContext context = new JavaSparkContext("spark://server110:7077", "SparkMysql", conf);
      SQLContext sqlContext =new SQLContext(context);
      
      Properties properties = new Properties();
      properties.put("user","root");
      properties.put("password","root");
      String url = "jdbc:mysql://192.168.43.102:3306/resource?useUnicode=true&characterEncoding=utf8";
      DataFrame jdbc = sqlContext.read().jdbc(url, "exam_question_point2", properties);
      //System.out.println(jdbc.collect());
      // DataFrame filter = jdbc.filter(" QUESTION_ID ='195266' ");
         DataFrame filter = jdbc.limit(3).as("filter");
        DataFrame limit = jdbc.limit(100).as("limit").selectExpr("QUESTION_ID as qid");
       
      //  String joinExprs=" filter.QUESTION_ID =limit.QUESTION_ID ";
		DataFrame join = filter.join(limit,
				filter.col("QUESTION_ID").equalTo(limit.col("qid")),
				"right");//.filter(" qid is null");
		
        join.show(1000);
        System.out.println(join.count());
    //  Row[] take = jdbc.take(100);
     // System.out.println(take);
   
	}

}
