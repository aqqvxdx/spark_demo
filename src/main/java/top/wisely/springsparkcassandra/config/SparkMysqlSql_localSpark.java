package top.wisely.springsparkcassandra.config;

import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.GroupedData;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

public class SparkMysqlSql_localSpark {



//
//    @Bean
//    public JavaRDD<Person> personTable(){
//        return CassandraJavaUtil.javaFunctions(javaSparkContext()).cassandraTable(cassandraKeyspace, "person",CassandraJavaUtil.mapRowTo(Person.class));
//    }
    
    public static void main(String[] args) {
    	 SparkConf conf = new SparkConf(true)
          .set("spark.cassandra.connection.host", "192.168.43.102")
         .set("spark.submit.deployMode", "client");
      JavaSparkContext context = new JavaSparkContext("local", "SparkMysql", conf);
      SQLContext sqlContext =new SQLContext(context);
      
      Properties properties = new Properties();
      properties.put("user","root");
      properties.put("password","root");
      String url = "jdbc:mysql://192.168.43.102:3306/resource?useUnicode=true&characterEncoding=utf8";
      DataFrame jdbc = sqlContext.read().jdbc(url, "study_materials", properties);
      
      DataFrame filter = jdbc.filter(" ID=1 ");
      //System.out.println(jdbc.collect());
      // DataFrame filter = jdbc.filter(" QUESTION_ID ='195266' ");
        System.out.println(filter.count());
    //  Row[] take = jdbc.take(100);
     // System.out.println(take);
   
	}

}
