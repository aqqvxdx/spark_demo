package top.wisely.springsparkcassandra.repo;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import top.wisely.springsparkcassandra.domain.Person;

@Repository
@Slf4j
public class PersonRepository {
    @Autowired
    CassandraSQLContext cassandraSQLContext;

//    @Autowired
//    JavaRDD<Person> personTable;

    public Long countPerson(){

		//        return personTable.count();
        System.out.println("-----------" +cassandraSQLContext.getCluster() + "-----------");
        System.out.println("-----------" +cassandraSQLContext.getKeyspace() + "-----------");

        DataFrame people = cassandraSQLContext.sql("select * from person order by id");
        System.out.println("-----------" +people.count() + "-----------");
        return people.count();
    }

}
