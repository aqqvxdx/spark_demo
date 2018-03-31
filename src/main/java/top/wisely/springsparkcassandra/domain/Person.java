package top.wisely.springsparkcassandra.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Person {
    private  String id;
    private  String name;
    private  int age;
}
