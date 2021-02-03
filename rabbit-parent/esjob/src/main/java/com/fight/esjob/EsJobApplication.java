package com.fight.esjob;

import com.fight.task.annotation.EnableElasticJob;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@EnableElasticJob
@SpringBootApplication
@ComponentScan(basePackages = {"com.fight.esjob.*","com.fight.esjob.task.*"})
public class EsJobApplication {

    public static void main(String[] args) {
        SpringApplication.run(EsJobApplication.class, args);
    }

}
