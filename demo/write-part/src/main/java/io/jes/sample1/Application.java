package io.jes.sample1;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "io.jes.sample1")
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(new Class[]{Application.class, Config.class}, args);
    }
}

