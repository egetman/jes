package store.jesframework.readmodel;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "store.jesframework.readmodel.*")
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(new Class[]{Application.class, Config.class}, args);
    }

}

