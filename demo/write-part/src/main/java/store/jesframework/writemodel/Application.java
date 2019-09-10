package store.jesframework.writemodel;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "store.jesframework.writemodel.*")
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(new Class[]{Application.class, Config.class}, args);
    }
}

