package nameless.canal;

import nameless.canal.client.CanalClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.ContextClosedEvent;

import java.net.UnknownHostException;

@Slf4j
@SpringBootApplication
public class Main {
    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(Main.class);
        application.addListeners((ApplicationListener<ContextClosedEvent>) event -> {
            getCanalClient(event.getApplicationContext()).shutdown();
        });
        ConfigurableApplicationContext applicationContext = application.run(args);
        getCanalClient(applicationContext).run();
    }

    private static CanalClient getCanalClient(ApplicationContext applicationContext) {
        return applicationContext.getBean(CanalClient.class);
    }
}
