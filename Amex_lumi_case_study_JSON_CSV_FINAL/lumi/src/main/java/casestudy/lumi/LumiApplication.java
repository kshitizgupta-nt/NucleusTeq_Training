package casestudy.lumi;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import java.util.Collections;

@SpringBootApplication
public class LumiApplication {
	public static void main(String[] args) {
		SpringApplication app = new SpringApplication(LumiApplication.class);

		app.setDefaultProperties(Collections.singletonMap(
				"spring.autoconfigure.exclude",
				"org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration"
		));
		app.run(args);
	}
}
