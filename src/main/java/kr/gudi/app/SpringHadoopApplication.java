package kr.gudi.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpringHadoopApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringHadoopApplication.class, args);
//		if(args.length != 2) { //input , output 2개가 아닐경우
//        	System.out.println("파라메터 2개 있어야합니다.");
//        	System.exit(0); //바로끄기
//        }
//        
//        System.out.println("Hadoop 시스템 동작시작");
	}

}
