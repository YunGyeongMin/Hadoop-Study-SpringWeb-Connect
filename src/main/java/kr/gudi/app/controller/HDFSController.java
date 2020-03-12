package kr.gudi.app.controller;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import kr.gudi.app.hdfs.JobMap;
import kr.gudi.app.hdfs.JobReduce;



@RestController
public class HDFSController {
	
	private Map<String, Object> resultMap = null;
	
	@GetMapping("/home")
	public Map<String, Object> home() throws Exception{
	
	Path p1 = new Path("/user/root/input");
	Path p2 = new Path("/user/root/ouput");
    
     // 하둡 프로그램 정의
     try {
     	Configuration con = new Configuration(); //설정정보 객체 생성
     	con.set("fs.defaultFS","hdfs://192.168.3.209:9000");//윈도우에서 실행하기때문에 
     	FileSystem hdfs = FileSystem.get(con); //파일시스템 객체 가져오기
     	if(hdfs.exists(p2)) { //경로 존재 여부 확인
     		hdfs.delete(p2, true); //경로 삭제 
     	}
			Job job = Job.getInstance(con); //실행 객체 생성
			job.setJarByClass(HDFSController.class);//실행 대상 클래스 설정
			job.setMapperClass(JobMap.class); // 맵클래스 설정
			job.setReducerClass(JobReduce.class);//리듀서 클래스 설정
			job.setMapOutputKeyClass(Text.class); //맵 클래스 결과 자료형 설정
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class); //리듀서 클래스 결과 자료형 설정
			job.setOutputValueClass(IntWritable.class);
			job.setNumReduceTasks(1); // 리듀서 실행 명령 번호 설정
			FileInputFormat.addInputPath(job, p1); // 입력파일과 출력 파일경로 설정
			FileOutputFormat.setOutputPath(job, p2); 
			job.waitForCompletion(true); //정상종료
			hdfs.close();
//			System.exit(job.waitForCompletion(true) ? 0 : 1); //하둡실행후 정상종료 :0 실패:1
		} catch (Exception e) {
			e.printStackTrace();
		}
     
     return resultMap;
     /*Client가 맵리듀스를 수행하면 Job이 생성되고 잡트래커와 태스크트래커에 잡을 할당합니다.
		할당된 잡은 각각의 데이터노드에서 수행되어 결과를 반환하며, 최종적으로 수행이 완료 됩니다*/

     /*MapReduce 작업을 제출하는 동안 MapReduce 작업이 출력에 쓸 새 출력 디렉토리를 제공해야합니다. 
      그러나 출력 디렉토리가 이미 존재하면 OutputDirectoryAlreadyExist라는 예외가 발생합니다. 따라서 경로 삭제 예외처리 추가 */
	}
}
