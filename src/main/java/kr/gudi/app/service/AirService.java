package kr.gudi.app.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.stereotype.Service;

import kr.gudi.app.controller.HDFSController;

@Service
public class AirService {

	public List<Map<String, Object>> find(String key) {
		Path p1 = new Path(key);
		Path p2 = new Path("/user/root/output");
	    
		String p = "/user/root/output";
		String file = "part-r-00000";
		
		Map<String, Integer> resultMap = null;
		List<Map<String, Object>> resultList = new ArrayList<Map<String,Object>>();
		
	     // 하둡 프로그램 정의
	     try {
	     	Configuration con = new Configuration(); //설정정보 객체 생성
	     	con.set("fs.defaultFS","hdfs://192.168.3.209:9000");//클라이언트에서 실행하기때문에 
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
				job.waitForCompletion(true);{
					FileStatus[] fs = hdfs.listStatus(new Path(p)); //목록가져오기
			     	for(FileStatus fss : fs) {
			     		String name = fss.getPath().getName(); //파일이름 가져오기
			     		if(file.equals(name)) {
			     			resultList = getFile(hdfs,fss.getPath());
			     		} else {
			     			
			     		}
			     	}
				} //정상종료
				hdfs.close();
//				System.exit(job.waitForCompletion(true) ? 0 : 1); //하둡실행후 정상종료 :0 실패:1
			} catch (Exception e) {
				e.printStackTrace();
			}
	     
	     return resultList;
	}
	
	public List<Map<String, Object>> getFile(FileSystem hdfs, Path path) throws Exception {
		FSDataInputStream fsis = hdfs.open(path);
		byte[] b = new byte[fsis.available()];
		fsis.read(b);
		String result = new String(b);
		System.out.println(result);
		fsis.close();
		String[] l = result.split("\n");
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		for(String r : l) {
			Map<String, Object> map = new HashMap<String, Object>();
			String[] s = r.split("\t");
			String key = s[0];
			int value = Integer.parseInt(s[1]);
			map.put("key", key);
			map.put("count", value);
			list.add(map);
		}
		return list; 
		
	}
	
}
