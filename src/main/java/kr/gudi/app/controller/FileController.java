package kr.gudi.app.controller;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class FileController {
	String p = "/user/root/ouput";
	String file = "part-r-00000";
	
	Map<String, Object> resultMap = null;
	
	@GetMapping("/file")
	public Map<String, Object> file() throws Exception{
		resultMap = new HashMap<String, Object>();
		Configuration con = new Configuration(); //설정정보 객체 생성
     	con.set("fs.defaultFS","hdfs://192.168.3.209:9000");//클라이언트로 접속할때 실행방법 
     	FileSystem hdfs = FileSystem.get(con); //파일시스템 객체 가져오기
     	FileStatus[] fs = hdfs.listStatus(new Path(p)); //목록가져오기
     	for(FileStatus fss : fs) {
     		String name = fss.getPath().getName(); //파일이름 가져오기
     		if(file.equals(name)) {
     			resultMap.put("result",getFile(hdfs,fss.getPath()));
     		}
     	}
     	hdfs.close();
		return resultMap;
	}
	
	
	public Map<String, Integer> getFile(FileSystem hdfs, Path path) throws Exception {
		FSDataInputStream fsis = hdfs.open(path);
		byte[] b = new byte[fsis.available()];
		fsis.read(b);
		String result = new String(b);
		System.out.println(result);
		fsis.close();
		Map<String, Integer> map = new HashMap<String, Integer>();
		String[] l = result.split("\n");
		for(String r : l) {
			String[] s = r.split("\t");
			String key = s[0];
			int value = Integer.parseInt(s[1]);
			map.put(key, value);
		}
		return map; 
		
	}
//		Path path = new Path(p + "/" + file);
//		FSDataInputStream fsis = hdfs.open(path);
//		StringBuffer sb = new StringBuffer();
//		int b = 0;
//		while((b = fsis.read()) > 0) {
//			sb.append(b);
//		}
//		System.out.println(sb.toString());
	
}
