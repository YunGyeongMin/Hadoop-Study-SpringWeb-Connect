package kr.gudi.app.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

import kr.gudi.app.service.AirService;

@Controller
public class AirController {

	String p = "/user/root/csv";
	String file = "part-r-00000";	
	
	@GetMapping("/Air")
	public String index(Model model) throws Exception {
		
		Configuration con = new Configuration(); 
     	con.set("fs.defaultFS","hdfs://192.168.3.209:9000");
     	FileSystem hdfs = FileSystem.get(con); 
     	FileStatus[] fs = hdfs.listStatus(new Path(p)); 
     	List<Map<String, String>> list = new ArrayList<Map<String, String>>();
     	for(FileStatus fss : fs) {
     		String name = fss.getPath().getName();
     		Map<String, String> map = new HashMap<String, String>();
     		String file = p + "/" + name;
     		String target = name.substring(0 ,name.lastIndexOf("."));
     		map.put("file", p + "/" + name);
     		map.put("key", target);
     		list.add(map);
     	}
     	model.addAttribute("list", list);
		return "index";
	}
	
	@Autowired private AirService as;
	
	@GetMapping("/Air/{key}")
	public String index(@PathVariable("key") String key, Model model) {
		System.out.println(key);
		model.addAttribute("result" , as.find(p + "/" + key + ".csv"));
		model.addAttribute("key",key);
		
		return "result";
	}
	
}
