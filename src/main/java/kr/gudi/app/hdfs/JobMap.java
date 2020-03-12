package kr.gudi.app.hdfs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//Mapper<(입력키<행번호> : 입력값<행의글자>) , (출력키<글자> : 출력값<1>)>
public class JobMap extends Mapper<LongWritable, Text, Text, IntWritable>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context result) throws IOException, InterruptedException {
		System.out.println("-----Map-----");
		System.out.println(value);
		
		String[] arr = value.toString().split("");
		for(String txt : arr) {
			Text t = new Text();
			t.set(txt); //text객체에 string txt넣기
			result.write(t, new IntWritable(1));
		}
		
	}
	
	
	
}
