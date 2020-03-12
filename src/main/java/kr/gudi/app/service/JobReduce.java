package kr.gudi.app.service;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Reducer<(입력키<Map출력키>: 입력값<Map출력값>) , (출력키<파일저장될 키워드> : 출력값<파일저장될 키워드의 수>)>
public class JobReduce extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	protected void reduce(Text key, Iterable<IntWritable> value, Reducer<Text, IntWritable, Text, IntWritable>.Context result) throws IOException, InterruptedException {
//		System.out.println("----reduce----");
//		System.out.println(key);
		int tot = 0;
		for(IntWritable i : value) {
			tot += i.get();
		}
		result.write(key, new IntWritable(tot));
	}
	
}
