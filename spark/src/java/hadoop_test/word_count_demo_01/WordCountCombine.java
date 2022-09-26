package hadoop_test.word_count_demo_01;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountCombine extends Reducer<Text,LongWritable,Text,LongWritable>{
//Reducer<key_in,value_in,key_out,value_out>
//  key_in,value_in 为map端的输出
//    key_out,value_out 为reduce端的输入
//    这个就是做了局部（本机器下的map汇总）的汇总
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		long result=0;
		for(LongWritable value:values){
			result=result+value.get();
		}
		context.write(key, new LongWritable(result));
	}
}
