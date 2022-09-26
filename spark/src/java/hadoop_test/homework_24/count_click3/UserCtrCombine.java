package hadoop_test.homework_24.count_click3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UserCtrCombine extends Reducer<Text,Text,Text,Text>{
//Reducer<key_in,value_in,key_out,value_out>
//  key_in,value_in 为map端的输出
//    key_out,value_out 为reduce端的输入
//    这个就是做了局部（本机器下的map汇总）的汇总
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		long click=0;
		long exp=0;
		for(Text value:values){

			if (value.toString().equals("1")){
                System.out.println(value+"::::::::::::::::");
				click+=1;}
				exp+=1;
		}
//        System.out.println("vaav:"+click+":::"+exp);
		context.write(key, new Text(click+"_"+exp));

	}
}
