package hadoop_test.min_max_demo_04;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line=value.toString();
//		拿出年份
		String year=line.substring(8,12);
//      拿出温度
		int temp=Integer.parseInt(line.substring(18));
		context.write(new Text(year), new IntWritable(temp));
	}

}
