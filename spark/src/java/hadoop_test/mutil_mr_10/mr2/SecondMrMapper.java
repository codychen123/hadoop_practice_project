package hadoop_test.mutil_mr_10.mr2;

import hadoop_test.mutil_mr_10.company.DoubleMr;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SecondMrMapper extends Mapper<LongWritable, Text, DoubleMr, NullWritable> {
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line=value.toString();
		String[] data=line.split(" ");
		DoubleMr mr=new DoubleMr();
		mr.setName(data[0]);
		mr.setProfit(Integer.parseInt(data[1]));
//		存在问题，是因为mapper端，是局部排序，要想全局排序,
//		context.write(1, mr);
		context.write(mr, NullWritable.get());
	}

}
