package hadoop_test.mutil_mr_10.mr1;

import hadoop_test.mutil_mr_10.company.DoubleMr;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FirstMrMapper extends Mapper<LongWritable, Text, Text, DoubleMr> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line=value.toString();
		String[] data=line.split(" ");
		DoubleMr mr=new DoubleMr();
		//作为keyOut
		mr.setName(data[1]);
		//计算利润作为valueOut
		mr.setProfit(Integer.parseInt(data[2])-Integer.parseInt(data[3]));
		
		context.write(new Text(mr.getName()), mr);
	}
}
