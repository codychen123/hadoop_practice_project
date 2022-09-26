package hadoop_test.partition_test_06.flow;

import hadoop_test.partition_test_06.domain.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line=value.toString();
		FlowBean flowBean=new FlowBean();
		flowBean.setPhone(line.split(" ")[0]);
		flowBean.setAddr(line.split(" ")[1]);
		flowBean.setName(line.split(" ")[2]);
		flowBean.setFlow(Integer.parseInt(line.split(" ")[3]));

		context.write(new Text(flowBean.getName()), flowBean);
	}
}
