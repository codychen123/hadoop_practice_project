package hadoop_test.partition_test_06.flow;

import hadoop_test.partition_test_06.domain.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean,Text,FlowBean> {

	@Override
	protected void reduce(Text name, Iterable<FlowBean> values,
                          Context context)
			throws IOException, InterruptedException {
		FlowBean tmp=new FlowBean();
		for(FlowBean flowbean:values){
			tmp.setAddr(flowbean.getAddr());
			tmp.setPhone(flowbean.getPhone());
			tmp.setName(flowbean.getName());
			tmp.setFlow(tmp.getFlow()+flowbean.getFlow());
		}
		context.write(name, tmp);
	}
}
