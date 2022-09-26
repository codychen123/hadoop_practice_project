package hadoop_test.avro_test_05.flow;

import hadoop_test.avro_test_05.domain.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean,Text,FlowBean> {

	@Override
	protected void reduce(Text name, Iterable<FlowBean> values,
						  Context context)
			throws IOException, InterruptedException {
       //重新实例化了一个类,他目的是计算用户消费总额
		FlowBean tmp=new FlowBean();
// [flowbean[phone=13766668888,add=sh,name=ls,consum=1000],flowbean[phobe=13766668888,add=BJ,name=ls,consum=21000]]
		for(FlowBean flowbean:values){
//		flowbean	FlowBean [phobe=13766668888,add=sh,name=ls,consum=9844]
// tmp.add=flowbean.getAdd()
//			System.out.println("FLOABEAN"+flowbean);
			tmp.setAdd(flowbean.getAdd());
			tmp.setPhone(flowbean.getPhone());
			tmp.setName(flowbean.getName());
//     tmp.getComsum（初始化是0）+flowbean.getConsum()[9844]
//			在第一轮tmp.getConsum()=[9844]

//			第二轮FlowBean [phobe=13766668888,add=sh,name=ls,consum=1000]
//		tmp.Consum	=tmp.getComsum（9844）+flowbean.getConsum()[100]
			System.out.println(name+"金额"+tmp.getConsum());
			tmp.setConsum(tmp.getConsum()+flowbean.getConsum());

		}
		context.write(name, tmp);
	}
}
