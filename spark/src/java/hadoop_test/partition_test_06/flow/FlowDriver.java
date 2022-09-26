package hadoop_test.partition_test_06.flow;

import hadoop_test.Utils_hadoop;
import hadoop_test.partition_test_06.domain.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class FlowDriver {
	public static void main(String[] args) throws Exception {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		

		job.setJarByClass(FlowDriver.class);

		job.setMapperClass(FlowMapper.class);

		job.setReducerClass(FlowReducer.class);
		

		job.setMapOutputKeyClass(Text.class);

		job.setMapOutputValueClass(FlowBean.class);
		

		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(FlowBean.class);
		//配置partition,只多不少
		job.setNumReduceTasks(4);

		job.setPartitionerClass(FlowPartitioner.class);

		if( Utils_hadoop.testExist(conf,"/hadoop_test/avro/result")){
			Utils_hadoop.rmDir(conf,"/hadoop_test/avro/result");}
		FileInputFormat.setInputPaths(job,new Path("/hadoop_test/avro/avro.txt"));
		FileOutputFormat.setOutputPath(job,new Path("/hadoop_test/avro/result"));
		job.waitForCompletion(true);
	}

}
