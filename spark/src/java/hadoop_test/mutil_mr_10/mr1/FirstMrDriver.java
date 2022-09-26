package hadoop_test.mutil_mr_10.mr1;

import hadoop_test.Utils_hadoop;
import hadoop_test.mutil_mr_10.company.DoubleMr;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class FirstMrDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		
		
		job.setJarByClass(FirstMrDriver.class);
	
		job.setMapperClass(FirstMrMapper.class);

		job.setReducerClass(FirstMrReducer.class);
		
		
		job.setMapOutputKeyClass(Text.class);
		
		job.setMapOutputValueClass(DoubleMr.class);
		
		
		job.setOutputKeyClass(DoubleMr.class);
		
		job.setOutputValueClass(NullWritable.class);
		
		
		FileInputFormat.setInputPaths(job,new Path("/hadoop_test/muti_mr/mutil_mr.txt"));


		if( Utils_hadoop.testExist(conf,"/hadoop_test/muti_mr/result")){
			Utils_hadoop.rmDir(conf,"/hadoop_test/muti_mr/result");}
		FileOutputFormat.setOutputPath(job,new Path("/hadoop_test/muti_mr/result"));
		
		job.waitForCompletion(true);
		
		
		
	}
}
