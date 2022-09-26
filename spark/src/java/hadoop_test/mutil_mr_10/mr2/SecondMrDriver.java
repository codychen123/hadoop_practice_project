package hadoop_test.mutil_mr_10.mr2;

import hadoop_test.Utils_hadoop;
import hadoop_test.mutil_mr_10.company.DoubleMr;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SecondMrDriver {
	public static void main(String[] args) throws Exception {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		
		
		job.setJarByClass(SecondMrDriver.class);
		job.setMapperClass(SecondMrMapper.class);

		job.setMapOutputKeyClass(DoubleMr.class);
		job.setMapOutputValueClass(NullWritable.class);
		/*
		 * 可以指到指到，对于标识文件，MR会自动摒弃
		 */
		FileInputFormat.setInputPaths(job,new Path("/hadoop_test/muti_mr/result"));
		if( Utils_hadoop.testExist(conf,"/hadoop_test/muti_mr/result01")){
			Utils_hadoop.rmDir(conf,"/hadoop_test/muti_mr/result01");}
		FileOutputFormat.setOutputPath(job,new Path("/hadoop_test/muti_mr/result01"));
		
		job.waitForCompletion(true);
		
		
		
	}
}
