package hadoop_test.sort_test_08.totalsort;

import hadoop_test.Utils_hadoop;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TotalSortDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		
		
		job.setJarByClass(TotalSortDriver.class);
		
		job.setMapperClass(TotalSortMapper.class);
		job.setReducerClass(TotalSortReducer.class);
		
	
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(3);
		job.setPartitionerClass(TotalSortPartitioner.class);

		FileInputFormat.setInputPaths(job,new Path("/hadoop_test/sort_test/sort2.txt"));

		if( Utils_hadoop.testExist(conf,"/hadoop_test/sort_test/result2")){
			Utils_hadoop.rmDir(conf,"/hadoop_test/sort_test/result2");}
		FileOutputFormat.setOutputPath(job,new Path("/hadoop_test/sort_test/result2"));
		
		job.waitForCompletion(true);
		
		
		
	}
}
