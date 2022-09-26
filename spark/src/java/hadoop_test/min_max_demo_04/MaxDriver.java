package hadoop_test.min_max_demo_04;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class MaxDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		

		job.setJarByClass(MaxDriver.class);

		job.setMapperClass(MaxMapper.class);

		job.setReducerClass(MaxReducer.class);
		

		job.setMapOutputKeyClass(Text.class);

		job.setMapOutputValueClass(IntWritable.class);
		

		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(IntWritable.class);
		

		FileInputFormat.setInputPaths(job,new Path("/hadoop_test/min_max/max.data"));

		FileOutputFormat.setOutputPath(job,new Path("/hadoop_test/min_max/result"));
		
		job.waitForCompletion(true);
		
		
		
	}

}
