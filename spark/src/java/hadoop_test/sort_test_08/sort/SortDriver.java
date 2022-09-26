package hadoop_test.sort_test_08.sort;

import hadoop_test.Utils_hadoop;
import hadoop_test.sort_test_08.domain.Movie;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;

public class SortDriver {
	
	public static void main(String[] args) throws Exception {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		
		job.setJarByClass(SortDriver.class);
		
		job.setMapperClass(SortMapper.class);
	
//		job.setMapOutputKeyClass(Movie.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);


		FileInputFormat.setInputPaths(job,new Path("/hadoop_test/sort_test/sort.txt"));


		if( Utils_hadoop.testExist(conf,"/hadoop_test/sort_test/result1")){
			Utils_hadoop.rmDir(conf,"/hadoop_test/sort_test/result1");}
		FileOutputFormat.setOutputPath(job,new Path("/hadoop_test/sort_test/result1"));
		
		job.waitForCompletion(true);
		

	}

}
