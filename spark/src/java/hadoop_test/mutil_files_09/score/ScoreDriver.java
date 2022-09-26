package hadoop_test.mutil_files_09.score;

import hadoop_test.Utils_hadoop;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import hadoop_test.mutil_files_09.domain.Score;
import org.apache.log4j.BasicConfigurator;


public class ScoreDriver {
	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);

		job.setJarByClass(ScoreDriver.class);

		job.setMapperClass(ScoreMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Score.class);


		job.setReducerClass(ScoreReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Score.class);
		

		FileInputFormat.setInputPaths(job,new Path("/hadoop_test/m_file_test"));

		if( Utils_hadoop.testExist(conf,"/hadoop_test/m_file_test/result")){
			Utils_hadoop.rmDir(conf,"/hadoop_test/m_file_test/result");}

		FileOutputFormat.setOutputPath(job,new Path("/hadoop_test/m_file_test/result"));
		
		job.waitForCompletion(true);
		
		
		
	}
}
