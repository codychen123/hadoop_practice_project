package hadoop_test.data_duplicate_demo_02;


import hadoop_test.Utils_hadoop;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DupDriver {
/*
192.168.234.21
192.168.234.22
192.168.234.21
192.168.234.21
192.168.234.23
192.168.234.21
192.168.234.21
192.168.234.21
192.168.234.25
192.168.234.21
192.168.234.21
192.168.234.26
192.168.234.21
192.168.234.27
192.168.234.21
192.168.234.27
192.168.234.21
192.168.234.29
192.168.234.21
192.168.234.26
192.168.234.21
192.168.234.25
192.168.234.25
192.168.234.21
192.168.234.22
192.168.234.21
 */
public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf);

    job.setJarByClass(DupDriver.class);

    job.setMapperClass(DupMapper.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);

    FileInputFormat.setInputPaths(job, new Path("/hadoop_test/dup/dup.txt"));
    job.setReducerClass(DupReducer .class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    FileOutputFormat.setOutputPath(job, new Path("/hadoop_test/dup/word_count_result"));
    if( Utils_hadoop.testExist(conf,"/hadoop_test/dup/word_count_result")){
        Utils_hadoop.rmDir(conf,"/hadoop_test/dup/word_count_result");}
    FileOutputFormat.setOutputPath(job, new Path("/hadoop_test/dup/word_count_result"));
    job.waitForCompletion(true);

    job.waitForCompletion(true);
}
}
