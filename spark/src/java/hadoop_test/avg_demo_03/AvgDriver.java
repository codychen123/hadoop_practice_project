package hadoop_test.avg_demo_03;

import hadoop_test.Utils_hadoop;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvgDriver {
/*
tom 69
tom 88
tom 78
jary 109
jary 90
jary 81
jary 35
rose 23
rose 100
rose 230
 */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(AvgDriver.class);
        job.setMapperClass(AvgMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job, new Path("/hadoop_test/avg/avg.txt"));
        job.setReducerClass(AvgReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileOutputFormat.setOutputPath(job, new Path("/hadoop_test/avg/result"));
        if( Utils_hadoop.testExist(conf,"/hadoop_test/avg/result")){
            Utils_hadoop.rmDir(conf,"/hadoop_test/avg/result");}
        FileOutputFormat.setOutputPath(job, new Path("/hadoop_test/avg/result"));
        job.waitForCompletion(true);

        job.waitForCompletion(true);
    }
}
