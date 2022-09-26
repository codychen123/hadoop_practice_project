package hadoop_test.little_files_12;

import hadoop_test.Utils_hadoop;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriverIntput {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(WordCountDriverIntput.class);
        job.setMapperClass(wordMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
//       输入作为SequenceFileInputFormat
        job.setInputFormatClass(SequenceFileInputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("/hadoop_test/word_count/acticle.txt"));

        job.setReducerClass(wordReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        if( Utils_hadoop.testExist(conf,"/hadoop_test/word_count/word_count_result")){
            Utils_hadoop.rmDir(conf,"/hadoop_test/word_count/word_count_result");}
        FileOutputFormat.setOutputPath(job, new Path("/hadoop_test/word_count/word_count_result"));
        job.waitForCompletion(true);

    }

}
