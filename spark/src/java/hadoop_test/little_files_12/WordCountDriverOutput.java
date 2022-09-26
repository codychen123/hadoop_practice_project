package hadoop_test.little_files_12;

import hadoop_test.Utils_hadoop;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class WordCountDriverOutput {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(WordCountDriverOutput.class);
        job.setMapperClass(wordMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 设置输出类
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        /**
         * 设置sequecnfile的格式，对于sequencefile的输出格式，有多种组合方式,
         */

        // 组合方式1：不压缩模式
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.NONE);
        //组合方式2：record压缩模式，并指定采用的压缩方式 ：默认、gzip压缩等
//                SequenceFileOutputFormat.setOutputCompressionType(job,
//                        SequenceFile.CompressionType.RECORD);
//                SequenceFileOutputFormat.setOutputCompressorClass(job,
//                        DefaultCodec.class);


        //组合方式3：block压缩模式，并指定采用的压缩方式 ：默认、gzip压缩等
//                SequenceFileOutputFormat.setOutputCompressionType(job,
//                        SequenceFile.CompressionType.BLOCK);
//                SequenceFileOutputFormat.setOutputCompressorClass(job,
//                        DefaultCodec.class);







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
