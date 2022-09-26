package hadoop_test.homework2.newsid_hot2;

import hadoop_test.Utils_hadoop;
import hadoop_test.homework2.cluster_step1.KmeansCombiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GroupSort {
    public static void main(String[] args) throws Exception {

        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1);

        job1.setJarByClass(GroupSort.class);

        job1.setMapperClass(CountClickMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(LongWritable.class);
        job1.setCombinerClass(CountClickCombiner.class);
        job1.setReducerClass(CountClickReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job1, new Path("/hadoop_test/homework2/result_data/part-r-00000"));
        if( Utils_hadoop.testExist(conf1,"/hadoop_test/homework2/sort_result_count")){
            Utils_hadoop.rmDir(conf1,"/hadoop_test/homework2/sort_result_count");}
        FileOutputFormat.setOutputPath(job1, new Path("/hadoop_test/homework2/sort_result_count"));
        job1.waitForCompletion(true);




        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2);

        job2.setJarByClass(GroupSort.class);

        job2.setMapperClass(ClusterSortMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);


        job2.setCombinerClass(ClusterSortCombiner.class);


        job2.setReducerClass(ClusterSortReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job2, new Path("/hadoop_test/homework2/sort_result_count/part-r-00000"));
        if( Utils_hadoop.testExist(conf2,"/hadoop_test/homework2/sort_result_sort")){
            Utils_hadoop.rmDir(conf2,"/hadoop_test/homework2/sort_result_sort");}
        FileOutputFormat.setOutputPath(job2, new Path("/hadoop_test/homework2/sort_result_sort"));
        job2.waitForCompletion(true);











    }

}
