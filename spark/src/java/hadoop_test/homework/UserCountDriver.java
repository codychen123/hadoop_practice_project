package hadoop_test.homework;

import hadoop_test.Utils_hadoop;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserCountDriver {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //程序入口，driver类
        job.setJarByClass(UserCountDriver.class);
        //设置mapper的类(蓝图，人类，鸟类)，实例（李冰冰，鹦鹉2号）,对象

        job.setMapperClass(UserMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(UserReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
//       需要指定combine的实现类，不用指定输出key和value类型
//        job.setCombinerClass(UserCountCombine.class);
//        输入文件
        FileInputFormat.setInputPaths(job, new Path("/hadoop_test/homeWork/ua.base"));

        if( Utils_hadoop.testExist(conf,"/hadoop_test/homeWork/result")){
            Utils_hadoop.rmDir(conf,"/hadoop_test/homeWork/result");}
        FileOutputFormat.setOutputPath(job, new Path("/hadoop_test/homeWork/result"));
        job.waitForCompletion(true);

    }

}
