package hadoop_test.homework_24.count_click1_2;

import hadoop_test.Utils_hadoop;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserClickListDriver {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);


        //程序入口，driver类
        job.setJarByClass(UserClickListDriver.class);
        //设置mapper的类(蓝图，人类，鸟类)，实例（李冰冰，鹦鹉2号）,对象

        job.setMapperClass(UserClickListMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(UserClickReducerList.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
//       需要指定combine的实现类，不用指定输出key和value类型
//        job.setCombinerClass(UserCtrCombine.class);
//        输入文件

        FileInputFormat.setInputPaths(job, new Path("/badou_24_homework/day02/ua.base"));
        if( Utils_hadoop.testExist(conf,"/badou_24_homework/day02/word_count_result")){
            Utils_hadoop.rmDir(conf,"/badou_24_homework/day02/word_count_result");}
        FileOutputFormat.setOutputPath(job, new Path("/badou_24_homework/day02/word_count_result"));
        job.waitForCompletion(true);

    }

}
