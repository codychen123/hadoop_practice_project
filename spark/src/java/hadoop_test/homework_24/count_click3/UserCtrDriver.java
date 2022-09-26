package hadoop_test.homework_24.count_click3;

import hadoop_test.Utils_hadoop;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserCtrDriver {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //程序入口，driver类
        job.setJarByClass(UserCtrDriver.class);
        //设置mapper的类(蓝图，人类，鸟类)，实例（李冰冰，鹦鹉2号）,对象

        job.setMapperClass(UserCtrMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(UserCtrReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
//       需要指定combine的实现类，不用指定输出key和value类型
        job.setCombinerClass(UserCtrCombine.class);


//        输入文件
        FileInputFormat.setInputPaths(job, new Path("/sample_train.csv"));

        if( Utils_hadoop.testExist(conf,"/badou_24_homework/ctr_result")){
            Utils_hadoop.rmDir(conf,"/badou_24_homework/ctr_result");}
        FileOutputFormat.setOutputPath(job, new Path("/badou_24_homework/ctr_result"));
        job.waitForCompletion(true);

    }

}
