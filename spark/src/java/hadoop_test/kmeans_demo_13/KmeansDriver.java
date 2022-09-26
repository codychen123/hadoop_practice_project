package hadoop_test.kmeans_demo_13;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KmeansDriver {
    // 主函数
    public static void main(String[] args) throws Exception {
        int repeats = 0;
        do {
            //每一轮都需要构造conf
            Configuration conf = new Configuration();
            // 新建MapReduce作业并指定作业启动类
            Job job = new Job(conf);
            // 设置输入输出路径（输出路径需要额外加判断）
            job.setJarByClass(KmeansDriver.class);
            //1.输入数据路径
            FileInputFormat.addInputPath(job, DataSource.inputpath);// 设置输入路径(指的是文件的输入路径)
//
            FileSystem fs = DataSource.newCenter.getFileSystem(conf);

            if (fs.exists(DataSource.newCenter)) {// 设置输出路径（指的是中心点的输出路径）
                fs.delete(DataSource.newCenter, true);
            }

            FileOutputFormat.setOutputPath(job, DataSource.newCenter);
            // 为作业设置map和reduce所在类
            job.setMapperClass(KmeansMap.class);
            job.setReducerClass(KmeansReduce.class);
            // 设置输出键和值的类
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            // 启动作业
            job.waitForCompletion(true);
            repeats++;

        } while (repeats < DataSource.REPEAT && (Util.isStop(DataSource.old_center+"/part-r-00000",

                DataSource.new_center+"/part-r-00000", repeats, DataSource.threshold)));

        // 进行最后的聚类工作（由map来完成）
        Configuration c_conf = new Configuration();
        // 新建MapReduce作业并指定作业启动类
        Job c_job = new Job(c_conf);
        // 设置输入输出路径（输出路径需要额外加判断）
        FileInputFormat.addInputPath(c_job, DataSource.inputpath);// 设置输入路径(指的是文件的输入路径)
        FileSystem fs = DataSource.newCenter.getFileSystem(c_conf);// 设置输出路径（指的是中心点的输出路径）
        if (fs.exists(DataSource.newCenter)) {
            fs.delete(DataSource.newCenter, true);
        }
        FileOutputFormat.setOutputPath(c_job, DataSource.newCenter);
        // 为作业设置map(没有reducer，则看到的输出结果为mapper的输出)
        c_job.setMapperClass(KmeansMap.class);
        // 设置输出键和值的类
        c_job.setOutputKeyClass(Text.class);
        c_job.setOutputValueClass(Text.class);
        // 启动作业
        c_job.waitForCompletion(true);
    }
}
