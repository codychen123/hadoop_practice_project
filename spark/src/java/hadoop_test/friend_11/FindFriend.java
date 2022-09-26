package hadoop_test.friend_11;

import hadoop_test.Utils_hadoop;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.util.Arrays;

public class FindFriend {
    /*
    * 1.遍历文件内容，讲朋友为key，用户为值，输出
    * */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
//        step1
        Job job1 = Job.getInstance(conf);
        job1.setJarByClass(FindFriend.class);
        job1.setMapperClass(FriendMapper.class);
        job1.setReducerClass(FriendReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job1,new Path("/hadoop_test/findFriend/friend.txt"));

        if( Utils_hadoop.testExist(conf,"/hadoop_test/findFriend/result")){
            Utils_hadoop.rmDir(conf,"/hadoop_test/findFriend/result");}
        FileOutputFormat.setOutputPath(job1, new Path("/hadoop_test/findFriend/result"));
        boolean res1=job1.waitForCompletion(true);


//        step1
        Job job2 = Job.getInstance(conf);
        job2.setJarByClass(FindFriend.class);
        job2.setMapperClass(FriendMapper1.class);
        job2.setReducerClass(FriendReducer1.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job2,new Path("/hadoop_test/findFriend/result"));
        if( Utils_hadoop.testExist(conf,"/hadoop_test/findFriend/result1")){
            Utils_hadoop.rmDir(conf,"/hadoop_test/findFriend/result1");}
        FileOutputFormat.setOutputPath(job2, new Path("/hadoop_test/findFriend/result1"));
        boolean res2 = job2.waitForCompletion(true);

        System.exit(res1?0:1);


    }


}
class FriendMapper  extends Mapper<LongWritable,Text, Text,Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//       u1 关注了 u2,
//       u1   u2,u3,u4,u5,u6,u7,u8

        String line = value.toString();

        String uid = line.split("\t")[0];

        String[] friends = line.split("\t")[1].split(",");
//    u1
//    friends[u2,u3,u4,u5,u6,u7,u8]
        for (String fre: friends
        )
//        u2,u1
//        u3,u1
//        u4,u1
//        ......

            context.write(new Text(fre),new Text(uid));
        }


    }

class FriendReducer extends Reducer<Text,Text,Text,Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//      u2， [u1,u4,u5,u8,u9 ] ，关注u2的用户有哪些。

        StringBuilder sb = new StringBuilder();

        for (Text fre: values
             ) {
            sb.append(fre.toString()).append(",");
        }
        String re=sb.substring(0,sb.length()-1);
//        xx美少女,  u1,u4,u5,u8,u9
        context.write(key,new Text(re));


    }
}
class FriendMapper1 extends Mapper<LongWritable,Text, Text,Text>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        /// xx美少女   u1,u4,u5,u8,u9
        String line = value.toString();

        String[] lines= line.split("\t");
        //博主
        String uid = lines[0];
        //拿粉丝
        String[] fri = lines[1].split(",");
        // 保证不会出现 u1_u2  和 u2_u1 两个ky
        Arrays.sort(fri);
//        u1,u4,u5,u8,u9
//        u1_u4,xx美少女
//        u1_u5,xx美少女
//        u8_u9,xx美少女

//        u1,u4,u5,u8,u9
////        u1_u4,xx篮球
////        u1_u5,xx篮球
////        u8_u9,xx篮球
        for (int i = 0; i <fri.length-1 ; i++) {
            for (int j = i+1; j <fri.length ; j++) {
                context.write(new Text(fri[i]+"_"+fri[j]),new Text(uid));
            }
        }
    }
}
class FriendReducer1 extends Reducer<Text,Text,Text,Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//        u1_u4,[xx美少女,xx篮球]
        StringBuilder sb = new StringBuilder();
        for (Text fre:values
             ) {
            sb.append(fre+",");
        }
        String re = sb.substring(0,sb.length()-1);
        context.write(new Text(key),new Text(re));
    }
}
