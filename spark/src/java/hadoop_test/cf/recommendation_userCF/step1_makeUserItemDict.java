package hadoop_test.cf.recommendation_userCF;

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
import java.util.HashMap;
import java.util.Map;

public class step1_makeUserItemDict {
    public static class Mapper1 extends Mapper<LongWritable, Text, Text, Text>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

            String [] values = value.toString().split("\t");
            String userID = values[0];
            String itemID = values[1];
            String score = values[2];
//          user_id，["i2"+"_"+"4","i3"+"_"+"5",]

            context.write(new Text(userID),new Text(itemID+"_"+score));

        }
    }

    public static class Reduce1 extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
//            key 是user_id,values是["i2"+"_"+"4","i3"+"_"+"5",]
            String userID = key.toString();
            Map<String, Integer> map = new HashMap<String, Integer>();
            for(Text value: values){
                String itemID = value.toString().split("_")[0];
                String score = value.toString().split("_")[1];
                if(map.get(itemID)==null){
                    map.put(itemID, Integer.valueOf(score));
                }else{
                    //物品如果多次评分 加起来
                    map.put(itemID, map.get(itemID)+Integer.valueOf(score));
                }

            }
//            创建一个可变字符串
            StringBuilder sb = new StringBuilder();
            for(Map.Entry<String, Integer> entry: map.entrySet()){
                String itemID = entry.getKey();
                String score = String.valueOf(entry.getValue());
                sb.append(itemID+"_"+score+",");
            }
            String line = sb.substring(0,sb.length()-1);
            context.write(new Text(userID),new Text(line));

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "step1_makeUserItemDict");
        job.setJarByClass(step1_makeUserItemDict.class);
        job.setMapperClass(step1_makeUserItemDict.Mapper1.class);
        job.setReducerClass(step1_makeUserItemDict.Reduce1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("/hadoop_test/cf/ua.base"));
        FileOutputFormat.setOutputPath(job, new Path("/hadoop_test/cf/User_item"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
