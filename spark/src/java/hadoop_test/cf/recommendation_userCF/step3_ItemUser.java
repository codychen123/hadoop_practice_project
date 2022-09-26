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

public class step3_ItemUser {
    // 将 itemID作为行
    public static class Mapper3 extends Mapper<LongWritable, Text, Text, Text>{
        private Text outKey = new Text();
        private Text outValue = new Text();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String [] values = value.toString().split("\t");
            String userID = values[0];
            String[] lines = values[1].split(",");
            for(String each : lines){
                String itemID = each.split("_")[0];
                String score = each.split("_")[1];
                outKey.set(itemID);
                outValue.set(userID+"_"+score);
                context.write(outKey,outValue);

            }

        }
    }

    public static class Reduce3 extends Reducer<Text, Text, Text, Text>{
        private Text outKey = new Text();
        private Text outValue = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

            StringBuilder sb = new StringBuilder();
            for(Text value: values)
                sb.append(value+",");
            String line = sb.substring(0,sb.length()-1);
            outKey.set(key);
            outValue.set(line);
            context.write(outKey,outValue);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "step3_ItemUser");
        job.setJarByClass(step3_ItemUser.class);
        job.setMapperClass(step3_ItemUser.Mapper3.class);
        job.setReducerClass(step3_ItemUser.Reduce3.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("/hadoop_test/cf/User_item/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("/hadoop_test/cf/Item_user/"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
