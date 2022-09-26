package hadoop_test.cf.recommendation_userCF;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;


public class step5_recommand_item {
    public static class Mapper5 extends Mapper<LongWritable, Text, Text, Text>{
        private Text outKey = new Text();
        private Text outValue = new Text();
        private List<String> cacheList = new ArrayList<String>();
        protected void setup(Context context ) throws IOException, InterruptedException{
            super.setup(context);
            Path path = new Path("/hadoop_test/cf/User_item/part-r-00000");
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
                cacheList.add(line);
            }
            reader.close();

        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String [] values = value.toString().split("\t");
            String userID = values[0];
            String [] item_score_array = values[1].split(",");

            for(String line: cacheList){
                String userID_matrix2 = line.split("\t")[0];
                String [] item_score_array_matrix2 = line.split("\t")[1].split(",");

                if(userID_matrix2.equals(userID)){
                    for(String item_score_value : item_score_array){
                        boolean flag = false;
                        String itemID_matrix1 = item_score_value.split("_")[0];
                        String score_matrix1 = item_score_value.split("_")[1];
                        for( String itemID_score_value_matrix2 : item_score_array_matrix2){
                            if(itemID_score_value_matrix2.startsWith(itemID_matrix1+"_")) {
                                flag = true;
                            }
                        }
                        if(flag == false){
                            outKey.set(userID);
                            outValue.set(itemID_matrix1+"_"+score_matrix1);
                            context.write(outKey,outValue);
                        }


                    }
                }

            }

        }
    }

    public static class Reduce5 extends Reducer<Text, Text, Text, Text>{
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
        Job job = Job.getInstance(conf, "step5_recommand_item");
        job.setJarByClass(step5_recommand_item.class);
        job.setMapperClass(step5_recommand_item.Mapper5.class);
        job.setReducerClass(step5_recommand_item.Reduce5.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("/hadoop_test/cf/predict_score/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("/hadoop_test/cf/final_result/"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
