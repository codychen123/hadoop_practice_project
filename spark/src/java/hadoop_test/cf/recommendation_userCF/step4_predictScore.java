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
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.abs;

public class step4_predictScore {
    public static class Mapper4 extends Mapper<LongWritable, Text, Text, Text>{
        private Text outKey = new Text();
        private Text outValue = new Text();
        private List<String> cacheList = new ArrayList<String>();
        private DecimalFormat df = new DecimalFormat("0.00");
        protected void setup(Context context ) throws IOException, InterruptedException{
            super.setup(context);

            Path path = new Path("/hadoop_test/cf/Item_user/part-r-00000");
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
            //拿到userId的相似用户
            String [] user_sim = values[1].split(",");
            //遍历每个用户
            for(String line: cacheList){
                //拿到itemid，  还有userId ，score
                String itemID_matrix2 = line.split("\t")[0];
                String [] userID_score_matrix2 = line.split("\t")[1].split(",");

                double numerator = 0.0;
                //遍历与该用户相似的用户
                for(String userID_score_value : user_sim){
                    //该用户的相似用户的ID
                    String userID_matrix1 = userID_score_value.split("_")[0];
                    //对应的相似分数
                    String sim_matrix1 = userID_score_value.split("_")[1];
                    //拿到这个物品，所有用户对其的评分
                    for( String userID_score_value_matrix2 : userID_score_matrix2){
                        //如果对这个物品评分的用户，在userID_matrix1相似用户中
                        if(userID_score_value_matrix2.startsWith(userID_matrix1+"_")) {
                            String score_matrix2 = userID_score_value_matrix2.split("_")[1];
                            numerator += Double.valueOf(sim_matrix1)*Double.valueOf(score_matrix2);
                        }
                    }

                }
                if( abs(numerator-0) < 1e-6) continue;
                outKey.set(userID);
                outValue.set(itemID_matrix2+"_"+df.format(numerator));
                context.write(outKey,outValue);
            }

        }
    }

    public static class Reduce4 extends Reducer<Text, Text, Text, Text>{
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
        Job job = Job.getInstance(conf, "step4_predictScore");
        job.setJarByClass(step4_predictScore.class);
        job.setMapperClass(Mapper4.class);
        job.setReducerClass(Reduce4.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("/hadoop_test/cf/user_simResult/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("/hadoop_test/cf/predict_score"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
