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

public class step2_similarity {
    public static class Mapper2 extends Mapper<LongWritable, Text, Text, Text>{
        private Text outKey = new Text();
        private Text outValue = new Text();
        private List<String> cacheList = new ArrayList<String>();
        private DecimalFormat df = new DecimalFormat("0.00");
//        若存在k个mapper进程，那么setup会被读取k次
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
//              FileReader fr = new FileReader("/hadoop_test/cf/User_item/part-r-00000");
//
//              BufferedReader br = new BufferedReader(fr);
//
//              String line = null;
////              导入user  item_文件
//              while((line = br.readLine())!=null){
//                  cacheList.add(line);
//              }
//              br.close();
//              fr.close();
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
//          一个用户的打分行为.
            String [] values = value.toString().split("\t");
            String userID = values[0];
            String [] itemID_score = values[1].split(",");

            double denominator1 = 0.0;
            //对该用户所有物品的打分 做平方相加，最后在开根号，目的是做分母
            for(String each : itemID_score){
                String score = each.split("_")[1];
                denominator1 += Double.valueOf(score)*Double.valueOf(score);
            }
            denominator1 = Math.sqrt(denominator1);

            //遍历所有用户历史列表
            for(String line: cacheList){
                //拿到用户id
                String userID_matrix2 = line.split("\t")[0];
                //拿到物品评分列表
                String [] itemID_score_matrix2 = line.split("\t")[1].split(",");
                if(userID_matrix2.equals(key.toString())){
                    continue;
                }
                //初始化 第二个对比向量
                double denominator2 = 0.0;
                for(String each : itemID_score_matrix2){
                    String score = each.split("_")[1];
                    denominator2 += Double.valueOf(score)*Double.valueOf(score);
                }
                denominator2 = Math.sqrt(denominator2);


                int numerator = 0;
                //1.对于该用户评价过的物品进行与其他物品计算相似度itemID_score_value  1132_5
                for(String itemID_score_value : itemID_score){
                    String itemID_matrix1 = itemID_score_value.split("_")[0];
                    String score_matrix1 = itemID_score_value.split("_")[1];
                    //拿到对比用户评分列表矩阵
                    for( String itemID_score_value_matrix2 : itemID_score_matrix2){
                        //找到共有的物品
                        if(itemID_score_value_matrix2.startsWith(itemID_matrix1+"_")) {
                            String score_matrix2 = itemID_score_value_matrix2.split("_")[1];
                            numerator += Integer.valueOf(score_matrix1)*Integer.valueOf(score_matrix2);
                        }
                    }

                }
                double cos = numerator / (denominator1*denominator2);
                if( abs(cos-0) < 1e-6) continue;
                outKey.set(userID);
                outValue.set(userID_matrix2+"_"+df.format(cos));
                context.write(outKey,outValue);
            }

        }
    }

    public static class Reduce2 extends Reducer<Text, Text, Text, Text>{
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
        Job job = Job.getInstance(conf, "step2_similarity");
        job.setJarByClass(step2_similarity.class);
        job.setMapperClass(step2_similarity.Mapper2.class);
        job.setReducerClass(step2_similarity.Reduce2.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("/hadoop_test/cf/User_item/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("/hadoop_test/cf/user_simResult"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
