package hadoop_test.homework2.newsid_hot2;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CountClickMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[]  features = value.toString().split(",");

            String click = features[1];
            String newsid = features[4];
            String cluster_id=features[features.length-1];

            if (click.equals("1")){
                context.write(new Text(newsid+":"+cluster_id),new LongWritable(1));
            }












    }
}