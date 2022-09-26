package hadoop_test.homework2.newsid_hot2;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ClusterSortMapper extends Mapper<LongWritable,Text,Text,Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //iid1_cluster1   394
            String[]  iid_cluster_sum = value.toString().split("\t");

            String[] iid_cluster=iid_cluster_sum[0].split(":");

            String sum = iid_cluster_sum[1];
            String iid = iid_cluster[0];
            String cluster_id=iid_cluster[1];

            context.write(new Text(cluster_id),new Text(iid+":"+sum));

    }
}