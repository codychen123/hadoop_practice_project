package hadoop_test.homework2.newsid_hot2;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountClickReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count =0;
        for (LongWritable v:
                values
             ) {
                count+=v.get();
        }
//iid1_cluster9 34
        context.write(new Text(key),new LongWritable(count));

    }
}
