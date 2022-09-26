package hadoop_test.data_duplicate_demo_02;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DupMapper extends Mapper<LongWritable,Text,Text,NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

//        value : 192.168.70.49,为什么不给1， 因为1没用。浪费网络io

        context.write(new Text(value),NullWritable.get());

    }
}
