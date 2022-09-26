package hadoop_test.avg_demo_03;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AvgMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        //  1,class01 69 key_in value_in
//        class01
        String outkeys=line.split(" ")[0];
//       69
        int outvalues=Integer.parseInt(line.split(" ")[1]);

        context.write(new Text(outkeys),new IntWritable(outvalues));

    }
}
