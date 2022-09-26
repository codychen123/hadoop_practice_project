package hadoop_test.combiner_07;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class wordReducer  extends Reducer<Text,LongWritable,Text,LongWritable> {


//    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
//        String keys = key.toString();
//        long count = 0;
//        for (LongWritable value: values
//             ){
//            count+=value.get();
//        }
//        context.write(new Text(keys),new LongWritable(count));
//
//
//    }
}
