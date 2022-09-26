package hadoop_test.word_count_demo_01;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class  wordReducer  extends Reducer<Text,LongWritable,Text,LongWritable>{
    //   Text,LongWritable
    //   Reducer : Text,LongWritable,Text,LongWritable
    //   （key1,value1）（key1,value2）（key1,value3）（key1,value4）
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
//  八斗  [19,32,45]
        long count = 0;

        for (LongWritable v:
                values
             ) {
                count+=v.get();
        }

//        // 八斗,15
        context.write(new Text(key),new LongWritable(count));

    }
}
