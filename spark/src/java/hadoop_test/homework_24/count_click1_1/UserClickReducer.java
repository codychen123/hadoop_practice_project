package hadoop_test.homework_24.count_click1_1;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UserClickReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
    //   Text,LongWritable
    //   Reducer : Text,LongWritable,Text,LongWritable
    //   （key1,value1）（key1,value2）（key1,value3）（key1,value4）
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
//key1, [value1,value2，value3，value4]
//  guid1， [0_1,1_1,0_1,0_1,1_1,1_1]
// keyin,keyvalue
//        click_count
//        exp_count
// love,[5,9,8]
        long count = 0;
//        [1,1,1,1,1,1,1,1,1,1,1,1,1,1]
        for (LongWritable v:
                values
             ) {
                count+=v.get();
        }
//        // 八斗,15
        context.write(new Text(key),new LongWritable(count));

    }
}
