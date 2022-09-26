package hadoop_test.little_files_12;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

//Text, 这个代表的是 ，mapper端来的key的数据格式
// LongWritable,  ：mapper端来的value的数据格式
// Text：reducer端输出的的key的数据格式
// ,LongWritable：reducer端输出的的value的数据格式
public class wordReducer  extends Reducer<Text,LongWritable,Text,LongWritable> {
//
//Text key：     chess
// Iterable<LongWritable> values   [1,1,1,1,1,1,1,1,1,1],这个是所mapper的汇总，来自8（假如）个mapper的value
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {



        String keys = key.toString();

        long count = 0;
        for (LongWritable value: values
             ){

            count+=value.get();
        }
//Text key  RedcuceOutKey：     chess
//  LongWritable,RedcuceOut Value  10
//        chess:10
        context.write(new Text(keys),new LongWritable(count));


    }
}
