package hadoop_test.homework_24.count_click3;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UserCtrReducer extends Reducer<Text,Text,Text,DoubleWritable>{
    //   Text,LongWritable
    //   Reducer : Text,LongWritable,Text,LongWritable
    //   （key1,value1）（key1,value2）（key1,value3）（key1,value4）
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//  八斗  [19,32,45]
        double click=0.0;
        double exp=0.0;
        for (Text v:
                values
             ) {
               String [] temp = v.toString().split("_");
               click+=Double.parseDouble(temp[0]);
               exp+=Double.parseDouble(temp[1]);
        }
//        // 八斗,15
        double ctr =  click/exp ;
        if (ctr>0) {
            System.out.println(key + ":" + ctr);
        }
        context.write(new Text(key),new DoubleWritable(ctr));

    }
}
