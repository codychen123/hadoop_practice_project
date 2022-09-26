package hadoop_test.homework;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import hadoop_test.Data_utils;
public class UserMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line =  value.toString();

        String[] data = line.split("\t");
        String outKey = data[0];

        String dates= Data_utils.stampToDate(String.valueOf(Integer.parseInt(data[3])*1000));
        //过滤
        if (dates.equals("1969-12-24")){
            context.write(new Text(outKey+":1969-12-24"),new LongWritable(1));
        }



    }
}