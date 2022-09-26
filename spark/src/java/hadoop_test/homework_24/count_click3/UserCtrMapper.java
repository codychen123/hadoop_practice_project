package hadoop_test.homework_24.count_click3;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UserCtrMapper extends Mapper<LongWritable,Text,Text,Text>{

    @Override

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line =  value.toString();
        String[] data = line.split(",");
        String target =data[1];
        String uid = data[5];
//        System.out.println(uid+":"+target);

        if (target.equals("1")){
            context.write(new Text(uid),new Text("1"));
        } else {
            context.write(new Text(uid), new Text("0"));
        }






    }


}