package hadoop_test.homework_24.count_click1_1;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//mapper进程，每一个split（block）会启动该类，
public class UserClickMapper extends Mapper<LongWritable,Text,Text,LongWritable>{

    @Override
//    map,是一条一条执行，只针对一条数据
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//  LongWritable key: 指的是偏移量。
//        uid            iid           score           ts（时间戳）
//        943	825	3	875502283
        String line =  value.toString();
        System.out.println(line);
//        data =[he,love,bigData]
//        2.业务切分每个单词，切分为字符串数组
//        [Harry,hung,back,for,a,last,word,with,Ron,and,Hermione]
        String[] data = line.split("\t");
//      String guid = data[5]
//      String target = data[2]
//        if (target=="1"):
//      context.write(guid,1_1)
//        else:
//        context.write(guid,0_1)
        System.out.println(line);
//         String ts=  data[3]    ;
//        3.遍历字符串数组，然后一步一步输出（word，1）
        context.write(new Text(data[0]),new LongWritable(1));


    }


}