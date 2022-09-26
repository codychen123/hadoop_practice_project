package hadoop_test.homework2.cluster_step1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import javax.jws.soap.SOAPBinding;
import java.io.IOException;

public class KmeansCombiner extends Reducer<Text, Text, Text,Text> {
    public void reduce(Text key, Iterable<Text> value, Context context)
            throws IOException,InterruptedException{
        //key为类编号，value为类内所有样本,Iterable<Text> value，海量数据，类少的情况下 value是海量的
        //combiner可以用到，
//        顶以一个k维度数，存储累加结果
        double[] re=new double[DataSource.feat_num];
        //记录一个mapper的key总共有多少条数据
        long count =0;
//        遍历value计算累加值，并标记样本个数,
        for(Text T:value){
            String onePoint=T.toString();
            String[] temp=onePoint.split(":");
            onePoint=onePoint.replace(",", " ");
            String[] parameters=onePoint.split(" ");

            //进行累加,此处仅有两个元素，如果多个元素该如何
            for (int i = 1; i <parameters.length ; i++) {
                re[i-1]+=Double.parseDouble(parameters[i]);
            }
//      统计有多少条数据
            count+=1;

        }

        String result=key.toString()+"::";
        for (int i = 0; i <re.length ; i++) {
            result=result+re[i]+",";
        }


        //valueout  1::[123.32,343.42]::129  [行号，各个字段累加，总共的行数]

        System.out.println(result.substring(0,result.length() -1)+"::"+count);
        context.write(key,new Text(result.substring(0,result.length() -1)+"::"+count));
    }
}
