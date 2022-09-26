package hadoop_test.homework2.cluster_step1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KmeansReduce extends Reducer<Text, Text, Text,Text> {
    public void reduce(Text key, Iterable<Text> value, Context context)
            throws IOException,InterruptedException{
        long num=0;
        double[] re=new double[DataSource.feat_num];
//        T  [1::123.9,32.4::35,1::123.9,32.4::35]
        for(Text T:value){
            String lines=T.toString();
//             T  1::123.9,32.4::35-----》[1,"123.9,32.4",35]
            String[] tmp=lines.split("::");
            num+=Integer.parseInt(tmp[2]);
            System.out.println("tttttt:"+T);
            String[] features = tmp[1].split(",");
            //进行累加,此处仅有两个元素，如果多个元素该如何
            for (int i = 0; i <features.length ; i++) {
                re[i]+=Double.parseDouble(features[i]);

            }

        }
        String result="";
        for (int i = 0; i <re.length ; i++) {
            result=result+re[i]/num+",";

        }

        String re1 = result.substring(0,result.length() -1);

        System.out.println(key.toString()+"----"+re1);
        context.write(key,new Text(re1));
    }
}
