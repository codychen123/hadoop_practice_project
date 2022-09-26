package hadoop_test.homework2.cluster_step1;

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KmeansMapPredict extends Mapper<LongWritable, Text, Text, NullWritable> {
    List<ArrayList<String>> centers;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
       centers= Util.getCenterFile(DataSource.old_center+"/part-r-00000");

    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//     step 1。读取一行数据
        String data = value.toString();
//      step2_similarity 数据切分获得元组
        String[] tmpSplit = data.split(",");
//      step3_ItemUser 申请一个字符列表
        List<String> parameter = new ArrayList<>();
//        uid
        parameter.add(tmpSplit[0]);
//        lng
        parameter.add(tmpSplit[11]);
//        lat
        parameter.add(tmpSplit[12]);

// step5_recommand_item 读取聚类中心点文件(其中路径参数是从命令中获取的)

        // step6 计算目标对象到各个中心点的距离，找最大距离对应的中心点，则认为此对象归到该点中
        String outKey="" ;// 默认聚类中心为0号中心点
        double minDist = Double.MAX_VALUE;
        for (int i = 0; i < centers.size(); i++) {
            double dist = 0;
            //第一个位置是cluster_id 和 uid
            for(int j=1;j<centers.get(i).size();j++){
                double a=Double.parseDouble(parameter.get(j));
                double b=Double.parseDouble(centers.get(i).get(j));
                dist+=Math.pow(a-b,2);

            }
            if (dist < minDist) {
                outKey = centers.get(i).get(0);// 类编号
                minDist = dist;
            }

        }
        String key_out= data+","+outKey;


        context.write(new Text(key_out), NullWritable.get());


    }
}
