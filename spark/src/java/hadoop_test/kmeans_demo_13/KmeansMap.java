package hadoop_test.kmeans_demo_13;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KmeansMap extends Mapper<LongWritable, Text, Text, Text> {
    List<ArrayList<String>> centers;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
       centers= Util.getCenterFile(DataSource.old_center+"/part-r-00000");
//1,0.5483598064494786,0.5620634207706902,0.631409127357256
//2,0.3097815453182178,0.30933422990614073,0.2494839735711083
//3,0.10216864838289068,0.056547788616962635,0.0982705121497766
//4,0.8526661438398866,0.8877750526698496,0.8710574668862512
//5,0.9282123373771547,0.8518260893453832,0.8992078636901775
       System.out.println(centers+"啊哈哈哈哈哈哈哈");
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//     step 1。读取一行数据
        String data = value.toString();
//      step2_similarity 数据切分获得元组
        String[] tmpSplit = data.split(",");
//      step3_ItemUser 申请一个字符列表
        List<String> parameter = new ArrayList<>();
//      step4_predictScore 塞入一条数据 此时paramter第一条数据 第一个位置是用户id
        for (int i = 0; i < tmpSplit.length; i++) {
            parameter.add(tmpSplit[i]);
        }

// step5_recommand_item 读取聚类中心点文件(其中路径参数是从命令中获取的)

        // step6 计算目标对象到各个中心点的距离，找最大距离对应的中心点，则认为此对象归到该点中
        String outKey="" ;// 默认聚类中心为0号中心点
        double minDist = Double.MAX_VALUE;
//        遍历5个聚类中心
        for (int i = 0; i < centers.size(); i++) {
            double dist = 0;
//        计算一个样本跟一个聚类中心的距离
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

        System.out.println(value+"属于："+outKey+"类");

//        1，【0.5997964273824741,0.5381577086398976,0.630918833446788】
//        1，【0.5477960351179503,0.6806860650336256,0.671837028690025】
//
        context.write(new Text(outKey), value);
    }
}
