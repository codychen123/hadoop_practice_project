package hadoop_test.kmeans_demo_13;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class InitRandomCenter {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
//        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        String[] otherArgs={"/hadoop_test/kmeans/user_info.txt","/hadoop_test/kmeans/init_center","5"};
        if (otherArgs.length != 3) {
            System.err.println("Usage: Data Deduplication <in> <out> <cluster num>");
            System.exit(2);
        }
        conf.set("ClusterNum", otherArgs[2]);
        FileSystem fs = FileSystem.get(conf);
        Path centerPath = new Path(otherArgs[1]);
        fs.deleteOnExit(centerPath);

        Job job = new Job(conf, "InitRandomCenter");
        job.setJarByClass(InitRandomCenter.class);
        job.setMapperClass(InitMap.class);
        job.setReducerClass(InitReduce.class);

        //设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        //设置输入和输出目录
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        job.waitForCompletion(true);
    }

}

class InitMap extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {


        if (value.toString() != null) {
//            对于初始聚类中心文件输出 kmeans，[1,3,2,4]
            context.write(new Text("kmeans"), value);
        }
    }
}

class InitReduce extends Reducer<Text, Text, Text, Text> {
    int clusterNum;
//    避免重复index进入初始聚类中心
    private static Set<Integer> indexSet = new HashSet<Integer>();

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        clusterNum = Integer.parseInt(conf.get("ClusterNum"));
    }

    @Override
    protected void reduce(Text key, Iterable<Text> value, Context context)
            throws IOException, InterruptedException {

//        定义字符数组 [["1","2","3"],["1.7","2.0","3.3"]]
        List<String> dataList = new ArrayList<String>();
//        塞入数据,此处容易把所有数据全部加载进来，导致直接崩掉，需要改进。
        for (Text val : value) {
            dataList.add(val.toString());
        }

        for (int k = 1; k <= clusterNum; k++) {
            int index = getIndex(dataList.size());
//            获取一个样本点
            String point = dataList.get(index);

            if (point != null) {

                context.write(new Text(point.toString()), new Text());
            }
        }
    }
    /*
     * 生成不重复的随机数
     */
    public static int getIndex(int size) {

        int res;
        Random random = new Random();
        while (true) {
            int index = random.nextInt(size);
            if (!indexSet.contains(index)) {
                res = index;
                indexSet.add(index);
                break;
            }
        }
        return res;
    }
}

