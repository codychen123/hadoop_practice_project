package hadoop_test.homework2.cluster_step1;

import com.sun.org.apache.bcel.internal.generic.NEW;
import hadoop_test.Filter.BloomFilter;
import hadoop_test.Utils_hadoop;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
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
        String[] otherArgs={"/hadoop_test/homework2/train_cluster.csv","/hadoop_test/homework2/old_center","10"};
        if (otherArgs.length != 3) {
            System.err.println("Usage: Data Deduplication <in> <out> <cluster num>");
            System.exit(2);
        }

        if( Utils_hadoop.testExist(conf,otherArgs[1])){
            Utils_hadoop.rmDir(conf,otherArgs[1]);}

        conf.set("ClusterNum", otherArgs[2]);
        FileSystem fs = FileSystem.get(conf);
        Path centerPath = new Path(otherArgs[1]);
        fs.deleteOnExit(centerPath);

        Job job = new Job(conf, "InitRandomCenter");
        job.setJarByClass(InitRandomCenter.class);
        job.setMapperClass(InitMap.class);
        job.setReducerClass(InitReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
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

        String[] lines = value.toString().split(",");
//        经纬度
        String values=lines[11]+","+lines[12];
        //skip  --->每隔1000拿一个出来
        Random r = new Random();
//        split 里面100W 最终map会落多少数据
//        随机1000个聚类中心
//  1 kw  ---->   1/100  --》100个
        int rand=r.nextInt(1000);
        //目的是抽取千分之一的数据，因为rand=2或者任意（0,1000）的数字的概率为1/1000
        if (rand==1) {
//            对于初始聚类中心文件输出 kmeans，[1,3,2,4]
            context.write(new Text("lng+lat"), new Text(values));
        }
    }
}

class InitReduce extends Reducer<Text, Text, Text, NullWritable> {
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
        HashSet<String> set = new HashSet<>();
//        塞入数据,此处容易把所有数据全部加载进来，导致直接崩掉，需要改进。
        //一万条，dataList存了一万条数据（抽样来的）
        for (Text val : value) {
            dataList.add(val.toString());
        }
        int k=0;
        //优化初始化聚类中心，你可以挑10个一组，计算均值，加入一个初始聚类中心
        while (k<clusterNum){
            int index = getIndex(dataList.size());
            String point = dataList.get(index);
            if( !set.contains((String.valueOf(point.hashCode())))
                    ){
                System.out.println(k+","+point);
                context.write(new Text(k+"\t"+point), NullWritable.get());
                set.add(String.valueOf(point.hashCode()));
                k+=1;
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

