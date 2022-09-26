package hadoop_test.matrix_mutil_13;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Matrix_mutil {
    public static void main(String[] args) throws Exception {
        String input1 = "/hadoop_test/matrix/matrixA";
        String input2 = "/hadoop_test/matrix/matrixB";
        String output = "/hadoop_test/matrix/matrixC";

        Configuration conf =new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Matrix_mutil.class);

        FileInputFormat.setInputPaths(job, new Path(input1), new Path(input2));// 加载2个输入数据集
        Path outputPath = new Path(output);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
        if (fs.exists(new Path(outputPath.toString()))) {
            fs.delete(new Path(outputPath.toString()), true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setMapperClass(MatrixMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(MatrixReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.waitForCompletion(true);

    }
}

class MatrixMapper extends Mapper<LongWritable, Text, Text, Text> {
//    核心教给大家 如何取分开独立计算。
    private String fileNAME = null;// 数据集名称
    //通过mr得到矩阵行数 列数，属于全局排序问题。1.前端大数据--》存好。2.只能在mr处理去加一个累加器。
    //
    private int rowNum_A = 4;// 矩阵A的行数
    private int colNum_B = 2;// 矩阵B的列数
    private int rowIndexA = 1; // 矩阵A，当前在第几行
    private int rowIndexB = 1; // 矩阵B，当前在第几行

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        //矩阵文件名
        fileNAME = ((FileSplit) context.getInputSplit()).getPath().getName();
    }
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] temp= value.toString().split(":");
        rowIndexA=Integer.parseInt(temp[0]);
        rowIndexB=Integer.parseInt(temp[0]);
        String[] row_value_temp = temp[1].split(",");
        //按行读取值
        String[] row_values = value.toString().split(",");
        //代表左矩阵
        if ("A.txt".equals(fileNAME)) {
                //如果是A矩阵 以一行为例[1, 2, 3]
            for (int i = 1; i <= colNum_B; i++) {
                //key 为 如果是A,则 key(rowIndexA为一个累加器)的第一个表示行索引，第二个元素为B的列索引
                //表示第一行的数据应该发送到同一个reduce中，而且需要这一行的是 B矩阵的第1 2 列
                //因此 表示为  key =1，1  1，2 表示A矩阵的第一行，被B矩阵的第1、2列所需要
                Text k = new Text(rowIndexA + "," + i);
                for (int j = 0; j < row_values.length; j++) {
//                    value  a(表示矩阵名称)  (j+1)对与A矩阵来说，表示列偏移
//                    (这个值需要跟B矩阵的行偏移确定位置后才可以相乘)，row_values[j] 表示值。
                    //value
                    Text v = new Text("a," + (j + 1) + "," + row_values[j]);
                    System.out.println(k+":"+v);

                    context.write(k, v);
                }
            }
            rowIndexA++;// 每执行一次map方法，矩阵向下移动一行
        } else if ("B.txt".equals(fileNAME)) {
            //如果是A矩阵 以一行为例[7, 9],B矩阵中的7,9 会被发往不同的reduce中去
            for (int i = 1; i <= rowNum_A; i++) {
                for (int j = 0; j < row_values.length; j++) {
//                    key表示 i为（A矩阵需要的行）
                    Text k = new Text(i + "," + (j + 1));
                    Text v = new Text("b," + rowIndexB + "," + row_values[j]);
                    System.out.println(k+":"+v);
                    context.write(k, v);
                }
            }
            rowIndexB++;// 每执行一次map方法，矩阵向下移动一行
        }
    }

}
class MatrixReducer extends Reducer<Text, Text, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws  InterruptedException {
        Map<String, String> mapA = new HashMap<String, String>();
        Map<String, String> mapB = new HashMap<String, String>();

        for (Text value : values) {
            String[] val = value.toString().split(",");
            if ("a".equals(val[0])) {
                mapA.put(val[1], val[2]);
            } else if ("b".equals(val[0])) {
                mapB.put(val[1], val[2]);
            }
        }
        int result = 0;
        Iterator<String> mKeys = mapA.keySet().iterator();
        while (mKeys.hasNext()) {
            String mkey = mKeys.next();
            if (mapB.get(mkey) == null) {// 因为mkey取的是mapA的key集合，所以只需要判断 mapB 是否存在即可。
                continue;
            }
            result += Integer.parseInt(mapA.get(mkey))
                    * Integer.parseInt(mapB.get(mkey));
        }
        try {
            context.write(key, new IntWritable(result));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}