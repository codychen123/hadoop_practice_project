package hadoop_test.little_files_12;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.InputStream;
import java.net.URI;

public class Seq {

    public static void main(String[] args) throws Exception {
        small2Big();
    }

/**
 * SequenceFile 写操作
 */

    public static void SequenceWriter() throws Exception{

        final String INPUT_PATH= "/hadoop_test/little_file";
        final String OUTPUT_PATH= "/hadoop_test/little_file_process";
        //获取文件系统

        Configuration conf = new Configuration();

        conf.set("fs.defaultFS", "hdfs://192.168.70.10:9000");

        FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);



        //创建seq的输出流

        Text key = new Text();

        Text value = new Text();

        SequenceFile.Writer writer = SequenceFile.createWriter(fileSystem, conf, new Path(OUTPUT_PATH), key.getClass(), value.getClass());


        //写新的数据

        System.out.println(writer.getLength());

        key.set("smallProcess.txt".getBytes());

        value.set("result".getBytes());

        writer.append(key, value);



        //关闭流

        IOUtils.closeStream(writer);

    }




/**
 * SequenceFile 读操作
 */

    public static  void sequenceRead() throws Exception {
//        合并完大文件输出的路径
        final String INPUT_PATH= "/hadoop_test/little_trans_big/big.Seq";


        //获取文件系统
        Configuration conf = new Configuration();



        FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);



        //准备读取seq的流

        Path path = new Path(INPUT_PATH);

        SequenceFile.Reader reader = new SequenceFile.Reader(fileSystem, path, conf);

        //通过seq流获得key和value准备承载数据

        Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);

        Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);

        //循环从流中读取key和value

        long position = reader.getPosition();

        while(reader.next(key, value)){

            //打印当前key value

            System.out.println(key+":"+value);

            //移动游标指向下一个key value

            position=reader.getPosition();

        }

        //关闭流

        IOUtils.closeStream(reader);

    }




/**
 * 多个小文件合并成大seq文件
 *
 * @throws Exception
 */

    public  static void small2Big() throws Exception {

        final String INPUT_PATH = "/hadoop_test/little_file";

        final String OUTPUT_PATH = "/hadoop_test/little_trans_big/big.Seq";

        // 获取文件系统

        Configuration conf = new Configuration();



        FileSystem fs = FileSystem.get(conf);

        // 通过文件系统获取所有要处理的文件，
        // 文件队列输出files[/hadoop_test/day1.txt,/hadoop_test/day2.txt]

        FileStatus[] files = fs.listStatus(new Path(INPUT_PATH));

        // 创建可以输出seq文件的输出流

        Text key = new Text();

        Text value = new Text();

        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, new Path(OUTPUT_PATH), key.getClass(),

                value.getClass());

        // 循环处理每个文件

        for (int i = 0; i < files.length; i++) {

            // key设置为文件名

            key.set(files[i].getPath().getName());

            // 读取文件内容

            InputStream in = fs.open(files[i].getPath());

            byte[] buffer = new byte[(int) files[i].getLen()];

            IOUtils.readFully(in, buffer, 0, buffer.length);

            // 值设置为文件内容

            value.set(buffer);

            // 关闭输入流

            IOUtils.closeStream(in);

            // 将key文件名value文件内容写入seq流中

            writer.append(key, value);

        }



    }
}
