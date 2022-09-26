package hadoop_test.kmeans_demo_13;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Util {
    //中心点的个数
    public static final int K=5;
    //读出中心点，注意必须包括键值
    public static List<ArrayList<String>> getCenterFile(String inputPath){

        List<ArrayList<String>> centers=new ArrayList<ArrayList<String>>();
        Configuration conf=new Configuration();
        try{
            FileSystem fs=DataSource.oldCenter.getFileSystem(conf);
            Path path=new Path(inputPath);
            FSDataInputStream fsIn=fs.open(path);
            //一行一行读取参数，存在Text中，再转化为String类型
            Text lineText=new Text();
            String tmpStr=null;
            LineReader linereader=new LineReader(fsIn,conf);
            while(linereader.readLine(lineText)>0){
                ArrayList<String> oneCenter=new ArrayList<>();
                tmpStr=lineText.toString();
                //分裂String，存于容器中
                String[] tmp=tmpStr.split(",");

                for(int i=0;i<tmp.length;i++){
                    oneCenter.add(tmp[i]);
                }
                //将此点加入集合
                centers.add(oneCenter);
            }
            fsIn.close();
        }catch(IOException e){
            e.printStackTrace();
        }
        //返回容器

        return centers;
    }

    //判断是否满足停止条件
    public static boolean isStop(String oldpath,String newpath,int repats,float threshold)
            throws IOException{
        //获得输入输出文件 center_inputlocation
        //[[1,0.5483598064494786,0.5620634207706902,0.631409127357256],
        //[2,0.3097815453182178,0.30933422990614073,0.2494839735711083],
        //[3,0.10216864838289068,0.056547788616962635,0.0982705121497766],
        //[4,0.8526661438398866,0.8877750526698496,0.8710574668862512],
        //5,0.9282123373771547,0.8518260893453832,0.8992078636901775]]

        List<ArrayList<String>> oldcenters= Util.getCenterFile(oldpath);
        List<ArrayList<String>> newcenters= Util.getCenterFile(newpath);

        //累加中心点间的距离
        float distance=0;
        for(int i=0;i<K;i++){
            //计算新旧聚类中心的距离
            for(int j=1;j<oldcenters.get(i).size();j++){
                float tmp=Math.abs(Float.parseFloat(oldcenters.get(i).get(j))
                        -Float.parseFloat(newcenters.get(i).get(j)));
                distance+=Math.pow(tmp,2);
            }
        }
        /*如果超出阈值，则返回false
         * 否则更新中心点文件
         */
        if(distance<threshold || DataSource.REPEAT<repats)
            return true;
        //核心，是将第一轮旧的聚类中心删除，第一轮新的聚类中心替换第一轮旧聚类中心，
        // 作为第二轮的初始聚类中心

        //1、删除旧的聚类中心文件
        Util.deleteLastResult(oldpath);
        //2. 将新的聚类中心文件移到旧的聚类中心文件中去
        Configuration conf=new Configuration();

        Path npath=new Path("/hadoop_test/kmeans/old_path");
        FileSystem fs=npath.getFileSystem(conf);
        //通过local作为中介，从hdfs上拉数据到本地。多余
        fs.moveToLocalFile(new Path(newpath), new Path(
                "/usr/local/src/class/MR/kmeans_temp_file/iter_file/tmp.data"));
        fs.delete(new Path(oldpath), true);//在写入inputpath之前再次确保此文件不存在
        fs.moveFromLocalFile(new Path("/usr/local/src/class/MR/kmeans_temp_file/iter_file/tmp.data")
                ,new Path(oldpath));
        return false;



    }

    //删除上一次mapreduce的结果
    public static void deleteLastResult(String inputpath){
        Configuration conf=new Configuration();
        try{
            Path path=new Path("inputpath");
            FileSystem fs2= path.getFileSystem(conf);
            fs2.delete(new Path(inputpath),true);
        }catch(IOException e){
            e.printStackTrace();
        }
    }


}
