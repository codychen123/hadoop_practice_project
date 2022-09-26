package hadoop_test.kmeans_demo_13;

import org.apache.hadoop.fs.Path;

public class DataSource {
    public static final int K=5;//聚类的类别数为5
    public static final String inputlocation="/hadoop_test/kmeans/user_info.txt";
    public static final String old_center="/hadoop_test/kmeans/old_path";
    public static final String new_center="/hadoop_test/kmeans/new_path";

    public static final int REPEAT=20;
    public static final float threshold=(float)0.1;
    public static Path inputpath=new Path(inputlocation);
    public static Path oldCenter=new Path(old_center);
    public static Path newCenter=new Path(new_center);

    public static int feat_num=3;
}

