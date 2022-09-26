package hadoop_test.homework;

import hadoop_test.Utils_hadoop;
import org.apache.hadoop.conf.Configuration;

import java.text.SimpleDateFormat;
import java.util.Date;

public class test_date {
    public static void main(String[] args) throws Exception {
        Configuration conf =new Configuration();
        String pathString = "/hadoop_test/homeWork/ua.base";
        Utils_hadoop.readFile(conf,pathString);
        System.out.println(stampToDate(String.valueOf(875502597*1000)));
        System.out.println(stampToDate(String.valueOf(888692465*1000)));

    }
    public static String stampToDate(String s){
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long lt = new Long(s);
        Date date = new Date(lt);
        res = simpleDateFormat.format(date).split(" ")[0];
        return res;
    }
}
