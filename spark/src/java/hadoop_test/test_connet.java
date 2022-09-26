package hadoop_test;
import org.apache.hadoop.conf.Configuration;

import java.net.Inet4Address;
import java.util.*;

public class test_connet {
    public static void main(String[] args) throws Exception {

        Random r = new Random();
        //skip_step 10000:
        int rand=r.nextInt(100);
        System.out.println(rand);

        HashMap<String, Integer> map = new HashMap<String, Integer>();
        map.put("d", 2);
        map.put("c", 1);
        map.put("b", 4);
        map.put("a", 3);
        List<Map.Entry<String, Integer>> infoIds = new ArrayList<Map.Entry<String, Integer>>(map.entrySet());

        // 对HashMap中的 value 进行排序
        Collections.sort(infoIds, new Comparator<Map.Entry<String, Integer>>() {
            public int compare(Map.Entry<String, Integer> o1,
                               Map.Entry<String, Integer> o2) {
                return (o2.getValue()).compareTo(o1.getValue());
            }
        });


//
//        // 对HashMap中的 value 进行排序后  显示排序结果
//        for (int i = 0; i < infoIds.size(); i++) {
//            String id = infoIds.get(i).toString();
//            System.out.println(id + "  ");
//        }
////          String hs="38.733889,116.119068";
//          int s=hs.hashCode()%1000;
//            Random r = new Random();
//            int s1 = r.nextInt(20);
//             System.out.println(s1%10);
//        Configuration conf =new Configuration();
//        String pathString = "/hadoop_test/word_count/acticle.txt";
//        Utils_hadoop.readFile(conf,pathString);

    }



}