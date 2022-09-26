package hadoop_test.homework2.newsid_hot2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class ClusterSortReducer extends Reducer<Text,Text,Text,Text>{
	HashMap<String, Integer> maps = new HashMap<String, Integer>();
	private int topn=20;
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {

		for (Text v:values
			 ) {
			String[] iid_sum =v.toString().split(":");
			maps.put(iid_sum[0],Integer.parseInt(iid_sum[1]));
		}
		List<Map.Entry<String, Integer>> infoIds = new ArrayList<Map.Entry<String, Integer>>(maps.entrySet());

		Collections.sort(infoIds, new Comparator<Map.Entry<String, Integer>>() {
			public int compare(Map.Entry<String, Integer> o2,
							   Map.Entry<String, Integer> o1) {
				return (o1.getValue()).toString().compareTo(o2.getValue().toString());
			}
		});
		int flag=0;
		String re="";
		for (int i = 0; i < infoIds.size(); i++) {
			String iid = infoIds.get(i).getKey();
			String  value = infoIds.get(i).getValue().toString();
			if(flag<topn) {
				re += iid + ":" + value + ",";
				flag+=1;
			}
		}
		 String out_value = re.substring(0,re.length()-1);

		 context.write(key,new Text(out_value));
	}
}
