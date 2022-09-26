package hadoop_test.homework2.newsid_hot2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountClickCombiner extends Reducer<Text,LongWritable,Text,LongWritable>{

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		long result=0;

		for(LongWritable value:values){
			result=result+value.get();
		}

		context.write(key, new LongWritable(result));
	}
}
