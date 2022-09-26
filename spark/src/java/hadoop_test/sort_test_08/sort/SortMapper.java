package hadoop_test.sort_test_08.sort;

import hadoop_test.sort_test_08.domain.Movie;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text,Movie,NullWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
//		iid  hot
		Movie movie=new Movie();
		String line=value.toString();
		String name=line.split(" ")[0];
		int hot=Integer.parseInt(line.split(" ")[1]);
		movie.setName(name);
		movie.setHot(hot);
//		movie类作为key
//		context.write( movie, NullWritable.get());
//		context.write( new Text("sort"), movie);
	}

}
