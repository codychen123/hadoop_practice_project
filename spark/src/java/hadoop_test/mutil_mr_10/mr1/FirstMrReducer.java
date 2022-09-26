package hadoop_test.mutil_mr_10.mr1;

import hadoop_test.mutil_mr_10.company.DoubleMr;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FirstMrReducer extends Reducer<Text, DoubleMr,DoubleMr,NullWritable > {
	
	@Override
	protected void reduce(Text key, Iterable<DoubleMr> values,
                          Context context) throws IOException, InterruptedException {
		
		DoubleMr tmp=new DoubleMr();
		tmp.setName(key.toString());
		for(DoubleMr mr:values){
			tmp.setProfit(tmp.getProfit()+mr.getProfit());
		}
		context.write(tmp, NullWritable.get());
	}

}
