package hadoop_test.mutil_files_09.score;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import hadoop_test.mutil_files_09.domain.Score;

public class ScoreReducer extends Reducer<Text, Score, Text, Score>{

	@Override
	protected void reduce(Text key, Iterable<Score> values, 
			Context context)
			throws IOException, InterruptedException {

		//new 一个tmp方法
		Score tmp=new Score();
		tmp.setName(key.toString());

		for(Score value:values){
			tmp.setChinese(tmp.getChinese()+value.getChinese());
			tmp.setEnglish(tmp.getEnglish()+value.getEnglish());
			tmp.setMath(tmp.getMath()+value.getMath());
		}
//		System.out.println(key+":"+tmp);

		context.write(key, tmp);
	}

}
