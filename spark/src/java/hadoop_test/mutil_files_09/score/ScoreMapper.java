package hadoop_test.mutil_files_09.score;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import hadoop_test.mutil_files_09.domain.Score;

//split
public class ScoreMapper extends Mapper<LongWritable,Text, Text,Score>{

	//一行一行处理
	@Override
	protected void map(LongWritable key,  Text value,
			Context context)
			throws IOException, InterruptedException {

		//split切片类
		FileSplit split=(FileSplit) context.getInputSplit();
		//split.getPath().getName();获得正在读取的这个split的文件名
		String filename=split.getPath().getName();
//		String filepath=split.getPath().toString();
//		System.out.println(filepath);
		//line===》 1 lisi 3
		String line=value.toString();
		//理解为join 的 key
		String name=line.split(" ")[1];
		//分数
		int score=Integer.parseInt(line.split(" ")[2]);

		Score s=new Score();
		s.setName(name);

//		System.out.println(filename);
		if(filename.equals("chinese.txt")){
			s.setChinese(score);
		}
		if(filename.equals("english.txt")){
			s.setEnglish(score);
		}
		if(filename.equals("math.txt")){
			s.setMath(score);
		}
//		lisi Student [ name=li, chinese=24, english=Null, math=NUll]

		System.out.println(s.toString());
		context.write(new Text(s.getName()), s);
	}

}
