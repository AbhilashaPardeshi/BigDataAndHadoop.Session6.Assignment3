package Assignment3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TelevisionMapper extends Mapper<Text,LongWritable,Television,LongWritable> 
{
	Television television;
	
	@Override
	public void setup(Context context)
	{
		television = new Television();
	}

	@Override
	public void map(Text key,LongWritable value,Context context) throws IOException, InterruptedException
	{
		television.set(key,value);
		System.out.println("Current key : "+television.toString() +" , value : "+value.get());
		context.write(television, value);
	}

}
