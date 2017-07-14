package Assignment3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TelevisionReducer extends Reducer<Television, LongWritable, Television, LongWritable>
{
	@Override
	public void reduce(Television key,Iterable<LongWritable> values,Context context) throws IOException, InterruptedException
	{
		int iEndCount = 3;
		Iterator<LongWritable> iterator = values.iterator();
		while(iterator.hasNext() && iEndCount>0)	
		{
			context.write(key, iterator.next());
			iEndCount--;
		}
	}
}
