package Assignment3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TelevisionPartitioner extends Partitioner<Television,LongWritable>
{

	@Override
	public int getPartition(Television television, LongWritable units, int iNumOfPartitions) 
	{
		String strCompanyName = television.getCompanyState().toString().split("-")[0];
		if(strCompanyName.equalsIgnoreCase("Samsung"))
		{
			System.out.println(strCompanyName+" to partition 0");
			return 0;
		}
		else if(strCompanyName.equalsIgnoreCase("Onida"))
		{
			System.out.println(strCompanyName+" to partition 1");
			return 1;
		}
		else if(strCompanyName.equalsIgnoreCase("Akai"))
		{
			System.out.println(strCompanyName+" to partition 2");
			return 2;
		}
		else if(strCompanyName.equalsIgnoreCase("Lava"))
		{
			System.out.println(strCompanyName+" to partition 3");
			return 3;
		}
		
		System.out.println(strCompanyName+" to partition 4");
		return 4;
	}
}
