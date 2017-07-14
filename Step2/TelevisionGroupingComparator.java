package Assignment3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TelevisionGroupingComparator extends WritableComparator 
{
	public TelevisionGroupingComparator()
	{
		super(Television.class,true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	/**
	 * This comparator controls which keys are grouped together in a single call 
	 * to reduce method
	 */
	public int compare(WritableComparable wc1,WritableComparable wc2)
	{
		Television t1=(Television) wc1;
		Television t2=(Television) wc2;
		
		String strCompany1 = t1.getCompanyState().toString().split("-")[0];
		String strCompany2 = t2.getCompanyState().toString().split("-")[0];
		return strCompany1.compareTo(strCompany2);
	}
}
