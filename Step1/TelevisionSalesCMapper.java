package Step1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * @author abhilasha
 * Input key is the offset of line in file
 * Input value format : CompanyName|ProductName|SizeInInches|State|PinCOde|Prize
 * Output key is the the name of the company-state
 * Output value is 1
 */
public class TelevisionSalesCMapper extends Mapper<LongWritable,Text,Text,LongWritable>
{
	private final static String DELIMITER = "\\|";
	private final static String NA = "NA";
	private final static LongWritable ONE = new LongWritable(1);
	Text companyState ;
	
	@Override
	public void setup(Context context)
	{
		companyState  = new Text();
	}
	
	@Override
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
	{
		String strValue  = value.toString();
		System.out.println("Current value is "+strValue);
		
		String[] aSplit  = strValue.split(DELIMITER);
		String strCompanyName = aSplit[0].trim();
		String strProductName  = aSplit[1].trim();
		String strStateName  = aSplit[3].trim();
		
		//Check if CompanyName or Product Name is 'NA'
		if(!strCompanyName.equalsIgnoreCase(NA)  && !strProductName.equalsIgnoreCase(NA))
		{
			companyState.set(strCompanyName+"-"+strStateName);
			context.write(companyState,ONE);
			System.out.println("Putting key : "+strCompanyName+" and value : 1 in context");
		}
	}
}
