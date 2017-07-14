package Assignment3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Television implements WritableComparable<Television>
{
	private Text companyState;
	private LongWritable units;
	
	
	public Text getCompanyState() 
	{
		return companyState;
	}
	
	public LongWritable getUnits() 
	{
		return units;
	}
	
	
	@Override
	public void readFields(DataInput in) throws IOException 
	{
		companyState = new Text(in.readUTF());
		units = new LongWritable(in.readLong()); //Secondary key
	}
	
	@Override
	public void write(DataOutput out) throws IOException 
	{
		out.writeUTF(companyState.toString());
		out.writeLong(units.get());
		
	}
	@Override
	public int compareTo(Television television) 
	{
	
		int iCompareValue = this.getCompanyState().compareTo(television.getCompanyState());
		if(iCompareValue == 0)
		{
			iCompareValue = this.getUnits().compareTo(television.getUnits());
		}
		return (-1)*iCompareValue;
	}

	public void set(Text companyState, LongWritable units) 
	{
		this.companyState = companyState;
		this.units = units;
	}

	@Override
	public String toString() {
		return "Television[companyState=" + companyState+ "]";
	}	
	
	
}
