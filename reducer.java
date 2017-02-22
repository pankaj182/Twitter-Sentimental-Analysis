package sentiment_analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class reducer extends Reducer<Text,IntWritable, Text, IntWritable> {

	public void reduce(Text key,Iterable<IntWritable> values,Context context)
	{
		try
		{
			int sum=0;
			for(IntWritable i:values)
			{
//				sum=sum+i.get();
				context.write(key,new IntWritable(i.get()));
				break;
			}
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
		
	}
}
