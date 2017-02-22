package sentiment_analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class driver extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
        
		Job job = new Job(conf, "Read a File");

		//Job job=Job.getInstance();
		job.setJobName("Sentiment Analysis");
		job.setJarByClass(getClass());
		
		job.setMapperClass(mapper.class);
		job.setReducerClass(reducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job,new Path(arg0[1]));
		
		job.setJarByClass(driver.class);     
        
		
		return job.waitForCompletion(true)?1:0;
	}

	public static void main(String[] args) {
		try
		{
			driver drive=new driver();
			ToolRunner.run(drive.getConf(),drive,args);
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}

	}

}
