package sentiment_analysis;

import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.util.*;
import java.net.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	ArrayList<String> positive = new ArrayList<String>();
	ArrayList<String> negative = new ArrayList<String>();

	public void setup(Context context) throws IOException{
		Path pt=new Path("hdfs://localhost:9000/positive.txt");//Location of file in HDFS
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		String line;
		line=br.readLine();
		while (line != null){
			String []x = line.split("\t");
			for(String each : x){
				if(!"".equals(each)){
					positive.add(line);
				}
			}
			line=br.readLine();
		}

		Path pt1=new Path("hdfs://localhost:9000/negative.txt");//Location of file in HDFS
		FileSystem fs1 = FileSystem.get(new Configuration());
		BufferedReader br1=new BufferedReader(new InputStreamReader(fs1.open(pt1)));
		String line1;
		line1=br1.readLine();
		while (line1 != null){
			String []x = line1.split("\t");
			for(String each : x){
				if(!"".equals(each)){
					negative.add(line1);
				}
			}
			line1=br1.readLine();
		}
	}

	public void map(LongWritable key, Text value, Context context) {
		try {

			String s =value.toString(), temp = "";
			StringTokenizer t = new StringTokenizer(s);
			int den = 0, num = 0;
			boolean f=false;
			while(t.hasMoreTokens())
			{
				temp=t.nextToken().toString().toLowerCase();
				if(temp.equals(' '))
					continue;
				f=false;
				for (String str : positive) {
					if(str.equals(temp)){
						num++;
						den++;
						f=true;
						break;
					}
				}
				if(f==false)
				{
					for (String str : negative) {
						if(str.equals(temp)){
							num--;
							den++;
							break;
						}
					}
				}
				int x=num*100;
				x=x/den;
				if(den!=0)
					context.write(value, new IntWritable(x));
				else
					context.write(value, new IntWritable(0));
			}
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
}
