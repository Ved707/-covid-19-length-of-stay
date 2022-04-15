package Mini_Project;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Mini_Project_Reducer extends Reducer<IntWritable, Text,NullWritable, Text >{
	
	private MultipleOutputs<NullWritable,Text> multipleOutputs;
	public void setup(Context context) throws IOException, InterruptedException
	{
		multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
	}
	
	@Override
	protected void reduce(IntWritable key, Iterable<Text> values,
		Reducer<IntWritable, Text, NullWritable,Text>.Context context) throws IOException, InterruptedException {
		
		Iterator<Text> val = values.iterator();
	while(val.hasNext()) {
		multipleOutputs.write(NullWritable.get(), val.next(),key.toString());
	}
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException
	{
	      multipleOutputs.close();
	}
}
