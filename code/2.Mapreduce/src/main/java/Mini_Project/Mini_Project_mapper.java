package Mini_Project;

import org.apache.hadoop.io.Text;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;



public class Mini_Project_mapper<LongWritable> extends Mapper<LongWritable, Text, IntWritable, Text>{
	
	private Text department = new Text();
	private IntWritable k = new IntWritable(1);
	
    

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text,IntWritable,Text >.Context context)
    		throws IOException, InterruptedException {
    	
    	
    	
    		
    		String line = value.toString();
    		String [] strarr = line.split(",");
    	    if(strarr[6].isBlank()) { //department column cannot be blank partitioning column
    	    	context.getCounter(Mini_Project.Mapreduce_job.GarbageCounters.count).increment(1);
    	    }
    	    else{
    	    	context.write(k,value);
    	    	}
    	
    	
    	
    }
				
}