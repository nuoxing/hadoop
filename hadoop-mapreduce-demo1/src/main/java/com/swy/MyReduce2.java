package com.swy;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * 统计访问量最大的ip
 * @author sjakl
 *
 */
public class MyReduce2 extends Reducer<Text, IntWritable, Text, IntWritable> {

	  private int max = 0;
	
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

		      for (IntWritable val : values) {
		        
		    	  int n =Integer.valueOf(val.toString());
		    	  if(n>max){
		    		  max  = n;
		    		  context.write(key,  new IntWritable(max));
		    	  }
		      }
		    
		      context.write(null, null);
		    }

	
}
