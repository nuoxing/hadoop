package com.swy;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * 统计访问量最大的ip
 * @author sjakl
 *
 */
public class MyReduce3 extends Reducer<Text, Text, Text, IntWritable> {

	    private int max = 0;
	
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
               Text t = null;
               Text m = null;
               IntWritable i = null;
               System.out.println(key);
		       for (Text val : values) {
		    	  StringTokenizer itr = new StringTokenizer(val.toString());
		    	
			        if (itr.hasMoreTokens()) {
			        	 t = new Text(itr.nextToken());
			        }
			        
			        if (itr.hasMoreTokens()) {
			        	 i = new IntWritable(Integer.valueOf(itr.nextToken()));
			        } 
			        if(max<i.get()){
			        	max = i.get();
			        	m = t;
			        	System.out.println(max);
			        }
		    	  
		      }
		    
		      context.write(m, new IntWritable(max));
		    }

	
}
