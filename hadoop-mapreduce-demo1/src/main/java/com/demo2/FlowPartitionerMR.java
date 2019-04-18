package com.demo2;


import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.common.collect.Maps;

/**
 * 3、 将流量汇总统计结果按照手机归属地不同省份输出到不同文件中
 * @author Administrator
 *
 */
public class FlowPartitionerMR {

    public static void main(String[] args) throws Exception {
        
        
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Job job = Job.getInstance(conf, "FlowSumMR");
        job.setJarByClass(FlowPartitionerMR.class);
        
        job.setMapperClass(FlowPartitionerMRMapper.class);
        job.setReducerClass(FlowPartitionerMRReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        
        /**
         * 非常重要的两句代码
         */
        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(10);
        
        
        FileInputFormat.setInputPaths(job, new Path("E:\\bigdata\\flow\\input"));
        Path outputPath = new Path("E:\\bigdata\\flow\\output_ptn2");
        if(fs.exists(outputPath)){
            fs.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);
        
        
        boolean isDone = job.waitForCompletion(true);
        System.exit(isDone ? 0 : 1);
    }
    
    public static class FlowPartitionerMRMapper extends Mapper<LongWritable, Text, Text, Text>{
        
        /**
         * value  =  13502468823    101663100    1529437140    1631100240
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
            
            String[] split = value.toString().split("\t");
            
            String outkey = split[1];
            String outValue = split[8] + "\t" + split[9];
            
            context.write(new Text(outkey), new Text(outValue));
            
        }
    }
    
    public static class FlowPartitionerMRReducer extends Reducer<Text, Text, Text, Text>{
        
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
            int upFlow = 0;
            int downFlow = 0;
            int sumFlow = 0;
            
            for(Text t : values){
                String[] split = t.toString().split("\t");
                
                int upTempFlow = Integer.parseInt(split[0]);
                int downTempFlow = Integer.parseInt(split[1]);
                
                upFlow+=upTempFlow;
                downFlow +=  downTempFlow;
            }
            
            sumFlow = upFlow + downFlow;
            
            context.write(key, new Text(upFlow + "\t" + downFlow + "\t" + sumFlow));
        }
    }
    
    
    public static class ProvincePartitioner extends Partitioner<FlowBean, NullWritable> {

    	private static Map<String, Integer> provinceMap = Maps.newHashMap();

    	static {
    		provinceMap.put("134", 0);
    		provinceMap.put("135", 1);
    		provinceMap.put("136", 2);
    		provinceMap.put("137", 3);
    		provinceMap.put("138", 4);
    		provinceMap.put("139", 5);
    	}

    	@Override
    	public int getPartition(FlowBean key, NullWritable value, int numPartitions) {
    		Integer code = provinceMap.get(key.getPhone().substring(0, 3));
    		if (code != null) {
    			return code;
    		}
    		return 6;
    	}

    }

}