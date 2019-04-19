package com.test;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.swy.MyMap;
import com.swy.MyMapper2;
import com.swy.MyReduce2;

/**
 * 统计访问量最大的的ip和访问值
 * @author sjakl
 *
 */
public class MyTest2 {

	public static void main(String[] args) {
		try {
			//可以不写 指定hadoop安装目录
		    //System.setProperty("hadoop.home.dir", "D:\\hadoop-2.6.5");
			//输入路径
	        String dst = "F:/key.txt";
	        //输出路径，必须是不存在的，空文件加也不行。
	        String dstOut = "F:/a2";
	        Configuration hadoopConfig = new Configuration();///加载配置文件
	        Job job = Job.getInstance(hadoopConfig);//创建一个job,供JobTracker使用
	        
	        //如果需要打成jar运行，需要下面这句
	        //job.setJarByClass(NewMaxTemperature.class);

	        //job执行作业时输入和输出文件的路径
	        FileInputFormat.addInputPath(job, new Path(dst));
	        FileOutputFormat.setOutputPath(job, new Path(dstOut));
	        //设置为几 输出文件就有几个
	       // job.setNumReduceTasks(4);

	        //指定自  定义的Mapper和Reducer作为两个阶段的任务处理类
	        job.setMapperClass(MyMap.class);
	        job.setReducerClass(MyReduce2.class);
	        
	        //设置最后输出结果的Key和Value的类型
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(IntWritable.class);
	        
	        //执行job，直到完成
	        job.waitForCompletion(true);
	        System.out.println("Finished");
		} catch (Exception e) {
			e.printStackTrace();
		}
	
		
	}

	
		
		

}
