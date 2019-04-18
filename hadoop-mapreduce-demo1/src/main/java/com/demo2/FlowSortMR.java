package com.demo2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 需求： 第二个题目，就是对第一个题目的结果数据，进行按照总流量倒叙排序（排序就需要自定义key）
 * 
 * 
 */
public class FlowSortMR {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "FlowSumMR");
		job.setJarByClass(FlowSortMR.class);

		job.setMapperClass(FlowSortMRMapper.class);
		job.setReducerClass(FlowSortMRReducer.class);

		job.setOutputKeyClass(FlowBean.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path("E:/bigdata/flow/output_sum"));
		FileOutputFormat.setOutputPath(job, new Path("E:/bigdata/flow/output_sort_777"));

		boolean isDone = job.waitForCompletion(true);
		System.exit(isDone ? 0 : 1);

	}

	public static class FlowSortMRMapper extends Mapper<LongWritable, Text, FlowBean, NullWritable> {

		/**
		 * value = 13602846565 26860680 40332600 67193280
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] split = value.toString().split("\t");

			FlowBean fb = new FlowBean(split[0], Long.parseLong(split[1]), Long.parseLong(split[2]));

			context.write(fb, NullWritable.get());
		}

	}

	public static class FlowSortMRReducer extends Reducer<FlowBean, NullWritable, FlowBean, NullWritable> {

		@Override
		protected void reduce(FlowBean key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {

			for (NullWritable nvl : values) {
				context.write(key, nvl);
			}

		}

	}
}