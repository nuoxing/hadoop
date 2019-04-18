package com.demo3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
 * 3�����ÿ�ſγ̲ο�ѧ���ɼ���ߵ�ѧ������Ϣ���γ̣�������ƽ����
 * @author Administrator
 *
 */
public class CourseScoreMR3{ 
    
    private static final int TOPN = 3;

    public static void main(String[] args) throws Exception {
        
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Job job = Job.getInstance(conf);
        
        
        job.setJarByClass(CourseScoreMR3.class);
        job.setMapperClass(CourseScoreMR2Mapper.class);
        job.setReducerClass(CourseScoreMR2Reducer.class);
        
        job.setMapOutputKeyClass(CourseScore.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(CourseScore.class);
        job.setOutputValueClass(NullWritable.class);
        
        
//        job.setPartitionerClass(CSPartitioner.class);
//        job.setNumReduceTasks(4);
        
        
        // ָ���������
        job.setGroupingComparatorClass(CourseScoreGC.class);
        
        
        Path inputPath = new Path("E:\\bigdata\\cs\\input");
        Path outputPath = new Path("E:\\bigdata\\cs\\output_3_last");
        FileInputFormat.setInputPaths(job, inputPath);
        if(fs.exists(outputPath)){
            fs.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);
        
        
        boolean isDone = job.waitForCompletion(true);
        System.exit(isDone ? 0 : 1);
    }
    
    public static class CourseScoreMR2Mapper extends Mapper<LongWritable, Text, CourseScore, NullWritable>{
        
        CourseScore cs = new CourseScore();
        
        /**
         * value =  math,huangxiaoming,85,75,85,99,66,88,75,91
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
            String[] split = value.toString().split(",");
            
            String course = split[0];
            String name = split[1];
            
            int sum = 0;
            int count = 0;
            
            for(int i = 2; i<split.length; i++){
                int tempScore = Integer.parseInt(split[i]);
                sum += tempScore;
                
                count++;
            }
            
            double avgScore = 1D * sum / count;
            
            cs.setCourse(course);
            cs.setName(name);
            cs.setScore(avgScore);
            
            context.write(cs, NullWritable.get());
        }
        
    }
    
    public static class CourseScoreMR2Reducer extends Reducer<CourseScore, NullWritable, CourseScore, NullWritable>{
        
        int count = 0;
        
        /**
         * reducer�׶ε�reduce�����ĵ��ò�����key��ͬ�Ķ�һ��key-vlaue
         * 
         * redcuer�׶Σ�ÿ������һ����ͬ��key��key_value�飬 ��ôreduce�����ͻᱻ����һ�Ρ�
         * 
         * 
         * values���������ֻ�ܵ���һ�Ρ�
         * values�������ڵ����Ĺ����е���������value��䣬ͬʱ�����value����Ӧ��keyҲ����ű�         ����
         * 
         */
        @Override
        protected void reduce(CourseScore key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            
            
            int count = 0;
            
            for(NullWritable nvl : values){
                System.out.println("*********************************** " + (++count) + "      " + key.toString());
                
                if(count == 3){
                    return;
                }
            }
            
            
            // ԭ�����
            /*for(NullWritable nvl :  values){
                context.write(key, nvl);
            }*/
            
            
            // ���ÿ�ſγ̵���߷���   ,  Ԥ�ڽ���У�key����ʾ����һ����
//            for(NullWritable nvl : values){
//                System.out.println(key + " - " nvl);
//                
//                valueList.add(nvl);
//            }
            
//            List<Value> valueList = null;
            // Ԥ�ڽ���У�key����ʾ����һ����
            /*int count = 0;
            for(NullWritable nvl : values){
                count++;
            }
            for(int i = 0; i<count; i++){
            valueList.get(i) = value
                System.out.println(key + " - "+ value);
            }*/
            
            
//            math hello 1
//            math hi  2
        }
    }
}