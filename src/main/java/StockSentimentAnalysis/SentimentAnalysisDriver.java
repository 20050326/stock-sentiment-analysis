package StockSentimentAnalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SentimentAnalysisDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: SentimentAnalysisDriver <input> <stopwords> <output>");
            System.exit(1);
        }
        
        Configuration conf = new Configuration();
        
        // 第一个Job：词频统计
        Job job1 = Job.getInstance(conf, "word count by sentiment");
        job1.setJarByClass(SentimentAnalysisDriver.class);
        
        job1.setMapperClass(WordCountMapper.class);
        job1.setCombinerClass(WordCountReducer.class);
        job1.setReducerClass(WordCountReducer.class);
        
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        
        // 添加停用词文件到分布式缓存
        job1.addCacheFile(new Path(args[1]).toUri());
        
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2] + "_temp"));
        
        boolean success = job1.waitForCompletion(true);
        
        if (success) {
            // 第二个Job：排序并取前100
            Job job2 = Job.getInstance(conf, "top words by sentiment");
            job2.setJarByClass(SentimentAnalysisDriver.class);
            
            job2.setMapperClass(TopWordsMapper.class);
            job2.setReducerClass(TopWordsReducer.class);
            
            // 修正：设置Mapper的输出类型
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            
            // 设置Reducer的输出类型
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(IntWritable.class);
            
            FileInputFormat.addInputPath(job2, new Path(args[2] + "_temp"));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));
            
            success = job2.waitForCompletion(true);
            
            // 清理临时目录
            if (success) {
                FileSystem.get(conf).delete(new Path(args[2] + "_temp"), true);
            }
        }
        
        System.exit(success ? 0 : 1);
    }
}
