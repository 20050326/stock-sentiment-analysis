package StockSentimentAnalysis;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.IOException;
import java.util.*;

public class TopWordsReducer extends Reducer<Text, Text, Text, IntWritable> {
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        // 使用TreeMap自动排序
        TreeMap<Integer, List<String>> countToWords = new TreeMap<>(Collections.reverseOrder());
        
        for (Text val : values) {
            String[] parts = val.toString().split("\\t");
            if (parts.length == 2) {
                String word = parts[0];
                int count = Integer.parseInt(parts[1]);
                
                countToWords.computeIfAbsent(count, k -> new ArrayList<>()).add(word);
            }
        }
        
        // 输出前100个高频词，并保留情感标签
        int counter = 0;
        String sentiment = key.toString(); // 获取情感标签 (1 或 -1)
        
        for (Map.Entry<Integer, List<String>> entry : countToWords.entrySet()) {
            for (String word : entry.getValue()) {
                if (counter < 100) {
                    // 输出格式: sentiment_word TAB count
                    context.write(new Text(sentiment + "_" + word), new IntWritable(entry.getKey()));
                    counter++;
                } else {
                    break;
                }
            }
            if (counter >= 100) break;
        }
    }
}
