package StockSentimentAnalysis;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.*;
import java.io.*;
import java.util.*;
import java.net.URI;  // 添加这个import

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Set<String> stopWords = new HashSet<>();
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 从HDFS加载停用词表
        loadStopWords(context);
    }
    
    private void loadStopWords(Context context) throws IOException {
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(FileSystem.get(context.getConfiguration())
                            .open(new Path(cacheFiles[0]))))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    stopWords.add(line.trim().toLowerCase());
                }
            }
        }
    }
    
    @Override
    public void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split(",", 2);
        if (parts.length == 2) {
            String text = parts[0].trim();
            String sentiment = parts[1].trim();
            
            // 清洗文本：转小写、去标点、去数字
            String cleanedText = text.toLowerCase()
                    .replaceAll("[^a-zA-Z\\s]", "")
                    .replaceAll("\\d+", "")
                    .trim();
            
            StringTokenizer tokenizer = new StringTokenizer(cleanedText);
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                // 过滤停用词和空词
                if (!stopWords.contains(token) && token.length() > 1) {
                    // 输出格式: sentiment_word, 1
                    word.set(sentiment + "_" + token);
                    context.write(word, one);
                }
            }
        }
    }
}
