package StockSentimentAnalysis;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.IOException;

public class TopWordsMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text sentimentKey = new Text();
    private Text wordCount = new Text();
    
    @Override
    public void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        String[] parts = value.toString().split("\\t");
        if (parts.length == 2) {
            String[] keyParts = parts[0].split("_", 2);
            if (keyParts.length == 2) {
                String sentiment = keyParts[0];
                String word = keyParts[1];
                String count = parts[1];
                
                sentimentKey.set(sentiment);
                wordCount.set(word + "\t" + count);
                context.write(sentimentKey, wordCount);
            }
        }
    }
}
