import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class WordMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable longWritable, Text text, OutputCollector<Text, IntWritable> outputCollector,
                    Reporter reporter) throws
                                       IOException
    {
        String s = text.toString();
        for (String word : s.split(" ")) {
            outputCollector.collect(new Text(word),new IntWritable(1));
        }
    }
}
