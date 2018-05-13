import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class WordReducer extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable> {
    public void reduce(Text key, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector,
                       Reporter reporter) throws
                                          IOException
    {
       /* AtomicInteger count = new AtomicInteger();
        iterator.forEachRemaining(va-> count.addAndGet(va.get()));*/
        int count =0;
        while (iterator.hasNext()){
            IntWritable i = iterator.next();
            count+=i.get();
        }
        outputCollector.collect(key,new IntWritable(count));
    }
}
