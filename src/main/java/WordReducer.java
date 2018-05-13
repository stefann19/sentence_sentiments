import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    public void reduce(Text key,Iterable<IntWritable> values,Context context) throws
                                                                              IOException,
                                                                              InterruptedException
    {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }if(sum>24||sum<-24)
        context.write(key,new IntWritable(sum));
    }

}
