import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    public void map(LongWritable key,Text value,Context context) throws
                                                                 IOException,
                                                                 InterruptedException
    {
        String s = value.toString();
        for (String line : s.split("\n")) {
            String[] split = line.split("\t");
            int val = Integer.parseInt(split[1]);
            if(val==0)val =-1;
            for(String word:split[0].split("\\s+")){
                context.write(new Text(word),new IntWritable(val));
            }
        }
    }
}
