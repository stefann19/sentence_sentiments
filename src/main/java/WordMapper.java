import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.BreakIterator;

public class WordMapper extends Mapper<LongWritable,Text,Text,JustTheTuple> {
    public void map(LongWritable key,Text value,Context context) throws
                                                                 IOException,
                                                                 InterruptedException {
        String[] split = value.toString().split("\t");

        BreakIterator boundary = BreakIterator.getWordInstance();
        boundary.setText(split[2]);
        int val = Integer.parseInt(split[3]) - 2;
        if (val == 0)
            return;
        int start = boundary.first();
        for (int end = boundary.next(); end != BreakIterator.DONE; start = end, end = boundary.next()) {
            String word = split[2].substring(start, end);
            if (isValidWord(word)) {
                context.write(new Text(word), new JustTheTuple(val < 0 ? 1 : 0, val > 0 ? 1 : 0));
            }
        }
    }

    private boolean isValidWord(String word){
        if(word.length()<=2)return false;
        if(!word.matches("[a-zA-Z]+"))return false;
        if(Character.isUpperCase(word.charAt(0)))return false;
        return true;
    }

}
