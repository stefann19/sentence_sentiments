import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class WordCountReducer extends Reducer<Text,IntWritable,Text,JustTheTuple> {
    private IntWritable result = new IntWritable();
    private Map<Text,IntWritable> countMap = new HashMap<Text,IntWritable>();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        countMap.put(key, result);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        Map<Text, IntWritable> sortedMap = MiscUtils.sortByValues(countMap);
        Map<Text,IntWritable> relevantWords = new HashMap<>();

        URI[] stopWordsFiles = context.getCacheFiles();

        HashSet<String> positiveWords = FileHelper.readFile(new Path(stopWordsFiles[1]), context.getConfiguration());
        HashSet<String> negativeWords = FileHelper.readFile(new Path(stopWordsFiles[2]), context.getConfiguration());

        int counter = 0;
        for (Text key : sortedMap.keySet()) {
            if (counter++ == 1000) {
                break;
            }
            relevantWords.put(key, sortedMap.get(key));
        }

        JustTheTuple sum=new JustTheTuple();
        for (Text key: relevantWords.keySet()) {
            sum.setPositiveAppearances(sum.getPositiveAppearances()+getValue(key, positiveWords));
            sum.setNegativeAppearances(sum.getNegativeAppearances()+getValue(key, negativeWords));
        }
        context.write(new Text("Result: "), sum);
    }

    private int getValue(Text word, HashSet<String> words) throws IOException {
        if(words.contains(word.toString()))
        {
            return 1;
        }
        return 0;
    }

}
