import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.URI;
import java.util.*;

public class PositiveWordReducer extends Reducer<Text,JustTheTuple,Text,IntWritable> {
    private Map<Text,IntWritable> countMap = new HashMap<Text,IntWritable>();

    public void reduce(Text key,Iterable<JustTheTuple> values,Context context) throws
            IOException,
            InterruptedException
    {
        JustTheTuple sum = new JustTheTuple();
        for (JustTheTuple val : values) {
            sum.setNegativeAppearances(val.getNegativeAppearances()+sum.getNegativeAppearances());
            sum.setPositiveAppearances(val.getPositiveAppearances()+sum.getPositiveAppearances());
        }
        if(sum.getPositiveAppearances()>sum.getNegativeAppearances()) {
            IntWritable appearences = new IntWritable(sum.getNegativeAppearances() + sum.getPositiveAppearances());
            countMap.put(new Text(key), appearences);
        }


    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        removeNeutralWords(context);

        Map<Text, IntWritable> sortedMap = MiscUtils.sortByValues(countMap);

        int counter = 0;
        for (Text key : sortedMap.keySet()) {
            if (counter++ == 10) {
                break;
            }
            context.write(key, sortedMap.get(key));
        }
    }

    private void removeNeutralWords(Context context) throws IOException {

        URI[] stopWordsFiles = context.getCacheFiles();

        if(stopWordsFiles != null && stopWordsFiles.length > 0) {
            HashSet<String> neutralWords = FileHelper.readFile(new Path(stopWordsFiles[0]),context.getConfiguration());

            assert neutralWords != null;

            List<Text> wordsToDelete= new ArrayList<Text>();

            for (Text key: countMap.keySet()) {
                boolean match = neutralWords.contains(key.toString());
                if (match) {
                    wordsToDelete.add(key);
                }
            }

            for (Text key: wordsToDelete) {
                countMap.remove(key);
            }
        }
    }


    /*private HashSet<String> readFile(Path filePath, Configuration conf) {

        try{
            HashSet<String> neutralWords = new HashSet<>();
            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream inputStream = fs.open(filePath);
            //Classical input stream usage
            String in= IOUtils.toString(inputStream, "UTF-8");
            inputStream.close();

            return new HashSet<String>(Arrays.asList(in.split("\n")));

        } catch(IOException ex) {
            ex.printStackTrace();
            System.err.println("Exception while reading stop words file: " + ex.getMessage());
            return null;
        }

    }*/

}
