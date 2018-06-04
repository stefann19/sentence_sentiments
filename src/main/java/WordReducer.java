import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class WordReducer extends Reducer<Text,JustTheTuple,Text,JustTheTuple> {
    public void reduce(Text key,Iterable<JustTheTuple> values,Context context) throws
                                                                              IOException,
                                                                              InterruptedException
    {
        JustTheTuple sum = new JustTheTuple();
        for (JustTheTuple val : values) {
            sum.setNegativeAppearances(val.getNegativeAppearances()+sum.getNegativeAppearances());
            sum.setPositiveAppearances(val.getPositiveAppearances()+sum.getPositiveAppearances());
        }

        URI[] stopWordsFiles = context.getCacheFiles();

        if(stopWordsFiles != null && stopWordsFiles.length > 0) {
            HashSet<String> neutralWords = FileHelper.readFile(new Path(stopWordsFiles[0]),context.getConfiguration());

            assert neutralWords != null;
/*
                System.out.println(neutralWords);
*/
            boolean match = neutralWords.contains(key.toString());
            if(match){
                context.write(key,sum);
            }
        }
    }

    /*private List<String> readFile(Path filePath, Configuration conf) {

        try{
            List<String> neutralWords = new ArrayList<>();
            *//*BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));

            String neutralWord = null;

            while((neutralWord = bufferedReader.readLine()) != null) {

                neutralWords.add(neutralWord.toLowerCase());

            }*//*

            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream inputStream = fs.open(filePath);
            //Classical input stream usage
            String in= IOUtils.toString(inputStream, "UTF-8");
            inputStream.close();

            return Arrays.asList(in.split("\n"));

        } catch(IOException ex) {
            ex.printStackTrace();
            System.err.println("Exception while reading stop words file: " + ex.getMessage());
            return null;
        }

    }*/

}
