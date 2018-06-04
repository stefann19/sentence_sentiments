import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
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
import java.util.*;

public class WordReducer extends Reducer<Text,JustTheTuple,Text,JustTheTuple> {
    private List<String> positiveWords= new ArrayList<>();
    private List<String> negativeWords= new ArrayList<>();
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

            boolean match = neutralWords.contains(key.toString());
            if(match){
                if(sum.getNegativeAppearances()>sum.getPositiveAppearances()){
                    negativeWords.add(key.toString());
                }else{
                    positiveWords.add(key.toString());
                }
                context.write(key,sum);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
         PrintToFiles("positiveWords.txt",context.getConfiguration(),positiveWords);
         PrintToFiles("negativeWords.txt",context.getConfiguration(),negativeWords);

    }


    void PrintToFiles(String path,Configuration conf,List<String> collection) throws
                                          IOException
    {



        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream sentencesFileStream = fs.create(new Path(path));
        sentencesFileStream.write(String.join("\n",collection).getBytes());
        sentencesFileStream.flush();
        sentencesFileStream.hsync();
        sentencesFileStream.close();
    }



}
