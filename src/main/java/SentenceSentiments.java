import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.StringTokenizer;

public class SentenceSentiments extends Configured implements Tool {
    public int run(String[] strings) throws Exception {
        if(strings.length<1) {
            System.out.println("Wrong input or output path..");
            return -1;
        }
        Configuration conf = new Configuration();
        if(strings.length>=4)
            conf.set("extra1", strings[3]);
        Job job = Job.getInstance(conf,"sentence sentiments");
        job.setJarByClass(SentenceSentiments.class);
        job.setMapperClass(WordMapper.class);
        //job.setCombinerClass(WordReducer.class);

        for (String s:
             strings) {
            System.out.println(s + ", ");
        }
        job.setCacheFiles(new URI[]{new Path("neutralWords.txt").toUri()});

        switch(strings[0]){
            case "Words":
                job.setReducerClass(TopWordsReducer.class);
                FileInputFormat.addInputPath(job,new Path("sentences.txt"));
                break;
            case "BookAnalysis":
                job.setReducerClass(WordCountReducer.class);
                job.setMapperClass(WordCountMapper.class);
                FileInputFormat.addInputPath(job,new Path("book.txt"));
                break;
            case "SentenceAnalysis":
                analyzeSentence(strings[3]);
                return 0;
            case "FindNeutralWords":
                PreprocessHelper preprocessHelper = new PreprocessHelper(strings[1],conf );
                preprocessHelper.preprocess();
                System.out.println("Neutral words found");
                return 0;
            case "ExtractWords":
                job.setReducerClass(WordReducer.class);
                break;
        }

        job.setMapOutputValueClass(JustTheTuple.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileSystem fs = FileSystem.get(conf);

        FileOutputFormat.setOutputPath(job,new Path(strings[2]));

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) {
        int exitCode=0;
        try {
            exitCode = ToolRunner.run(new SentenceSentiments(),args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(exitCode);
    }

    private void analyzeSentence(String sentence) {
        StringTokenizer itr = new StringTokenizer(sentence);
        String word;
        int sum=0;
        while (itr.hasMoreTokens()) {
            word = itr.nextToken();
            sum+= getValue(word);
        }
    }

    private void getValue(String word){}

}
