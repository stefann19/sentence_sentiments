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

public class NegativeWords extends Configured implements Tool {

    public int run(String[] strings) throws Exception {
        if(strings.length<2) {
            System.out.println("Wrong input or output path..");
            return -1;
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"negative count");
        job.setJarByClass(NegativeWords.class);

        job.setMapperClass(WordMapper.class);
        //job.setCombinerClass(WordReducer.class);
        job.setReducerClass(NegativeWordsReducer.class);

        job.setMapOutputValueClass(JustTheTuple.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileSystem fs = FileSystem.get(conf);

/*
        if(!fs.exists(new Path("sentences.txt"))){
*/      PreprocessHelper preprocessHelper = new PreprocessHelper(strings[0],conf );
        preprocessHelper.preprocess();
/*
        }
*/

        FileInputFormat.addInputPath(job,new Path("sentences1.txt"));
        FileOutputFormat.setOutputPath(job,new Path(strings[1]));
        job.setCacheFiles(new URI[]{new Path("neutralWords1.txt").toUri()});

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) {
        int exitCode=0;
        try {
            exitCode = ToolRunner.run(new NegativeWords(),args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(exitCode);
    }
}
