import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class PreprocessHelper {
    private Configuration conf;
    private String filepath;
    private String sentencesFile;
    private String neutralWordsFile;
    private List<String> sentences;
    private List<String> neutralWords;


    public PreprocessHelper(String path,Configuration conf) {
        this.conf = conf;

        filepath = path;
        sentencesFile=filepath+"/../Sentences.txt";
        neutralWordsFile=filepath+"/../NeutralWords.txt";
        sentences = new ArrayList<String>();
        neutralWords = new ArrayList<String>();
    }

    public String getFilepath() {
        return filepath;
    }

    public void setFilepath(String filepath) {
        this.filepath = filepath;
    }

    public void preprocess(){
        try {
            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream inputStream = fs.open(new Path(filepath));
            //Classical input stream usage
            String in= IOUtils.toString(inputStream, "UTF-8");
            inputStream.close();

            String[] arr =in.split("\n");
            System.out.println(arr.length + "HEEELP");
            Stream<String> stream = Arrays.stream(arr);

            stream.forEach(this::processLine);




          /*  Files.write(Paths.get(sentencesFile),sentences, new StandardOpenOption[]{StandardOpenOption.CREATE_NEW});
            Files.write(Paths.get(neutralWordsFile),neutralWords, new StandardOpenOption[]{StandardOpenOption.CREATE_NEW});
*/
            FSDataOutputStream sentencesFileStream = fs.create(new Path("sentences1.txt"));
            sentencesFileStream.write(String.join("\n",sentences).getBytes());
            sentencesFileStream.flush();
            sentencesFileStream.hsync();
            sentencesFileStream.close();


            FSDataOutputStream neutralWordsFileStream = fs.create(new Path("neutralWords1.txt"));
            neutralWordsFileStream.write(String.join("\n",neutralWords).getBytes());
            neutralWordsFileStream.flush();
            neutralWordsFileStream.hsync();
            neutralWordsFileStream.close();
            fs.close();


        } catch (IOException e) {
            e.printStackTrace();

        }
    }

    private void processLine(String line){
        String[] parts=line.split("\t");
/*
        System.out.println(parts.length +" ... parts");
*/
        if(parts.length!=4)return;
        if(parts[2].endsWith(".") && Character.isUpperCase(parts[2].charAt(0))){
            if(!parts[2].contains("not") && !parts[2].contains("but")&&!parts[2].contains("Not") && !parts[2].contains("But")) {
                sentences.add(line);
                return;
            }
        }
        String[] secondSplit =parts[2].split(" ");
        if (secondSplit.length==1 && "123".contains(parts[3])){
            neutralWords.add(parts[2]);
        }

    }


}
