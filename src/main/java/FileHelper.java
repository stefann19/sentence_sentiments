import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

public class FileHelper {
    public static HashSet<String> readFile(Path filePath, Configuration conf) {

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

    }
}
