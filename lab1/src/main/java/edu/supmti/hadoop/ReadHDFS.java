package edu.supmti.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class ReadHDFS {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: ReadHDFS <hdfs_file_path>");
            System.exit(1);
        }

        String hdfsPath = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(hdfsPath);

        if (!fs.exists(path)) {
            System.out.println("File not found: " + hdfsPath);
            System.exit(1);
        }

        try (FSDataInputStream inStream = fs.open(path);
                BufferedReader br = new BufferedReader(new InputStreamReader(inStream))) {
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
            // Ne PAS faire System.out.println(line) ici → line == null !
        }

        fs.close();
    }
}