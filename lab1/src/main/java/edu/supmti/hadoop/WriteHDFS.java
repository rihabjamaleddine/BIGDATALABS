package edu.supmti.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class WriteHDFS {
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("Usage: WriteHDFS <hdfs_output_path> <text_to_write>");
            System.exit(1);
        }

        String outputPath = args[0];
        String text = args[1];

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(outputPath);

        // Vérifier que le fichier n'existe pas déjà
        if (fs.exists(path)) {
            System.out.println("File already exists: " + outputPath);
            System.exit(1);
        }

        // Créer le fichier et écrire le texte
        try (FSDataOutputStream out = fs.create(path)) {
            out.writeUTF(text);
        }

        System.out.println("✅ File written successfully: " + outputPath);
        fs.close();
    }
}