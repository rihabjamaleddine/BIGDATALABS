package edu.supmti.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HadoopFileStatus {
    public static void main(String[] args) {
        // التحقق من عدد المُعطيات
        if (args.length < 3) {
            System.err.println("Usage: HadoopFileStatus <parent_path> <filename> <newname>");
            System.exit(1);
        }

        String parentPath = args[0];
        String fileName = args[1];
        String newName = args[2];

        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(conf);
            Path filepath = new Path(parentPath, fileName);

            if (!fs.exists(filepath)) {
                System.out.println("File does not exist: " + filepath);
                System.exit(1);
            }

            FileStatus status = fs.getFileStatus(filepath);
            System.out.println("File Name: " + status.getPath().getName());
            System.out.println("File Size: " + status.getLen() + " bytes");
            System.out.println("File Owner: " + status.getOwner());
            System.out.println("File Permission: " + status.getPermission());
            System.out.println("Replication: " + status.getReplication());
            System.out.println("Block Size: " + status.getBlockSize());

            BlockLocation[] blockLocations = fs.getFileBlockLocations(status, 0, status.getLen());
            for (BlockLocation blockLocation : blockLocations) {
                System.out.println("Block offset: " + blockLocation.getOffset());
                System.out.println("Block length: " + blockLocation.getLength());
                System.out.print("Block hosts: ");
                for (String host : blockLocation.getHosts()) {
                    System.out.print(host + " ");
                }
                System.out.println();
            }

            Path newPath = new Path(parentPath, newName);
            fs.rename(filepath, newPath);
            System.out.println("File renamed to: " + newName);

            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}