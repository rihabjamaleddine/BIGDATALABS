package edu.ismagi.hadoop.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
   private IntWritable result = new IntWritable();

   public IntSumReducer() {
   }

   public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
      int sum = 0;

      IntWritable val;
      for(Iterator var5 = values.iterator(); var5.hasNext(); sum += val.get()) {
         val = (IntWritable)var5.next();
      }

      this.result.set(sum);
      context.write(key, this.result);
   }
}
