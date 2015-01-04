package heigvd.bda.labs.utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PROutputFormat extends FileOutputFormat<IntWritable, NodePR> {

   static class PRRecordWriter extends RecordWriter<IntWritable, NodePR> {

      BufferedWriter writer;
      
      public PRRecordWriter(String path) throws IOException, URISyntaxException {
         super();
         this.writer = new BufferedWriter(new FileWriter(path), 4096);
      }

      @Override
      public void write(IntWritable key, NodePR value) throws IOException, InterruptedException {
         this.writer.write(String.format("%s\t%s", key, value));
         this.writer.newLine();
      }

      @Override
      public void close(TaskAttemptContext context) throws IOException, InterruptedException {
         this.writer.close();
      }
      
   }
   
   @Override
   public RecordWriter<IntWritable, NodePR> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
      try {
         String outputFilename = FileOutputFormat.getUniqueFile(job, FileOutputFormat.getOutputName(job), ".txt");
         return new PRRecordWriter(String.format("%s/%s", FileOutputFormat.getOutputPath(job), outputFilename));
      } catch (URISyntaxException e) {
         e.printStackTrace();
         return null;
      }
   }
   
}
