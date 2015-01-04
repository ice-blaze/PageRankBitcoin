package heigvd.bda.labs.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class StrangeBinary extends FileInputFormat<IntWritable, NodePR>  {
      
   static class PRRecordReader extends RecordReader<IntWritable, NodePR> {

      ArrayList<BufferedReader> readers = new ArrayList<BufferedReader>();
      IntWritable currentKey;
      NodePR currentValue;
      
      long fileSize = 0;
      long byteRead = 0; // This is an approximation.
      
      int N = 0; // Number of nodes.
      
      @Override
      public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
         try {            
            File directory = new File(new URI(StrangeBinary.getInputPaths(context)[0].toString()));
            for (File file : directory.listFiles(new FilenameFilter() {
               public boolean accept(File dir, String name) {
                  return name.toLowerCase().endsWith(".txt");
               }
              }))
            {
               this.fileSize += file.length();
               
               BufferedReader br = new BufferedReader(new FileReader(file), 4096);               
               this.readers.add(br);
            }
         } catch (URISyntaxException e) {
            e.printStackTrace();
         }
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
         for (;;) {
            if (this.readers.isEmpty())
               return false;
            String currentLine = this.readers.get(0).readLine();
            
            if (currentLine == null) { // EOF.
               this.readers.get(0).close();
               this.currentKey = null;
               this.currentValue = null;
               this.readers.remove(0);
            } else {
               this.byteRead += currentLine.length() + 1; 
               
               String[] str = currentLine.split("\t");
               
               this.currentKey = new IntWritable(Integer.parseInt(str[0]));
               
               this.currentValue = NodePR.fromString(str[1],str[2],(str.length==4?str[3]:""));
               return true;
            }
         }
      }

      @Override
      public IntWritable getCurrentKey() throws IOException, InterruptedException {
         return this.currentKey;
      }

      @Override
      public NodePR getCurrentValue() throws IOException, InterruptedException {
         return this.currentValue;
      }

      @Override
      public float getProgress() throws IOException, InterruptedException {
         if (this.byteRead >= this.fileSize)
            return 1.0f;
         return (float)this.byteRead / (float)this.fileSize;
      }

      @Override
      public void close() throws IOException {
         for (BufferedReader reader : this.readers)
            reader.close();
         this.currentKey = null;
         this.currentValue = null;         
      }
   }

   /**
    * In our case there is only one split.
    */
   @Override
   protected boolean isSplitable(JobContext context, Path filename) {
      return false;
   }
   
   @Override
   public RecordReader<IntWritable, NodePR> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
      return new PRRecordReader();
   }
}
