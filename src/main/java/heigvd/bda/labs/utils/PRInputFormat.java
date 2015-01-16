package heigvd.bda.labs.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.sound.midi.SysexMessage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class PRInputFormat extends FileInputFormat<BitcoinAddress, BitcoinAddress> {

	static class PRRecordReader extends RecordReader<BitcoinAddress, BitcoinAddress> {

		FSDataInputStream reader;
		BitcoinAddress currentKey = new BitcoinAddress();
		BitcoinAddress currentValue = new BitcoinAddress();
		
		long byteRead = 0;
		long start = 0;
		long end = 0;

	   static final Log LOG = LogFactory.getLog(PRRecordReader.class);	 
		
		@Override
		public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
			FileSplit split = (FileSplit) genericSplit;
			Configuration job = context.getConfiguration();
			final Path file = split.getPath();
         FileSystem fs = file.getFileSystem(job);
         this.reader = fs.open(split.getPath());
         
         this.start = split.getStart();
         long rem = this.start % 40L;
         if (rem != 0)
            this.start -= rem;         
         
         long size = split.getLength();
         rem = size % 40L;
         if (rem != 0)
            size -= rem;         
         
         this.end = this.start + size;
         this.reader.seek(this.start);
         
         LOG.info(String.format("PRRecordReader.initialize(), start: %d", this.start));
         LOG.info(String.format("PRRecordReader.initialize(), end: %d", this.end));
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
		   if (this.start + this.byteRead >= this.end)
		      return false;
			
			byte[] key = new byte[BitcoinAddress.SIZE];
			byte[] value = new byte[BitcoinAddress.SIZE];
			boolean eof = this.reader.read(key) < BitcoinAddress.SIZE || this.reader.read(value) < BitcoinAddress.SIZE;
			if (eof) {
				this.currentKey = null;
				this.currentValue = null;
				return false;
			} else {
				this.byteRead += 2 * BitcoinAddress.SIZE;
				this.currentKey.set(key);
				this.currentValue.set(value);
				return true;
			}
		}

		@Override
		public BitcoinAddress getCurrentKey() throws IOException, InterruptedException {
			return this.currentKey;
		}

		@Override
		public BitcoinAddress getCurrentValue() throws IOException, InterruptedException {
			return this.currentValue;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			if (this.start + this.byteRead >= this.end)
				return 1.0f;
			return (float)this.byteRead / (float)(this.end - this.start);
		}

		@Override
		public void close() throws IOException {
			
		   this.reader.close();
			
			this.currentKey = null;
			this.currentValue = null;
		}
	}

	@Override
	public RecordReader<BitcoinAddress, BitcoinAddress> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
//		List<InputSplit> splits = super.getSplits(context);
		return new PRRecordReader();
	}
}
