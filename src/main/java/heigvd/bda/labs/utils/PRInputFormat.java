package heigvd.bda.labs.utils;

import heigvd.bda.labs.graph.Debug;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.sound.midi.SysexMessage;

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

		ArrayList<FSDataInputStream> readers = new ArrayList<FSDataInputStream>();
		BitcoinAddress currentKey = new BitcoinAddress();
		BitcoinAddress currentValue = new BitcoinAddress();

		long fileSize = 0;
		long byteRead = 0; // This is an approximation.

//		int N = 0; // Number of nodes.
		
		@Override
		public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
			
			FileSplit split = (FileSplit) genericSplit;
			Configuration job = context.getConfiguration();
			final Path file = split.getPath();
	        FileSystem fs = file.getFileSystem(job);
	        FSDataInputStream fileIn = fs.open(split.getPath());
	        this.fileSize += split.getLength();
			this.readers.add(fileIn);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			for (;;) {
				if (this.readers.isEmpty())
					return false;
				
				System.out.print("available : ");
				System.out.println(readers.get(0).available());
				
				byte[] key = new byte[BitcoinAddress.SIZE];
				byte[] value = new byte[BitcoinAddress.SIZE];
				boolean eof = this.readers.get(0).read(key) < BitcoinAddress.SIZE
						|| this.readers.get(0).read(value) < BitcoinAddress.SIZE;
				if (eof) {
					this.readers.get(0).close();
					this.currentKey = null;
					this.currentValue = null;
					this.readers.remove(0);
				} else {
					this.byteRead += 2 * BitcoinAddress.SIZE;

					this.currentKey.set(key);
					this.currentValue.set(value);
//					N++;
					return true;
				}
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
			if (this.byteRead >= this.fileSize)
				return 1.0f;
			return (float) this.byteRead / (float) this.fileSize;
		}

		@Override
		public void close() throws IOException {
			for (FSDataInputStream reader : this.readers)
				reader.close();
			this.currentKey = null;
			this.currentValue = null;
			
//			Debug.print("transaction number : ");
//			Debug.print(N);
//			System.out.print("transaction number : ");
//			System.out.println(N);
//			System.exit(0);
		}
	}

	/**
	 * In our case there is only one split.
	 */
//	@Override
//	protected boolean isSplitable(JobContext context, Path filename) {
//		return false;
//	}
	
//	@Override
//	 protected long getFormatMinSplitSize() {
//	    return 40;
//	  }
	

	@Override
	public RecordReader<BitcoinAddress, BitcoinAddress> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
//		List<InputSplit> splits = super.getSplits(context);
		return new PRRecordReader();
	}
}
