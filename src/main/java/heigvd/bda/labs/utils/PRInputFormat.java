package heigvd.bda.labs.utils;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class PRInputFormat extends FileInputFormat<BitcoinAddress, BitcoinAddress> {

	static class PRRecordReader extends RecordReader<BitcoinAddress, BitcoinAddress> {

		ArrayList<BufferedInputStream> readers = new ArrayList<BufferedInputStream>();
		BitcoinAddress currentKey;
		BitcoinAddress currentValue;

		long fileSize = 0;
		long byteRead = 0; // This is an approximation.

		int N = 0; // Number of nodes.

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			try {
				File directory = new File(new URI(PRInputFormat.getInputPaths(context)[0].toString()));
				for (File file : directory.listFiles(new FilenameFilter() {
					public boolean accept(File dir, String name) {
						return name.toLowerCase().endsWith(".bin");
					}
				})) {
					this.fileSize += file.length();

					BufferedInputStream br = new BufferedInputStream(new FileInputStream(file), 4096);
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

					this.currentKey = new BitcoinAddress(key);
					this.currentValue = new BitcoinAddress(value);
					return true;
				}
			}
		}

		private void splitTabInTwo(byte[] completTab, byte[] tab1, byte[] tab2) {
			int half = (int) Math.floor(completTab.length / 2);
			for (int i = 0; i < half; i++) {
				tab1[i] = completTab[i];
			}
			for (int i = half; i < completTab.length; i++) {
				tab2[i - half] = completTab[i];
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
			for (BufferedInputStream reader : this.readers)
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
	public RecordReader<BitcoinAddress, BitcoinAddress> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new PRRecordReader();
	}
}
