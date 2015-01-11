package heigvd.bda.labs.graph;

import heigvd.bda.labs.utils.BitcoinAddress;
import heigvd.bda.labs.utils.DoubleWritable;
import heigvd.bda.labs.utils.NodeBitcoin;
import heigvd.bda.labs.utils.PRInputFormat;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Bitcoin extends Configured implements Tool {

	public enum UpdateCounter {
		UPDATED, DANGLING, MAXLINE, LOSS, COUNT
	}

	private int numReducers;
	private Path inputPath;
	private Path outputPath;
	private static String outputString;
//	private static int NB_NODES = 0;
//	private static double COUNT = 0;
	private static final float ALPHA = 0.15f;
	private static final float BETA = 1 - ALPHA;
//	private static float ALPHA_DIV_N = 0;
//	private static final long MAX_DANG_DIGIT = 1000000000;
	private static final long MAX_DANG_DIGIT = 1000000;
	private static final String DANGLING = "DANGLING";
	private static final String MAX_LINE = "MAX_LINE";
	private static final String COUNT = "COUNT";
	private static double EPSILON_CRITERION = 0.00001;
//	private static double EPSILON_CRITERION = 0.01;

	public static String[] splitNode(String text) {
		return text.split("\t");
	}

	public Bitcoin(String[] args) throws IOException {
		if (args.length != 3) {
			System.out.println("Usage: Graph <num_reducers> <input_path> <output_path>");
			System.exit(0);
		}
		numReducers = Integer.parseInt(args[0]);

		outputString = args[2];
		inputPath = new Path(args[1]);
		outputPath = new Path(outputString + "0");
	}

	static class IIMapperREGROUP extends Mapper<BitcoinAddress, BitcoinAddress, BitcoinAddress, BitcoinAddress> {
		@Override
		protected void map(BitcoinAddress key, BitcoinAddress value, Context context) throws IOException, InterruptedException {
			
			// dans le cas où on a pas les en clé tout les noeuds il faut les envoyé à double avec un flag
			// 1->2 3->1      nouveau format (géré avec le empty)
			// 1->2 3->1 2->  ancien  format
			context.write(key, value);
			key.setEmpty(true);
			context.write(value, key);
		}
	}

	static class IIReducerREGROUP extends Reducer<BitcoinAddress, BitcoinAddress, BitcoinAddress, NodeBitcoin> {
		private static final NodeBitcoin NODE = new NodeBitcoin();

		@Override
		protected void reduce(BitcoinAddress key, Iterable<BitcoinAddress> values, Context context) throws IOException,
				InterruptedException {
			
			// merge tous les receveur avec en clé l'expéditeur
			context.getCounter(UpdateCounter.MAXLINE).increment(1);
			
			NODE.setMass(-1);
			NODE.setOldMass(-1);
			NODE.clear();
			

			for (BitcoinAddress v : values) {
				if(!v.isEmpty()){
					NODE.addAdja(v);
				}
			}
			
			context.write(key, NODE);
			System.out.println(key.toString()+" "+NODE.toString());
		}
	}
	
	static class IIMapperPARSE extends Mapper<BitcoinAddress, NodeBitcoin, BitcoinAddress, NodeBitcoin> {
		@Override
		protected void map(BitcoinAddress key, NodeBitcoin value, Context context) throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	static class IIReducerPARSE extends Reducer<BitcoinAddress, NodeBitcoin, BitcoinAddress, NodeBitcoin> {
		private static final NodeBitcoin NODE = new NodeBitcoin();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			//calcule la masse initiale
			float temp = 1.0f / (float) context.getConfiguration().getLong(MAX_LINE, -1);
			
			context.getConfiguration().setFloat(COUNT, temp);
		}

		@Override
		protected void reduce(BitcoinAddress sender, Iterable<NodeBitcoin> values, Context context) throws IOException,
				InterruptedException {
			
			//ajoute la masse initiale à tous les noeuds
			NODE.setMass(context.getConfiguration().getFloat(COUNT,-110));
			NODE.setOldMass(0);
			
			for (NodeBitcoin v : values) {
				NODE.clearSetAdjacency(v.getAdjacency());
				context.write(sender, NODE);
			}
		}
	}

	static class IIMapper extends Mapper<BitcoinAddress, NodeBitcoin, BitcoinAddress, NodeBitcoin> {

		private static final BitcoinAddress ID = new BitcoinAddress();
		private static final NodeBitcoin NODE = new NodeBitcoin();

		@Override
		protected void map(BitcoinAddress key, NodeBitcoin value, Context context) throws IOException, InterruptedException {

			System.out.println(key.toString()+" "+value.toString()+" map");
			
			NODE.clear();
			NODE.setMass(0);
			NODE.setUnDang();

			float p = value.getMass();

			NODE.setOldMass(p);

			if (value.getAdjacency().size() > 0) {
				NODE.clearSetAdjacency(value.getAdjacency());
			} else {
				NODE.setMass(p);
				NODE.setDang();
			}
			context.write(key, NODE);
			NODE.setOldMass(0);

			p /= (double) NODE.getAdjacency().size();
			NODE.setMass(p);
			List<BitcoinAddress> list = NODE.getAdjacencyCopy();
			NODE.clear();
			for (BitcoinAddress i : list) {
				ID.set(i);
				context.write(ID, NODE);
			}
		}
	}

	static class IIReducer extends Reducer<BitcoinAddress, NodeBitcoin, BitcoinAddress, NodeBitcoin> {

		private static final BitcoinAddress ID = new BitcoinAddress();
		private static final NodeBitcoin NODE = new NodeBitcoin();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			context.getCounter(UpdateCounter.DANGLING).setValue(0);
			context.getCounter(UpdateCounter.LOSS).setValue(0);
		}

		@Override
		protected void reduce(BitcoinAddress key, Iterable<NodeBitcoin> values, Context context) throws IOException,
				InterruptedException {

			NODE.clear();

			float sum = 0;

			for (NodeBitcoin d : values) {
				if (d.getAdjacency().size() != 0) {
					NODE.setOldMass(d.getOldMass());
					NODE.clearSetAdjacency(d.getAdjacency());
				} else if (d.isDang()) {
					NODE.setOldMass(d.getOldMass());
					context.getCounter(UpdateCounter.LOSS).increment((long) (d.getMass()*MAX_DANG_DIGIT));
				} else {
					sum += d.getMass();
				}
			}

			ID.set(key);

			NODE.setMass(sum);
			
			System.out.println(ID.toString()+" "+NODE.toString()+" reducer");
			context.write(ID, NODE);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);

			long loss = context.getCounter(UpdateCounter.LOSS).getValue();
			context.getCounter(UpdateCounter.DANGLING).increment((long) (loss / (double) context.getConfiguration().getLong(MAX_LINE, -1)));
		}
	}
	
	static class IIMapperLOSS extends Mapper<BitcoinAddress, NodeBitcoin, BitcoinAddress, NodeBitcoin> {

		@Override
		protected void map(BitcoinAddress key, NodeBitcoin value, Context context) throws IOException, InterruptedException {

			// corrige la perte on la rajoutant à tous les noeuds de manière égale
			float rank = value.getMass();
			float massLoss = context.getConfiguration().getFloat(DANGLING, 0.0f);
			rank += massLoss;
			
			
			float alpha_div_n = 1.0f / (float) context.getConfiguration().getLong(MAX_LINE, -110);
			alpha_div_n *= ALPHA;
			rank = (float) (alpha_div_n + BETA * rank);
			System.out.print(rank);System.out.println("  alphadivn+beta*rank");
			
			value.setMass(rank);
			
			context.write(key, value);
		}
	}

	static class IIReducerLOSS extends Reducer<BitcoinAddress, NodeBitcoin, BitcoinAddress, NodeBitcoin> {
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			context.getCounter(UpdateCounter.UPDATED).setValue(0);
		}

		@Override
		protected void reduce(BitcoinAddress key, Iterable<NodeBitcoin> values, Context context) throws IOException,
				InterruptedException {
			for (NodeBitcoin v : values) { // only done one time
				if (Math.abs(v.getMass() - v.getOldMass()) < EPSILON_CRITERION) {
					context.getCounter(UpdateCounter.UPDATED).increment(1);
				}
				System.out.println(key.toString()+" "+v.toString()+" reduce loss");
				context.write(key, v);
			}
		}
	}
	
	static class IIMapperSORT extends Mapper<BitcoinAddress, NodeBitcoin, DoubleWritable, Text> {
		private static final DoubleWritable KEY = new DoubleWritable();
		private static final Text VALUE = new Text();

		@Override
		protected void map(BitcoinAddress key, NodeBitcoin value, Context context) throws IOException, InterruptedException {

			KEY.set(value.getMass());
			
			VALUE.set(key.toString());
			
			context.write(KEY, VALUE);
		}

	}

	static class IIReducerSORT extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
		@Override
		protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			for (Text v : values) {
				context.write(key, v);
			}
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
//		FileSystem.get(conf).delete(new Path(outputString), true);//distributed only
		
		Path in;
		Path out;

		long counter = 0;
		int depth = 1;
		
		Job jobDATA = new Job(conf, "Graph");
		
		out = new Path(outputString + "regroup");
		doJob(jobDATA, IIMapperREGROUP.class, IIReducerREGROUP.class, BitcoinAddress.class, BitcoinAddress.class, BitcoinAddress.class,
				NodeBitcoin.class, inputPath, out, numReducers, Bitcoin.class,PRInputFormat.class, SequenceFileOutputFormat.class, true);
		
		long nbNodes = jobDATA.getCounters().findCounter(UpdateCounter.MAXLINE).getValue();
		conf.setLong(MAX_LINE, nbNodes);
		
		Job jobPARSE = new Job(conf, "Graph");

		doJob(jobPARSE, IIMapperPARSE.class, IIReducerPARSE.class, BitcoinAddress.class, NodeBitcoin.class, BitcoinAddress.class,
				NodeBitcoin.class, out, outputPath, numReducers, Bitcoin.class,SequenceFileInputFormat.class, SequenceFileOutputFormat.class, true);
		
		FileSystem fs = out.getFileSystem(conf);
		fs.delete(out, true);//clean folder
		
		do {
			Job job = new Job(conf, "Graph");

			in = new Path(outputString + (depth - 1));
			out = new Path(outputString + depth + "tmp");

			doJob(job, IIMapper.class, IIReducer.class, BitcoinAddress.class, NodeBitcoin.class, BitcoinAddress.class, NodeBitcoin.class,
					in, out, numReducers, Bitcoin.class,SequenceFileInputFormat.class, SequenceFileOutputFormat.class, true);

			float dangling = (float) job.getCounters().findCounter(UpdateCounter.DANGLING).getValue();
			dangling /= (float) MAX_DANG_DIGIT;
			conf.setFloat(DANGLING, dangling);

			fs = in.getFileSystem(conf);
			fs.delete(in, true);
			
			Job job2 = new Job(conf, "Graph");
			in = new Path(outputString + depth + "tmp");
			out = new Path(outputString + depth);
			doJob(job2, IIMapperLOSS.class, IIReducerLOSS.class, BitcoinAddress.class, NodeBitcoin.class, BitcoinAddress.class,
					NodeBitcoin.class, in, out, numReducers, Bitcoin.class,SequenceFileInputFormat.class, SequenceFileOutputFormat.class, true);

			// avoid too much folders. comment for debuging is usefull
			fs = in.getFileSystem(conf);
			fs.delete(in, true);			

			depth++;
			counter = job2.getCounters().findCounter(UpdateCounter.UPDATED).getValue();
		} while (counter < nbNodes);

		conf.set("mapred.textoutputformat.separator", "\t\t");
		Job jobSORT = new Job(conf, "Graph");
		doJob(jobSORT, IIMapperSORT.class, IIReducerSORT.class, DoubleWritable.class, Text.class, DoubleWritable.class,
				Text.class, out, new Path(outputString + "SORTED"), numReducers, Bitcoin.class, SequenceFileInputFormat.class, TextOutputFormat.class, true);

		return 0;
	}

	@SuppressWarnings("rawtypes")
	public static void doJob(Job job, Class<? extends Mapper> mapper, Class<? extends Reducer> reducer,
			Class<?> mapOutKey, Class<?> mapOutVal, Class<?> outKey, Class<?> outVal, Path input, Path output,
			int reduce, Class<?> main, Class<? extends InputFormat> inputFormat, Class<? extends OutputFormat> outputFormat, boolean wait) throws Exception {
		job.setMapperClass(mapper);
		job.setReducerClass(reducer);

		job.setMapOutputKeyClass(mapOutKey);
		job.setMapOutputValueClass(mapOutVal);

		job.setOutputKeyClass(outKey);
		job.setOutputValueClass(outVal);
		
		TextInputFormat.addInputPath(job, input);
		job.setInputFormatClass(inputFormat);

		FileOutputFormat.setOutputPath(job, output);
		job.setOutputFormatClass(outputFormat);

		job.setNumReduceTasks(reduce);

		job.setJarByClass(main);

		job.waitForCompletion(wait);
	}
	public static void main(String args[]) throws Exception {
		ToolRunner.run(new Configuration(), new Bitcoin(args), args);
	}
}
