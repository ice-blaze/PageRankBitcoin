package heigvd.bda.labs.graph;

import heigvd.bda.labs.utils.DoubleWritable;
import heigvd.bda.labs.utils.NodePR;
import heigvd.bda.labs.utils.PRInputFormat;
import heigvd.bda.labs.utils.PROutputFormat;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Bitcoin extends Configured implements Tool {

	public enum UpdateCounter {
		UPDATED, DANGLING
	}

	private int numReducers;
	private Path inputPath;
	private Path outputPath;
	private static String outputString;
	private static int NB_NODES = 0;
	private static double COUNT = 0;
	private static final double ALPHA = 0.15;
	private static final double BETA = 1 - ALPHA;
	private static double ALPHA_DIV_N = 0;
	private static final long MAX_DANG_DIGIT = 1000000000;
	private static final String DANGLING = "DANGLING";
	private static double EPSILON_CRITERION = 0.00001;

	private static final int TAG_DANG = 0;
	private static final int TAG_UNDANG = 1;

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

	static class IIMapperLOSS extends Mapper<IntWritable, NodePR, IntWritable, NodePR> {

		@Override
		protected void map(IntWritable key, NodePR value, Context context) throws IOException, InterruptedException {

			double rank = value.getMass();
			double massLoss = context.getConfiguration().getFloat(DANGLING, 0.0f);
			rank += massLoss;
			rank = ALPHA_DIV_N + BETA * rank;
			value.setMass(rank);
			
			context.write(key, value);
		}
	}

	static class IIReducerLOSS extends Reducer<IntWritable, NodePR, IntWritable, NodePR> {
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			context.getCounter(UpdateCounter.UPDATED).setValue(0);
		}

		@Override
		protected void reduce(IntWritable key, Iterable<NodePR> values, Context context) throws IOException,
				InterruptedException {
			for (NodePR v : values) { // only done one time
				if (Math.abs(v.getMass() - v.getOldMass()) < EPSILON_CRITERION) {
					context.getCounter(UpdateCounter.UPDATED).increment(1);
				}
				context.write(key, v);
			}
		}
	}

	static class IIMapperSORT extends Mapper<IntWritable, NodePR, DoubleWritable, Text> {
		private static final DoubleWritable KEY = new DoubleWritable();
		private static final Text VALUE = new Text();

		@Override
		protected void map(IntWritable key, NodePR value, Context context) throws IOException, InterruptedException {

			KEY.set(value.getMass());
			
			StringBuilder builder = new StringBuilder();
			builder.append(String.valueOf(key.get()));
			VALUE.set(builder.toString());
			
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

	static class IIMapperREGROUP extends Mapper<LongWritable, Text, IntWritable, Text> {
		private static final IntWritable KEY = new IntWritable();
		private static final Text VALUE = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] s = value.toString().split("\t".intern());

			KEY.set(Integer.valueOf(s[0]));

			if (s.length == 1) {
				VALUE.set("".intern());
				context.write(KEY, VALUE);
			}

			for (int i = 1; i < s.length; i++) {
				VALUE.set(s[i]);
				context.write(KEY, VALUE);
			}
		}
	}

	static class IIReducerREGROUP extends Reducer<IntWritable, Text, IntWritable, NodePR> {
		private static final NodePR NODE = new NodePR();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
		}

		@Override
		protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			NODE.setMass(-1);
			NODE.setOldMass(-1);

			StringBuilder strB = new StringBuilder();
			for (Text v : values) {
				strB.append(v.toString()).append(":");
			}
			
			
			NODE.setAdjacency(strB.toString());
			context.write(key, NODE);
		}
	}
	
	static class IIMapperPARSE extends Mapper<IntWritable, NodePR, IntWritable, NodePR> {

		@Override
		protected void map(IntWritable key, NodePR value, Context context) throws IOException, InterruptedException {

			NB_NODES++;
			

			context.write(key, value);
		}
	}

	static class IIReducerPARSE extends Reducer<IntWritable, NodePR, IntWritable, NodePR> {
		private static final NodePR NODE = new NodePR();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			COUNT = 1.0 / (double) NB_NODES;
			ALPHA_DIV_N = ALPHA * COUNT;
		}

		@Override
		protected void reduce(IntWritable key, Iterable<NodePR> values, Context context) throws IOException,
				InterruptedException {
			NODE.setMass(COUNT);
			NODE.setOldMass(0);
			
			for (NodePR v : values) {
				NODE.setAdjacency(v.getAdjacency());
				context.write(key, NODE);
			}
			System.out.println(String.valueOf(key)+" "+NODE.toString());
		}
	}

	static class IIMapper extends Mapper<IntWritable, NodePR, IntWritable, NodePR> {

		private static final IntWritable ID = new IntWritable();
		private static final NodePR NODE = new NodePR();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			context.getCounter(UpdateCounter.DANGLING).setValue(0);
		}

		@Override
		protected void map(IntWritable key, NodePR value, Context context) throws IOException, InterruptedException {

			NODE.clear();
			NODE.setMass(0);
			NODE.setID(TAG_UNDANG);

			ID.set(key.get());
			double p = value.getMass();

			NODE.setOldMass(p);

			if (value.getAdjacency().size() > 0) {
				NODE.setAdjacency(value.getAdjacency());
			} else {
				NODE.setMass(p);
				NODE.setID(TAG_DANG);
			}
			context.write(ID, NODE);
			NODE.setOldMass(0);

			p /= (double) NODE.getAdjacency().size();
			NODE.setMass(p);
			List<Integer> list = NODE.getAdjacencyCopy();
			NODE.clear();
			for (int i : list) {
				ID.set(i);
				context.write(ID, NODE);
			}
		}
	}

	static class IIReducer extends Reducer<IntWritable, NodePR, IntWritable, NodePR> {

		private static final IntWritable ID = new IntWritable();
		private static final NodePR NODE = new NodePR();
		private static double loss = 0;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			loss = 0;
		}

		@Override
		protected void reduce(IntWritable key, Iterable<NodePR> values, Context context) throws IOException,
				InterruptedException {

			NODE.clear();

			double sum = 0;

			for (NodePR d : values) {
				if (d.getAdjacency().size() != 0) {
					NODE.setOldMass(d.getOldMass());
					NODE.setAdjacency(d.getAdjacency());
				} else if (d.getID() == TAG_DANG) {
					NODE.setOldMass(d.getOldMass());
					loss += d.getMass();
				} else {
					sum += d.getMass();
				}
			}

			ID.set(key.get());

			NODE.setMass(sum);
			context.write(ID, NODE);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);

			context.getCounter(UpdateCounter.DANGLING).increment((long) (loss * MAX_DANG_DIGIT / (double) NB_NODES));
		}
	}

	public int run(String[] args) throws Exception {
		FileSystem.get(new Configuration()).delete(new Path(outputString), true);
		Path in;
		Path out;

		long counter = 0;
		int depth = 1;
		Configuration conf = this.getConf();
		Job jobDATA = new Job(conf, "Graph");
		FileSystem.get(conf).delete(outputPath, true);
		
		
		out = new Path(outputString + "regroup");
		doJob(jobDATA, IIMapperREGROUP.class, IIReducerREGROUP.class, IntWritable.class, Text.class, IntWritable.class,
				NodePR.class, inputPath, out, numReducers, Bitcoin.class,SequenceFileInputFormat.class, PROutputFormat.class, true);
		
		Job jobPARSE = new Job(conf, "Graph");

		doJob(jobPARSE, IIMapperPARSE.class, IIReducerPARSE.class, IntWritable.class, NodePR.class, IntWritable.class,
				NodePR.class, out, outputPath, numReducers, Bitcoin.class,PRInputFormat.class, PROutputFormat.class, true);

		do {
			Job job = new Job(conf, "Graph");

			in = new Path(outputString + (depth - 1));
			out = new Path(outputString + depth + "tmp");

			doJob(job, IIMapper.class, IIReducer.class, IntWritable.class, NodePR.class, IntWritable.class, NodePR.class,
					in, out, numReducers, Bitcoin.class,PRInputFormat.class, PROutputFormat.class, true);

			float dangling = (float) job.getCounters().findCounter(UpdateCounter.DANGLING).getValue();
			dangling /= (float) MAX_DANG_DIGIT;
			conf.setFloat(DANGLING, dangling);

			Job job2 = new Job(conf, "Graph");
			in = new Path(outputString + depth + "tmp");
			out = new Path(outputString + depth);
			doJob(job2, IIMapperLOSS.class, IIReducerLOSS.class, IntWritable.class, NodePR.class, IntWritable.class,
					NodePR.class, in, out, numReducers, Bitcoin.class,PRInputFormat.class, PROutputFormat.class, true);

			// avoid too much folders. comment for debuging is usefull
			FileSystem.get(new Configuration()).delete(in, true);

			depth++;
			counter = job2.getCounters().findCounter(UpdateCounter.UPDATED).getValue();
		} while (counter < NB_NODES);

		conf.set("mapred.textoutputformat.separator", "\t\t");
		Job jobSORT = new Job(conf, "Graph");
		doJob(jobSORT, IIMapperSORT.class, IIReducerSORT.class, DoubleWritable.class, Text.class, DoubleWritable.class,
				Text.class, out, new Path(outputString + "SORTED"), numReducers, Bitcoin.class, PRInputFormat.class, TextOutputFormat.class, true);

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
