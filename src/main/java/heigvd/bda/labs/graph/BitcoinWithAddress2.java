package heigvd.bda.labs.graph;

import heigvd.bda.labs.utils.DoubleWritable;
import heigvd.bda.labs.utils.NodeText;
import heigvd.bda.labs.utils.PRInputFormat;
import heigvd.bda.labs.utils.PROutputFormat;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BitcoinWithAddress2 extends Configured implements Tool {

	public enum UpdateCounter {
		UPDATED, DANGLING, MAXLINE
	}

	private int numReducers;
	private Path inputPath;
	private Path outputPath;
	private static String outputString;
//	private static int NB_NODES = 0;
	private static double COUNT = 0;
	private static final double ALPHA = 0.15;
	private static final double BETA = 1 - ALPHA;
	private static double ALPHA_DIV_N = 0;
	private static final long MAX_DANG_DIGIT = 1000000000;
	private static final String DANGLING = "DANGLING";
	private static final String MAX_LINE = "MAX_LINE";
//	private static double EPSILON_CRITERION = 0.00001;
	private static double EPSILON_CRITERION = 0.001;

	private static final int TAG_DANG = 0;
	private static final int TAG_UNDANG = 1;

	public static String[] splitNode(String text) {
		return text.split("\t");
	}

	public BitcoinWithAddress2(String[] args) throws IOException {
		if (args.length != 3) {
			System.out.println("Usage: Graph <num_reducers> <input_path> <output_path>");
			System.exit(0);
		}
		numReducers = Integer.parseInt(args[0]);

		outputString = args[2];
		inputPath = new Path(args[1]);
		outputPath = new Path(outputString + "0");
	}

	static class IIMapperREGROUP extends Mapper<LongWritable, Text, Text, Text> {
		private static final Text SENDER = new Text();
		private static final Text RECIEVER = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			// on split le tout, et on envoie expéditeur/receveur
			String[] s = value.toString().split("\t".intern());

			SENDER.set(s[0]);
			
			if (s.length == 1) {
				RECIEVER.set("".intern());
				context.write(SENDER, RECIEVER);
			}

			for (int i = 1; i < s.length; i++) {
				RECIEVER.set(s[i]);
				context.write(SENDER, RECIEVER);
			}
		}
	}

	static class IIReducerREGROUP extends Reducer<Text, Text, Text, NodeText> {
		private static final NodeText NODE = new NodeText();

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			
			// merge tous les receveur avec en clé l'expéditeur
			context.getCounter(UpdateCounter.MAXLINE).increment(1);
			
			NODE.setMass(-1);
			NODE.setOldMass(-1);
			NODE.clear();
			

			for (Text v : values) {
				NODE.addAdja(v.toString());
			}
			
			context.write(key, NODE);
		}
	}
	
	static class IIMapperPARSE extends Mapper<Text, NodeText, Text, NodeText> {
		@Override
		protected void map(Text key, NodeText value, Context context) throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	static class IIReducerPARSE extends Reducer<Text, NodeText, Text, NodeText> {
		private static final NodeText NODE = new NodeText();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			//calcule la masse initiale
			COUNT = 1.0 / (double) context.getConfiguration().getLong(MAX_LINE, -1);
			ALPHA_DIV_N = ALPHA * COUNT;
		}

		@Override
		protected void reduce(Text sender, Iterable<NodeText> values, Context context) throws IOException,
				InterruptedException {
			
			//ajoute la masse initiale à tous les noeuds
			NODE.setMass(COUNT);
			NODE.setOldMass(0);
			
			for (NodeText v : values) {
				NODE.clearSetAdjacency(v.getAdjacency());
				context.write(sender, NODE);
			}
		}
	}

	static class IIMapper extends Mapper<Text, NodeText, Text, NodeText> {

		private static final Text ID = new Text();
		private static final NodeText NODE = new NodeText();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			context.getCounter(UpdateCounter.DANGLING).setValue(0);
		}

		@Override
		protected void map(Text key, NodeText value, Context context) throws IOException, InterruptedException {

			NODE.clear();
			NODE.setMass(0);
			NODE.setUnDang();
//			NODE.setID(TAG_UNDANG);

			ID.set(key.toString());
			double p = value.getMass();

			NODE.setOldMass(p);

			if (value.getAdjacency().size() > 0) {
				NODE.clearSetAdjacency(value.getAdjacency());
			} else {
				NODE.setMass(p);
//				NODE.setID(TAG_DANG);
				NODE.setDang();
			}
			context.write(ID, NODE);
			NODE.setOldMass(0);

			p /= (double) NODE.getAdjacency().size();
			NODE.setMass(p);
			List<String> list = NODE.getAdjacencyCopy();
			NODE.clear();
			for (String i : list) {
				ID.set(i);
				context.write(ID, NODE);
			}
		}
	}

	static class IIReducer extends Reducer<Text, NodeText, Text, NodeText> {

		private static final Text ID = new Text();
		private static final NodeText NODE = new NodeText();
		private static double loss = 0;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			loss = 0;
		}

		@Override
		protected void reduce(Text key, Iterable<NodeText> values, Context context) throws IOException,
				InterruptedException {

			NODE.clear();

			double sum = 0;

			for (NodeText d : values) {
				if (d.getAdjacency().size() != 0) {
					NODE.setOldMass(d.getOldMass());
					NODE.clearSetAdjacency(d.getAdjacency());
				} else if (d.isDang()) {
					NODE.setOldMass(d.getOldMass());
					loss += d.getMass();
				} else {
					sum += d.getMass();
				}
			}

			ID.set(key.toString());

			NODE.setMass(sum);
			context.write(ID, NODE);
			System.out.println(key.toString()+" "+NODE.toString()+" reducer");
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);

			context.getCounter(UpdateCounter.DANGLING).increment((long) (loss * MAX_DANG_DIGIT / (double) context.getConfiguration().getLong(MAX_LINE, -1)));
		}
	}
	
	static class IIMapperLOSS extends Mapper<Text, NodeText, Text, NodeText> {

		@Override
		protected void map(Text key, NodeText value, Context context) throws IOException, InterruptedException {

			// corrige la perte on la rajoutant à tous les noeuds de manière égale
			double rank = value.getMass();
			double massLoss = context.getConfiguration().getFloat(DANGLING, 0.0f);
			rank += massLoss;
			rank = ALPHA_DIV_N + BETA * rank;
			value.setMass(rank);
			
			context.write(key, value);
		}
	}

	static class IIReducerLOSS extends Reducer<Text, NodeText, Text, NodeText> {
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			context.getCounter(UpdateCounter.UPDATED).setValue(0);
		}

		@Override
		protected void reduce(Text key, Iterable<NodeText> values, Context context) throws IOException,
				InterruptedException {
			for (NodeText v : values) { // only done one time
				if (Math.abs(v.getMass() - v.getOldMass()) < EPSILON_CRITERION) {
					context.getCounter(UpdateCounter.UPDATED).increment(1);
				}
				context.write(key, v);
			}
		}
	}
	
	static class IIMapperSORT extends Mapper<Text, NodeText, DoubleWritable, Text> {
		private static final DoubleWritable KEY = new DoubleWritable();
		private static final Text VALUE = new Text();

		@Override
		protected void map(Text key, NodeText value, Context context) throws IOException, InterruptedException {

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
		FileSystem.get(new Configuration()).delete(new Path(outputString), true);
		Path in;
		Path out;

		long counter = 0;
		int depth = 1;
		Configuration conf = this.getConf();
		Job jobDATA = new Job(conf, "Graph");
		FileSystem.get(conf).delete(outputPath, true);
		
		
		out = new Path(outputString + "regroup");
		doJob(jobDATA, IIMapperREGROUP.class, IIReducerREGROUP.class, Text.class, Text.class, Text.class,
				NodeText.class, inputPath, out, numReducers, BitcoinWithAddress2.class,TextInputFormat.class, SequenceFileOutputFormat.class, true);
		
		long nbNodes = jobDATA.getCounters().findCounter(UpdateCounter.MAXLINE).getValue();
		conf.setLong(MAX_LINE, nbNodes);
		
		Job jobPARSE = new Job(conf, "Graph");

		doJob(jobPARSE, IIMapperPARSE.class, IIReducerPARSE.class, Text.class, NodeText.class, Text.class,
				NodeText.class, out, outputPath, numReducers, BitcoinWithAddress2.class,SequenceFileInputFormat.class, SequenceFileOutputFormat.class, true);
		

		do {
			Job job = new Job(conf, "Graph");

			in = new Path(outputString + (depth - 1));
			out = new Path(outputString + depth + "tmp");

			doJob(job, IIMapper.class, IIReducer.class, Text.class, NodeText.class, Text.class, NodeText.class,
					in, out, numReducers, BitcoinWithAddress2.class,SequenceFileInputFormat.class, SequenceFileOutputFormat.class, true);

			float dangling = (float) job.getCounters().findCounter(UpdateCounter.DANGLING).getValue();
			dangling /= (float) MAX_DANG_DIGIT;
			conf.setFloat(DANGLING, dangling);

			Job job2 = new Job(conf, "Graph");
			in = new Path(outputString + depth + "tmp");
			out = new Path(outputString + depth);
			doJob(job2, IIMapperLOSS.class, IIReducerLOSS.class, Text.class, NodeText.class, Text.class,
					NodeText.class, in, out, numReducers, BitcoinWithAddress2.class,SequenceFileInputFormat.class, SequenceFileOutputFormat.class, true);

			// avoid too much folders. comment for debuging is usefull
			FileSystem.get(new Configuration()).delete(in, true);

			depth++;
			counter = job2.getCounters().findCounter(UpdateCounter.UPDATED).getValue();
		} while (counter < nbNodes);

		conf.set("mapred.textoutputformat.separator", "\t\t");
		Job jobSORT = new Job(conf, "Graph");
		doJob(jobSORT, IIMapperSORT.class, IIReducerSORT.class, DoubleWritable.class, Text.class, DoubleWritable.class,
				Text.class, out, new Path(outputString + "SORTED"), numReducers, BitcoinWithAddress2.class, SequenceFileInputFormat.class, TextOutputFormat.class, true);

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
		ToolRunner.run(new Configuration(), new BitcoinWithAddress2(args), args);
	}
}
