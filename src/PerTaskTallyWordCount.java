import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author Shiqi Luo
 *
 */
public class PerTaskTallyWordCount {

	/**
	 * Partitioner class partition keys
	 * @author Shiqi Luo
	 *
	 */
	public static class LetterPartitioner extends Partitioner<Text, IntWritable> {

		@Override
		public int getPartition(Text arg0, IntWritable arg1, int arg2) {
			char firstLetter = Character.toLowerCase((char)arg0.charAt(0));
			
			if(firstLetter == 'm'){
				return 0;
			}
			else if(firstLetter == 'n'){
				return 1;
			}
			else if(firstLetter == 'o'){
				return 2;
			}
			else if(firstLetter == 'p'){
				return 3;
			}
			else{
				return 4;
			}
		}
	}
	
	/**
	 * Mapper class to execute map function
	 * @author Shiqi Luo
	 *
	 */
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		private Text word = new Text();
		private HashMap<String, Integer> map;
		
		@Override
		protected void setup(Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			this.map = new HashMap<>();
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			StringTokenizer itr = new StringTokenizer(value.toString());

			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				char firstChar = Character.toLowerCase((char)word.charAt(0));
				String letters = word.toString();
				if(firstChar == 'n' || firstChar == 'm' || firstChar == 'o' || firstChar == 'p' || firstChar == 'q'){
					map.put(letters, map.getOrDefault(letters, 0) + 1);
				}
			}
		}
		
		@Override
		protected void cleanup(Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
			for(Map.Entry<String, Integer> entry: this.map.entrySet()){
				context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
			}
		}
	}
	
	/**
	 * Reducer class to execute map function
	 * @author Shiqi Luo
	 *
	 */
	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		 private IntWritable result = new IntWritable();
		
		 public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			 int sum = 0;
			 for (IntWritable val : values) {
				 sum += val.get();
			 }
			 result.set(sum);
			 context.write(key, result);
		}
	}
	
	

	/**
	 * Main function of WordCount class
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(PerTaskTallyWordCount.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setPartitionerClass(LetterPartitioner.class);
	    job.setNumReduceTasks(5);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
