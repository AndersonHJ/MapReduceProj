import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import examples.CountNumUsersByStateDriver.CountNumUsersByStateMapper;
import join.CSVHelper;
import join.Flight;


/**
 * @author Shiqi Luo
 *
 */
public class TestWordCount {

	/**
	 * Partitioner class partition keys
	 * @author Shiqi Luo
	 *
	 */
//	public static class LetterPartitioner extends Partitioner<Text, DoubleWritable> {
//
//		@Override
//		public int getPartition(Text arg0, DoubleWritable arg1, int arg2) {
//			
//			if(arg0.toString().equals("But")){
//				return 0;
//			}
//			else{
//				return 1;
//			}
//		}
//	}
	
	/**
	 * Mapper class to execute map function
	 * @author Shiqi Luo
	 *
	 */
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//System.out.println(value.toString());
			String validDate = CSVHelper.checkFlightValidation(value.toString(), 2007, 6, 2008, 5);
			
			if(validDate != null){
				word.set(validDate);
				context.write(word, value);
			}
		}
	}
	
	/**
	 * Reducer class to execute map function
	 * @author Shiqi Luo
	 *
	 */
	public static class DoubleSumReducer extends Reducer<Text,Text,Text,DoubleWritable> {
		 private DoubleWritable result = new DoubleWritable();
		 public static final String FLIGHTSUM_COUNTER_GROUP = "flightsDelaySum";
		 public static final String FLIGHT_COUNTER_GROUP = "flightsCount";
		
		 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			 ArrayList<Flight> flights1 = new ArrayList<>();
			 ArrayList<Flight> flights2 = new ArrayList<>();
			 for(Text origin : values){
				 Flight flight = CSVHelper.getFlight(origin.toString(), 2007, 6, 2008, 5);
				 flights1.add(flight);
			 }
			 for(Flight fli: flights1){
				 flights2.add(fli);
			 }
			 
			 for (Flight origin : flights1) {
				 if(origin.getOrigin().toString().equals("ORD")){
					 for(Flight dest: flights2) {
						 if(dest.getDestination().toString().equals("JFK")
								 && origin.getDestination().toString().equals(dest.getOrigin().toString())
								 && origin.getArrTime().get() < dest.getDepTime().get()){
							 result.set(origin.getArrDelayMinutes().get() + dest.getArrDelayMinutes().get());
							 context.write(new Text(origin.toString() + " ---> " + dest.toString()), result);
							 context.getCounter(FLIGHT_COUNTER_GROUP, "flight nums").increment(1);
							 context.getCounter(FLIGHTSUM_COUNTER_GROUP, "flight delay sum").increment((long)result.get());
						 }
					 }
				 }
			 }

		}
	}
	
	public static class KeyComparator extends WritableComparator {
		protected KeyComparator() {
			super(DoubleWritable.class, true);
		}
		
		@Override
		public int compare(WritableComparable w1, WritableComparable w2){
			
			return 0;
		}
	}
	
	public static class GroupComparator extends WritableComparator {
		protected GroupComparator() {
			super(DoubleWritable.class, true);
		}
		
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			return 0;
		}
	}

	/**
	 * Main function of WordCount class
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(TestWordCount.class);
	    job.setMapperClass(TokenizerMapper.class);
	    //job.setCombinerClass(DoubleSumReducer.class);
	    job.setReducerClass(DoubleSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    //job.setSortComparatorClass(KeyComparator.class);
	    //job.setGroupingComparatorClass(GroupComparator.class);
	    job.setOutputValueClass(Text.class);
	    job.setNumReduceTasks(5);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    int code = job.waitForCompletion(true) ? 0 : 1;
	    
	    if (code == 0) {
	    	long sum = 0;
	    	long count = 0;
			for (Counter counter : job.getCounters().getGroup(
					DoubleSumReducer.FLIGHT_COUNTER_GROUP)) {
				if(counter.getDisplayName().equals("flight nums")){
					count = counter.getValue();
					System.out.println(counter.getDisplayName() + "\t" + counter.getValue());
				}
			}
			for (Counter counter : job.getCounters().getGroup(
					DoubleSumReducer.FLIGHTSUM_COUNTER_GROUP)) {
				if(counter.getDisplayName().equals("flight delay sum")){
					sum = counter.getValue();
					System.out.println(counter.getDisplayName() + "\t"
						+ counter.getValue());
				}
			}
			System.out.println("Average delay: " + sum/count);
		}
	    
	    System.exit(code);
	}
}
