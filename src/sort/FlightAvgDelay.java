package sort;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * @author Shiqi Luo
 *
 */
public class FlightAvgDelay {
	public static class AirlinePartitioner extends Partitioner<IntPair, NumberPair> {

	    @Override
	    public int getPartition(IntPair key, NumberPair value, int numPartitions) {
	    	return (key.getFirst() % 10);
	    }
	}
	
	/**
	 * Mapper class to execute map function
	 * @author Shiqi Luo
	 *
	 */
	public static class ValidateFlightMapper extends Mapper<Object, Text, IntPair, NumberPair> {

		private IntPair airline = null;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Flight flight = CSVHelper.getValidFlight(value.toString(), 2008);
			
			if(flight != null){
				this.airline = new IntPair(flight.getAirlineID(), flight.getMonth());
				context.write(this.airline, new NumberPair(flight.getArrDelayMinutes(), 1));
			}
		}
	}
	
	public static class ValidFlightCombiner extends Reducer<IntPair, NumberPair, IntPair, NumberPair> {
		
		public void reduce(IntPair key, Iterable<NumberPair> values, Context context) throws IOException, InterruptedException {

			 double delaySum = 0;
			 int count = 0;

			 for(NumberPair delayValue: values){
				 delaySum += delayValue.getFirst();
				 count += delayValue.getSecond();
			 }

			 context.write(new IntPair(key.getFirst(), key.getSecond()), new NumberPair(delaySum, count));
		}
	}
	
	/**
	 * Reducer class to execute reducer function
	 * @author Shiqi Luo
	 *
	 */
	public static class FlightsDelayReducer extends Reducer<IntPair, NumberPair, IntWritable, Text> {
		
		 public void reduce(IntPair key, Iterable<NumberPair> values, Context context) throws IOException, InterruptedException {

			 int month = 1;
			 int airlineId = key.getFirst();
			 double delaySum = 0;
			 int count = 0;
			 StringBuilder list = new StringBuilder();
			 ArrayList<Integer> months = new ArrayList<>();
			 ArrayList<Double> delays = new ArrayList<>();
			 ArrayList<Integer> counts = new ArrayList<>();
			 
			 for(NumberPair delayValue: values){
				 months.add(key.getSecond());
				 delays.add(delayValue.getFirst());
				 counts.add(delayValue.getSecond());
			 }

			 int index = 0;
			 while(month<13){
				 if(index < months.size() && month == months.get(index)) {
					 delaySum += delays.get(index);
					 count += counts.get(index);
					 index++;
				 }
				 else {
					 if(count == 0){
						 list.append("(" + month + ", 0), ");
					 }
					 else{
						 list.append("(" + month + ", " + (int)Math.round(delaySum/count) + "), ");
					 }
					 month++;
					 delaySum = 0;
					 count = 0;
				 }
			 }
			 
			 context.write(new IntWritable(airlineId), new Text(list.toString()));
		}
	}
	
	public static class KeyComparator extends WritableComparator {
	    protected KeyComparator() {
	      super(IntPair.class, true);
	    }
	    
	    @Override
	    public int compare(WritableComparable w1, WritableComparable w2) {
	      IntPair ip1 = (IntPair) w1;
	      IntPair ip2 = (IntPair) w2;
	      return ip1.compareTo(ip2);
	    }
	  }
	  
	public static class GroupComparator extends WritableComparator {
	    protected GroupComparator() {
	      super(IntPair.class, true);
	    }
	    
	    @Override
	    public int compare(WritableComparable w1, WritableComparable w2) {
	      IntPair ip1 = (IntPair) w1;
	      IntPair ip2 = (IntPair) w2;
	      return Integer.compare(ip1.getFirst(), ip2.getFirst());
	    }
	}

	/**
	 * Main function of Flight Join class
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Flight delay compute");
	    job.setJarByClass(FlightAvgDelay.class);
	    job.setMapperClass(ValidateFlightMapper.class);
	    job.setCombinerClass(ValidFlightCombiner.class);
	    job.setReducerClass(FlightsDelayReducer.class);
	    job.setPartitionerClass(AirlinePartitioner.class);
	    job.setMapOutputKeyClass(IntPair.class);
	    job.setSortComparatorClass(KeyComparator.class);
	    job.setGroupingComparatorClass(GroupComparator.class);
	    job.setMapOutputValueClass(NumberPair.class);
	    job.setNumReduceTasks(10);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    int code = job.waitForCompletion(true) ? 0 : 1;

	    System.exit(code);
	}
}

