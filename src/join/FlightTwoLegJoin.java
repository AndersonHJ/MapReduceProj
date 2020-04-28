package join;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * @author Shiqi Luo
 *
 */
public class FlightTwoLegJoin {
	
	/**
	 * Mapper class to execute map function
	 * @author Shiqi Luo
	 *
	 */
	public static class ValidateFlightMapper extends Mapper<Object, Text, Text, Text> {

		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String validDate = CSVHelper.checkFlightValidation(value.toString(), 2007, 12, 2008, 1);
			
			if(validDate != null){
				word.set(validDate);
				context.write(word, value);
			}
		}
	}
	
	/**
	 * Reducer class to execute reducer function
	 * @author Shiqi Luo
	 *
	 */
	public static class FlightsJoinReducer extends Reducer<Text,Text,Text,DoubleWritable> {
		 private DoubleWritable result = new DoubleWritable();
		 public static final String FLIGHTSUM_COUNTER_GROUP = "flightsDelaySum";
		 public static final String FLIGHT_COUNTER_GROUP = "flightsCount";
		 public static final String FLIGHT_AVG_DELAY = "flights avg delay";
		
		 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			 ArrayList<Flight> flights1 = new ArrayList<>();
			 ArrayList<Flight> flights2 = new ArrayList<>();
			 for(Text value : values){
				 Flight flight = CSVHelper.getFlight(value.toString(), 2007, 12, 2008, 1);
				 if(flight.getOrigin().toString().equals("ORD") && !flight.getDestination().toString().equals("JFK")){
					 flights1.add(flight);
				 }
				 if(!flight.getOrigin().toString().equals("ORD") && flight.getDestination().toString().equals("JFK")){
					 flights2.add(flight);
				 }
			 }
			 
			 for (Flight origin : flights1) {
				 for(Flight dest: flights2) {
					 if(origin.getDestination().toString().equals(dest.getOrigin().toString())
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

	/**
	 * Main function of Flight Join class
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(FlightTwoLegJoin.class);
	    job.setMapperClass(ValidateFlightMapper.class);
	    //job.setCombinerClass(DoubleSumReducer.class);
	    job.setReducerClass(FlightsJoinReducer.class);
	    job.setOutputKeyClass(Text.class);
	    //job.setSortComparatorClass(KeyComparator.class);
	    //job.setGroupingComparatorClass(GroupComparator.class);
	    job.setOutputValueClass(Text.class);
	    job.setNumReduceTasks(10);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    int code = job.waitForCompletion(true) ? 0 : 1;
	    
	    if (code == 0) {
	    	double sum = 0;
	    	double count = 0;
			for (Counter counter : job.getCounters().getGroup(
					FlightsJoinReducer.FLIGHT_COUNTER_GROUP)) {
				if(counter.getDisplayName().equals("flight nums")){
					count = counter.getValue();
					System.out.println(counter.getDisplayName() + "\t\t" + counter.getValue());
				}
			}
			for (Counter counter : job.getCounters().getGroup(
					FlightsJoinReducer.FLIGHTSUM_COUNTER_GROUP)) {
				if(counter.getDisplayName().equals("flight delay sum")){
					sum = counter.getValue();
					System.out.println(counter.getDisplayName() + "\t\t" + counter.getValue());
				}
			}
			System.out.println("Average delay: \t\t" + (double)sum/count);
			
		}
	    
	    System.exit(code);
	}
}

