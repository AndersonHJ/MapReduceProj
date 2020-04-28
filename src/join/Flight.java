package join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class Flight implements Writable, WritableComparable{

	private Text flightNum;
	private Text origin;
	private Text destination;
	private LongWritable flightDate;
	private LongWritable arrTime;
	private LongWritable depTime;
	private DoubleWritable arrDelayMinutes;

	/**
	 * Origin, Dest, FlightDate, ArrTime, DepTime, Cancelled != 1, Diverted != 1, ArrDelayMinutes, FlightNum,
	 * Year, Month
	 */
	public Flight(String flightNum, String origin, String destination, long flightDate, long arrTime, long depTime,
			double arrDelayMinutes) {

		this.flightNum = new Text(flightNum);
		this.origin = new Text(origin);
		this.destination = new Text(destination);
		this.flightDate = new LongWritable(flightDate);
		this.arrTime = new LongWritable(arrTime);
		this.depTime = new LongWritable(depTime);
		this.arrDelayMinutes = new DoubleWritable(arrDelayMinutes);
	}
	
	public Flight(){
		this.flightNum = new Text();
		this.origin = new Text();
		this.destination = new Text();
		this.flightDate = new LongWritable();
		this.arrTime = new LongWritable();
		this.depTime = new LongWritable();
		this.arrDelayMinutes = new DoubleWritable();
	}

	/**
	 * @return the flightNum
	 */
	public Text getFlightNum() {
		return flightNum;
	}

	/**
	 * @return the origin
	 */
	public Text getOrigin() {
		return origin;
	}

	/**
	 * @return the destination
	 */
	public Text getDestination() {
		return destination;
	}

	/**
	 * @return the flightDate
	 */
	public LongWritable getFlightDate() {
		return flightDate;
	}

	/**
	 * @return the arrTime
	 */
	public LongWritable getArrTime() {
		return arrTime;
	}

	/**
	 * @return the depTime
	 */
	public LongWritable getDepTime() {
		return depTime;
	}

	/**
	 * @return the arrDelayMinutes
	 */
	public DoubleWritable getArrDelayMinutes() {
		return arrDelayMinutes;
	}
	
	
	public String toString() {
		return "data: " + this.flightNum + "\t" + this.origin + "\t" + this.destination + "\t" + this.arrTime + "\t" + this.depTime + "\t" + this.flightDate + "\t" + this.arrDelayMinutes;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.flightNum.readFields(arg0);
		this.origin.readFields(arg0);
		this.destination.readFields(arg0);
		this.flightDate.readFields(arg0);
		this.arrTime.readFields(arg0);
		this.depTime.readFields(arg0);
		this.arrDelayMinutes.readFields(arg0);
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		this.flightNum.write(arg0);
		this.origin.write(arg0);
		this.destination.write(arg0);
		this.flightDate.write(arg0);
		this.arrTime.write(arg0);
		this.depTime.write(arg0);
		this.arrDelayMinutes.write(arg0);
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}
}
