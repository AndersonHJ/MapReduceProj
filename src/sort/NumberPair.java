package sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author Shiqi Luo
 *
 */
public class NumberPair implements WritableComparable<NumberPair> {
	private double first = 0;
	private int second = 0;
	
	public NumberPair(){}
	
	public NumberPair(double first, int second){

		this.set(first, second);
	}
	
	/**
	* Set the left and right values.
	*/
	public void set(double left, int right) {
		first = left;
		second = right;
	}
	
	public double getFirst() {
		return first;
	}
	
	public int getSecond() {
		return second;
	}
	
	/**
	* Read the two integers. 
	* Encoded as: MIN_VALUE -> 0, 0 -> -MIN_VALUE, MAX_VALUE-> -1
	*/
	@Override
	public void readFields(DataInput in) throws IOException {
		first = in.readDouble() + Double.MIN_VALUE;
		second = in.readInt() + Integer.MIN_VALUE;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(first - Double.MIN_VALUE);
		out.writeInt(second - Integer.MIN_VALUE);
	}
	
	@Override
	public int hashCode() {
		return (int)(first * 157) + second;
	}
	
	@Override
	public boolean equals(Object right) {
		if (right instanceof NumberPair) {
			NumberPair r = (NumberPair) right;
			return r.first == first && r.second == second;
		}
		else {
			return false;
		}
	}
	
	/** A Comparator that compares serialized IntPair. */ 
	public static class Comparator extends WritableComparator {
		public Comparator() {
			super(NumberPair.class);
		}
	
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return compareBytes(b1, s1, l1, b2, s2, l2);
		}
	}
	
	static { // register this comparator
		WritableComparator.define(NumberPair.class, new Comparator());
	}
	
	@Override
	public int compareTo(NumberPair o) {
		if (first != o.first) {
			return first < o.first ? -1 : 1;
		} 
		else if (second != o.second) {
			return second < o.second ? -1 : 1;
		} 
		else {
			return 0;
		}
	}
}
