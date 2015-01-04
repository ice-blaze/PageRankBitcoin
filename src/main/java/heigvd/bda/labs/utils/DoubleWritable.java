package heigvd.bda.labs.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DoubleWritable implements WritableComparable<DoubleWritable> {

	private double value = 0.0;

	public DoubleWritable() {

	}

	public DoubleWritable(double value) {
		set(value);
	}

	public void readFields(DataInput in) throws IOException {
		value = in.readDouble();
	}

	public void write(DataOutput out) throws IOException {
		out.writeDouble(value);
	}

	public void set(double value) {
		this.value = value;
	}

	public double get() {
		return value;
	}

	public boolean equals(Object o) {
		if (!(o instanceof DoubleWritable)) {
			return false;
		}
		DoubleWritable other = (DoubleWritable) o;
		return this.value == other.value;
	}

	public int hashCode() {
		return (int) Double.doubleToLongBits(value);
	}

	public String toString() {
		return Double.toString(value);
	}

	public static class Comparator extends WritableComparator {
		public Comparator() {
			super(DoubleWritable.class);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			double thisValue = readDouble(b1, s1);
			double thatValue = readDouble(b2, s2);
			return (thisValue < thatValue ? 1 : (thisValue == thatValue ? 0 : -1));
		}
	}

	static {
		WritableComparator.define(DoubleWritable.class, new Comparator());
	}

	public int compareTo(DoubleWritable o) {
		return (value < o.value ? 1 : (value == o.value ? 0 : -1));
	}

}