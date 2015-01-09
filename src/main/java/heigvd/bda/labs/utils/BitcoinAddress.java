package heigvd.bda.labs.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class BitcoinAddress implements WritableComparable<BitcoinAddress> {
	public final static int SIZE = 20;
	byte[] address = new byte[SIZE];
	
	public BitcoinAddress(String str){// wrong
		set(str);
	}
	
	public BitcoinAddress(byte[] tab){
		address = tab;
	}
	
	public BitcoinAddress(){
	}
	
	public void set(BitcoinAddress a){
		address = a.address.clone();
	}
	
	public void set(String string){
		address = string.getBytes();
		if(address.length!=SIZE){
			System.out.println(string+" NOT 20 LENGTH !!!");
		}
	}
	
	
	public String toString(){//TODO should be 256 to 58
		try {
			return new String(address, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			return "Address encoding error";
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(getLength());
		for(int i=0;i<getLength();i++)
			out.writeByte(address[i]);
	}

	public void readFields(DataInput in) throws IOException {
		int length = in.readInt();
		for(int i=0;i<length;i++)
			address[i] = in.readByte();
	}

	public int getLength() {
		return address.length;
	}

	public byte[] getBytes() {
		return address;
	}

	public int compareTo(BitcoinAddress o) {
		if (this == o)
		      return 0;
		    return WritableComparator.compareBytes(getBytes(), 0, getLength(),
		             o.getBytes(), 0, o.getLength());
	}
	
	public int compareTo(byte[] other, int off, int len) {
	    return WritableComparator.compareBytes(getBytes(), 0, getLength(),
	             other, off, len);
	  }
	
	public boolean equals(Object other) {
	    if (!(other instanceof BitcoinAddress))
	      return false;
	    BitcoinAddress that = (BitcoinAddress)other;
	    if (this.getLength() != that.getLength())
	      return false;
	    return this.compareTo(that) == 0;
	  }
	
	public int hashCode() {
	    return WritableComparator.hashBytes(getBytes(), getLength());
	  }
}
