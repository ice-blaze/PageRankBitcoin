package heigvd.bda.labs.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class BitcoinAddress implements WritableComparable<BitcoinAddress> {
	public final static int SIZE = 20;
	byte[] address = new byte[SIZE];
	boolean empty = false;
	
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
	
	public void setEmpty(boolean b){
		empty = b;
	}
	
	public boolean isEmpty(){
		return empty;
	}
	
	public void set(String string){
		address = string.getBytes();
		if(address.length!=SIZE){
			System.out.println(string+" NOT 20 LENGTH !!!");
		}
	}
	
	
	public String toString(){//TODO should be 256 to 58
		byte[] tab = new byte[21];
		tab[0] = 0x00;
		for(int i=1;i<SIZE+1;i++){
			tab[i]=address[i-1];
		}
		
		
		
		MessageDigest md = null;
		try {
			md = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		byte[] tabResult = new byte[25];
		for(int i=0;i<21;i++){
			tabResult[i] = tab[i];
		}
		
	    byte[] checksum = md.digest(md.digest(tab));
	    tabResult[21] = checksum[0];
	    tabResult[22] = checksum[1];
	    tabResult[23] = checksum[2];
	    tabResult[24] = checksum[3];
	    
	    return Base58.encode(tabResult);
//		try {
//			return new String(address, "UTF-8");
//		} catch (UnsupportedEncodingException e) {
//			e.printStackTrace();
//			return "Address encoding error";
//		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeBoolean(empty);
		out.writeInt(getLength());
		for(int i=0;i<getLength();i++)
			out.writeByte(address[i]);
	}

	public void readFields(DataInput in) throws IOException {
		empty = in.readBoolean();
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
