package heigvd.bda.labs.utils;

import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.BytesWritable;

public class BitcoinAddress2 extends BytesWritable {
	static final int SIZE = 20;
	
	public BitcoinAddress2(byte[] tab){
		super(tab);
	}
	
	public BitcoinAddress2(){
		super();
	}
	
	public void setString(String string){
		set(string.getBytes()); 
	}
	
	public void set(byte[] tab){
		this.set(tab,0,tab.length);
		if(this.getBytes().length!=SIZE){
			System.out.println("NOT 20 LENGTH !!!");
			System.out.println(toString());
		}
	}
	
	
	public String toString(){//TODO should be 256 to 58
		try {
			return new String(getBytes(), "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			return "Address encoding error";
		}
	}
}
