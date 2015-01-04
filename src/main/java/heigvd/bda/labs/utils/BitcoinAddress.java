package heigvd.bda.labs.utils;

public class BitcoinAddress {
	byte[] address = new byte[25];
	
	public BitcoinAddress(String str){
		address = str.getBytes();
		if(address.length!=25)	
			System.out.println("WARNING ADDRESS NOT 25 !!!!!!!!!!!!");
			System.out.println("WARNING ADDRESS NOT 25 !!!!!!!!!!!!");
			System.out.println("WARNING ADDRESS NOT 25 !!!!!!!!!!!!");
	}
}
