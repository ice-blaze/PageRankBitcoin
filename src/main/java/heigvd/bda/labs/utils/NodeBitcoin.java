package heigvd.bda.labs.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

public class NodeBitcoin implements Writable {

	private List<BitcoinAddress> adjacency = new ArrayList<BitcoinAddress>();
	private float mass = 0;
	private float oldMass = 0;
	private boolean dang = true;

	
	public void setUnDang(){
		this.dang = false;
	}
	public void setDang(){
		this.dang = true;
	}
	public boolean isDang(){
		return this.dang==true;
	}
	public boolean isUnDang(){
		return !isDang();
	}

	public NodeBitcoin() {

	}

//	public List<BytesWritable> getAdjacency(String adj) {
//		adjacency.clear();
//		if (!adj.equals("")){
//
//			String[] ids = adj.split(":");
//			for (String id : ids) {
//				adjacency.add(Integer.valueOf(id));
//			}
//		}
//
//		return adjacency;
//	}

	public void clear() {
		adjacency.clear();
	}

	public String toString() {
		StringBuilder s = new StringBuilder();
		s.append(String.valueOf(mass) + "\t".intern());
		s.append(String.valueOf(oldMass));
		if (adjacency.size() > 0) {
			s.append("\t".intern());
		}
		for (BitcoinAddress e : adjacency) {
			s.append(e);
			s.append(":".intern());
		}
		return s.toString();
	}
	
	

//	static public NodeBitcoin fromString(String pr, String oldPR, String adjs) {
//		NodeBitcoin n = new NodeBitcoin();
//		n.mass = Double.valueOf(pr);
//		n.oldMass = Double.valueOf(oldPR);
//		n.adjacency = new ArrayList<BitcoinAddress>();
//
//		for (String adj : adjs.trim().split(":"))
//			if (adj.trim() != "")
//				n.adjacency.add(new BitcoinAddress(adj));
//
//		return n;
//	}

	public void readFields(DataInput in) throws IOException {
		mass = in.readFloat();
		oldMass = in.readFloat();
		dang = in.readBoolean();
		int len = in.readInt();
		adjacency.clear();
		for (int i = 0; i < len; i++) {
			BitcoinAddress toto = new BitcoinAddress();
			toto.readFields(in);
			adjacency.add(toto);
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeFloat(mass);
		out.writeFloat(oldMass);
		out.writeBoolean(dang);
		out.writeInt(adjacency.size());
		for (BitcoinAddress myLong : adjacency) {
			myLong.write(out);
		}
	}

	public void setMass(float mass) {
		this.mass = mass;
	}

//	public void setAdjacency(String adj) {
//		this.adjacency = getAdjacency(adj);
//	}
	
	public void clearSetAdjacency(List<BitcoinAddress> adj) {
		this.adjacency.clear();
		this.adjacency.addAll(adj);
	}
	
	public void clearAdja(){
		adjacency.clear();
	}
	
	public void addAdja(String str){
		if(str.trim().length()>0)
			adjacency.add(new BitcoinAddress(str));
	}
	
	public void addAdja(BitcoinAddress b){
		adjacency.add(b);
	}

	public float getMass() {
		return this.mass;
	}

	public float getOldMass() {
		return this.oldMass;
	}

	public void setOldMass(float oldMass) {
		this.oldMass = oldMass;
	}

	public List<BitcoinAddress> getAdjacency() {
		return this.adjacency;
	}

	public List<BitcoinAddress> getAdjacencyCopy() {
		return new ArrayList<BitcoinAddress>(adjacency);
	}

	public void addAdja(byte[] byteTab) {
		addAdja(new BitcoinAddress(byteTab));
	}
}
