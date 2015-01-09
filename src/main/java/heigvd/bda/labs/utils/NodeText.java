package heigvd.bda.labs.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class NodeText implements Writable {

	private List<String> adjacency = new ArrayList<String>();
	private int id = -1;
	private double mass = 0;
	private double oldMass = 0;
	private boolean dang = true;

	public NodeText(int id, double mass, String adjacency) {
		this.id = id;
		this.mass = mass;
		this.adjacency = getAdjacency(adjacency);
	}

	public NodeText(int id, double mass) {
		this.id = id;
		this.mass = mass;
	}

	public NodeText() {

	}
	
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

	public List<String> getAdjacency(String adj) {
		adjacency.clear();
		if (!adj.equals("")){

			String[] ids = adj.split(":");
			for (String id : ids) {
				adjacency.add(id);
			}
		}

		return adjacency;
	}

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
		for (String e : adjacency) {
			s.append(e);
			s.append(":".intern());
		}
		return s.toString();
	}

	static public NodeText fromString(String pr, String oldPR, String adjs) {
		NodeText n = new NodeText();
		n.mass = Double.valueOf(pr);
		n.oldMass = Double.valueOf(oldPR);
		n.adjacency = new ArrayList<String>();

		for (String adj : adjs.trim().split(":"))
			if (adj.trim() != "")
				n.adjacency.add(adj);

		return n;
	}

	public void readFields(DataInput in) throws IOException {
		id = in.readInt();
		mass = in.readDouble();
		oldMass = in.readDouble();
		int len = in.readInt();
		adjacency.clear();
		for (int i = 0; i < len; i++) {
			adjacency.add(in.readUTF());
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeDouble(mass);
		out.writeDouble(oldMass);
		out.writeInt(adjacency.size());
		for (String myLong : adjacency) {
			out.writeUTF(myLong);
		}
	}

	public int getID() {
		return this.id;
	}

	public void setID(int id) {
		this.id = id;
	}

	public void setMass(double mass) {
		this.mass = mass;
	}

	public void setAdjacency(String adj) {
		this.adjacency = getAdjacency(adj);
	}

	public void clearSetAdjacency(List<String> adj) {
		this.adjacency.clear();
		this.adjacency.addAll(adj);
	}
	
	public void addAdja(String str){
		if(str.trim().length()>0)
			this.adjacency.add(str);
	}
	
	public void clearAdja(){
		this.adjacency.clear();
	}

	public double getMass() {
		return this.mass;
	}

	public double getOldMass() {
		return this.oldMass;
	}

	public void setOldMass(double oldMass) {
		this.oldMass = oldMass;
	}

	public List<String> getAdjacency() {
		return this.adjacency;
	}

	public List<String> getAdjacencyCopy() {
		return new ArrayList<String>(adjacency);
	}
}
