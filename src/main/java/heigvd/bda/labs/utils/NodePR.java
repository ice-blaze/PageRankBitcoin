package heigvd.bda.labs.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class NodePR implements Writable {

	private List<Integer> adjacency = new ArrayList<Integer>();
	private int id = -1;
	private double mass = 0;
	private double oldMass = 0;

	public NodePR(int id, double mass, String adjacency) {
		this.id = id;
		this.mass = mass;
		this.adjacency = getAdjacency(adjacency);
	}

	public NodePR(int id, double mass) {
		this.id = id;
		this.mass = mass;
	}

	public NodePR() {

	}

	public List<Integer> getAdjacency(String adj) {
		adjacency.clear();
		if (!adj.equals("")){

			String[] ids = adj.split(":");
			for (String id : ids) {
				adjacency.add(Integer.valueOf(id));
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
		for (Integer e : adjacency) {
			s.append(e);
			s.append(":".intern());
		}
		return s.toString();
	}

	static public NodePR fromString(String pr, String oldPR, String adjs) {
		NodePR n = new NodePR();
		n.mass = Double.valueOf(pr);
		n.oldMass = Double.valueOf(oldPR);
		n.adjacency = new ArrayList<Integer>();

		for (String adj : adjs.trim().split(":"))
			if (adj.trim() != "")
				n.adjacency.add(Integer.parseInt(adj));

		return n;
	}

	public void readFields(DataInput in) throws IOException {
		id = in.readInt();
		mass = in.readDouble();
		oldMass = in.readDouble();
		int len = in.readInt();
		adjacency.clear();
		for (int i = 0; i < len; i++) {
			adjacency.add(in.readInt());
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeDouble(mass);
		out.writeDouble(oldMass);
		out.writeInt(adjacency.size());
		for (Integer myLong : adjacency) {
			out.writeInt(myLong);
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

	public void setAdjacency(List<Integer> adj) {
		this.adjacency.clear();
		this.adjacency.addAll(adj);
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

	public List<Integer> getAdjacency() {
		return this.adjacency;
	}

	public List<Integer> getAdjacencyCopy() {
		return new ArrayList<Integer>(adjacency);
	}
}
