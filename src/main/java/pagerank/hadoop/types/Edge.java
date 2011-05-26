package pagerank.hadoop.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Edge implements Writable {
	private String name;
	private float weight;
	
	public Edge() {}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public float getWeight() {
		return weight;
	}

	public void setWeight(float weight) {
		this.weight = weight;
	}

	public void set(String name, float weight) {
		this.name = name;
		this.weight = weight;
	}

	public void readFields(DataInput in) throws IOException {
		name = in.readUTF();
		weight = in.readFloat();
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeFloat(weight);
	}
	
    public static Edge read(DataInput in) throws IOException {
    	Edge w = new Edge();
    	w.readFields(in);
        return w;
	}
	
	public String toString() {
		return name + " " + Float.toString(weight);
	}
}