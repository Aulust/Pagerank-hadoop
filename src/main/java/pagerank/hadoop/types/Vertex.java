package pagerank.hadoop.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class Vertex implements Writable {
	private String name;
	private List<Edge> edges;
	
	public Vertex() {}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<Edge> getEdges() {
		return edges;
	}

	public void setEdges(List<Edge> edges) {
		this.edges = edges;
	}

	public void set(String name, List<Edge> edges) {
		this.name = name;
		this.edges = edges;
	}

	public void readFields(DataInput in) throws IOException {
		name = in.readUTF();
		
		ArrayList<Edge> _edges = new ArrayList<Edge>();
		
		while(true) {
			try {
				_edges.add(Edge.read(in));
			} catch (Exception e) {
				break;
			}
		}
		
		edges = _edges;
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		
		for (Iterator<Edge> i = edges.iterator(); i.hasNext(); ) {
			Edge g = i.next();
			g.write(out);
		}
	}
	
	public static Vertex read(String in) {
		Vertex vertex = new Vertex();
		
		String[] data = in.split(" ");
		
		vertex.name = data[0];
		
		ArrayList<Edge> _edges = new ArrayList<Edge>();
		
		int i = 1;
		
		while(true) {
			try {
				Edge _edge = new Edge();
				_edge.set(data[i], Float.parseFloat(data[i+1]));
				_edges.add(_edge);
				i+=2;
			} catch (Exception e) {
				break;
			}
		}
		
		vertex.edges = _edges;
		
		return vertex;
	}
	
	/*public void readFields(String name, float pagerank, Iterable<Text> edges) throws IOException {
		this.name = name;
		this.pagerank = pagerank;
		
		ArrayList<Edge> _edges = new ArrayList<Edge>();
		
		for (Iterator<Text> i = edges.iterator(); i.hasNext(); ) {
			String g = i.next().toString();
			Edge _edge = new Edge();
			_edge.readFields(g);
			_edges.add(_edge);
		}
		
		this.edges = _edges;
	}*/
	
	public String toString() {
		String out = name;
		
		for (Iterator<Edge> i = edges.iterator(); i.hasNext(); ) {
			Edge g = i.next();
			out += " " + g.toString();
		}
		
		return out;
	}
}