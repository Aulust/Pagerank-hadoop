package pagerank.hadoop.dataloader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import pagerank.hadoop.types.Edge;
import pagerank.hadoop.types.Vertex;

public class LoaderReduce extends Reducer<Text, Edge, Text, Vertex> {
	private Vertex outputValue = new Vertex();

    public void reduce(Text key, Iterable<Edge> values, Context context) throws IOException, InterruptedException {
		ArrayList<Edge> _edges = new ArrayList<Edge>();
		
		for (Iterator<Edge> i = values.iterator(); i.hasNext(); ) {
			_edges.add(i.next());
		}
		
		outputValue.set(key.toString(), _edges);
		context.write(null, outputValue);
    }
}