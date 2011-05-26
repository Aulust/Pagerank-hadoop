package pagerank.hadoop.dataloader;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import pagerank.hadoop.types.Edge;

public class LoaderMap extends Mapper<Object, Text, Text, Edge> {
	private Text outputKey = new Text();
	private Edge outputValue = new Edge();
	
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	String[] data = value.toString().split(" ");
    	
    	outputKey.set(data[0]);
    	outputValue.set(data[1], Float.parseFloat(data[2]));
    	
		context.write(outputKey, outputValue);
	}
}