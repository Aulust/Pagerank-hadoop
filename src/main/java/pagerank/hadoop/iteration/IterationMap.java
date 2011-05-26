package pagerank.hadoop.iteration;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import pagerank.hadoop.Settings;
import pagerank.hadoop.types.Edge;
import pagerank.hadoop.types.Vertex;

public class IterationMap extends Mapper<Object, Text, Text, FloatWritable> {
	private Text outputKey = new Text();
	private FloatWritable outputValue = new FloatWritable();
	private HTable htable;
	
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "localhost");
		conf.set("hbase.zookeeper.property.clientPort","2181");
		
		htable = new HTable(conf, Settings.tableName);
	}
	
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	Vertex _vertex = Vertex.read(value.toString());
    	
		Get getPagerank = new Get(Bytes.toBytes(_vertex.getName()));
		Result res = htable.get(getPagerank);
    	float pagerank = Bytes.toFloat(res.getColumnLatest(Bytes.toBytes(Settings.columnFamily), Bytes.toBytes(Settings.columnQualifier)).getValue());

		for (Iterator<Edge> i = _vertex.getEdges().iterator(); i.hasNext(); ) {
			Edge _edge = i.next();
			
			outputKey.set(_edge.getName());
			
			if(_vertex.getEdges().size() > 0)
				outputValue.set(pagerank * _edge.getWeight() / _vertex.getEdges().size());
			else
				outputValue.set(pagerank * _edge.getWeight());
			
			context.write(outputKey, outputValue);
		}
	}
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	htable.close();
    }
}