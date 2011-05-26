package pagerank.hadoop.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import pagerank.hadoop.Settings;
import pagerank.hadoop.types.Vertex;

public class SortMap extends Mapper<Object, Text, FloatWritable, Text> {
	private FloatWritable outputKey = new FloatWritable();
	private Text outputValue = new Text();
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

    	outputKey.set(pagerank);
    	outputValue.set(_vertex.getName());

    	context.write(outputKey, outputValue);
	}
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	htable.close();
    }
}
