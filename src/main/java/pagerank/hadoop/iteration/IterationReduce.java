package pagerank.hadoop.iteration;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import pagerank.hadoop.Settings;
import pagerank.hadoop.types.Vertex;

public class IterationReduce extends Reducer<Text, FloatWritable, Text, Vertex> {
	private HTable htable;
	
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "localhost");
		conf.set("hbase.zookeeper.property.clientPort","2181");
		
		htable = new HTable(conf, Settings.tableName);
	}
	
    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
	    float sum = 0;
	    
		for (Iterator<FloatWritable> i = values.iterator(); i.hasNext(); ) {
			FloatWritable _data = i.next();
			
			sum += _data.get();
		}

		float newPagerank = Settings.pagerank + (1 - Settings.pagerank) * sum;
		
		Put putPagerank = new Put(Bytes.toBytes(key.toString()));
		putPagerank.add(Bytes.toBytes(Settings.columnFamily), Bytes.toBytes(Settings.columnQualifier), Bytes.toBytes(newPagerank));
		
		htable.put(putPagerank);
	}
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	htable.close();
    }
}
