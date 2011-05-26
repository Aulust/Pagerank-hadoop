package pagerank.hadoop.dataloader;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import pagerank.hadoop.Settings;
import pagerank.hadoop.types.Vertex;

public class TableInitializationMap extends Mapper<Object, Text, Text, Text> {
	private HTable htable;
	
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "localhost");
		conf.set("hbase.zookeeper.property.clientPort","2181");
		
		htable = new HTable(conf, Settings.tableName);
	}
	
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	Vertex _vertex = Vertex.read(value.toString());
    	
		Put putPagerank = new Put(Bytes.toBytes(_vertex.getName()));
		putPagerank.add(Bytes.toBytes(Settings.columnFamily), Bytes.toBytes(Settings.columnQualifier), Bytes.toBytes(Settings.pagerank));
		
		htable.put(putPagerank);
	}
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	htable.close();
    }
}
