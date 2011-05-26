package pagerank.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class PrintResult {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "localhost");
		conf.set("hbase.zookeeper.property.clientPort","2181");
		
		HTable htable = new HTable(conf, Settings.tableName);

		Scan scan = new Scan();
		ResultScanner scanner = htable.getScanner(scan);
		Result r;
		while (((r = scanner.next()) != null)) {
			byte[] key = r.getRow();
			String userId = Bytes.toString(key);
			byte[] totalValue = r.getValue(Bytes.toBytes(Settings.columnFamily), Bytes.toBytes(Settings.columnQualifier));
			Float value = Bytes.toFloat(totalValue);
			
			System.out.println("node: " + userId+ ", pagerank: " + value);
		}
		scanner.close();
		htable.close();
	}
}
