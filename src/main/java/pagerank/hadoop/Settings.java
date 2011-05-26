package pagerank.hadoop;

public final class Settings {
	public final static float pagerank = (float) 0.15;
	
	public static final String dataFolder = "pagerank";
	public static final String loaderFolder = "graph";
	public static final String iterationFolder = "graph-temp";
	public static final String resultFolder = "result";
	
	public static final String tableName = "pagerank_result";
	public static final String columnFamily = "pagerank";
	public static final String columnQualifier = "value";
}