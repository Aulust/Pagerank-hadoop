package pagerank.hadoop.dataloader;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import pagerank.hadoop.Settings;
import pagerank.hadoop.types.Edge;
import pagerank.hadoop.types.Vertex;

public class DataLoader {
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
	    Configuration conf = new Configuration();
	    
	    conf.set("mapred.task.timeout", "6000000");
	    
	    FileSystem fs = FileSystem.get(conf);

	    Job job = new Job(conf, "Pagerank dataload");

	    job.setJarByClass(DataLoader.class);
	    
	    job.setMapperClass(LoaderMap.class);
	    job.setReducerClass(LoaderReduce.class);
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Edge.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Vertex.class);
	    
	    fs.delete(new Path(Settings.loaderFolder), true);
	    
	    FileInputFormat.addInputPath(job, new Path(Settings.dataFolder));
	    FileOutputFormat.setOutputPath(job, new Path(Settings.loaderFolder));
	    
	    job.waitForCompletion(true);
	}
}