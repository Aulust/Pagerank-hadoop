package pagerank.hadoop.dataloader;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import pagerank.hadoop.Settings;

public class TableInitialization {
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
	    Configuration conf = new Configuration();
	    
	    FileSystem fs = FileSystem.get(conf);

	    Job job = new Job(conf, "Pagerank dable initialisation");

	    job.setJarByClass(TableInitialization.class);
	    
	    job.setMapperClass(TableInitializationMap.class);
	    
	    fs.delete(new Path(Settings.iterationFolder), true);
	    
	    FileInputFormat.addInputPath(job, new Path(Settings.loaderFolder));
	    FileOutputFormat.setOutputPath(job, new Path(Settings.iterationFolder));
	    
	    job.waitForCompletion(true);
	}
}
