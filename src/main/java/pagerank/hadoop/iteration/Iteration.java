package pagerank.hadoop.iteration;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import pagerank.hadoop.Settings;
import pagerank.hadoop.types.Vertex;

public class Iteration {
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
	    Configuration conf = new Configuration();
	    
	    FileSystem fs = FileSystem.get(conf);

	    Job job = new Job(conf, "Pagerank iteration");

	    job.setJarByClass(Iteration.class);
	    
	    job.setMapperClass(IterationMap.class);
	    job.setCombinerClass(IterationCombiner.class);
	    job.setReducerClass(IterationReduce.class);
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(FloatWritable.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Vertex.class);
	    
	    fs.delete(new Path(Settings.iterationFolder), true);
	    
	    FileInputFormat.addInputPath(job, new Path(Settings.loaderFolder));
	    FileOutputFormat.setOutputPath(job, new Path(Settings.iterationFolder));
	    
	    job.waitForCompletion(true);
	}
}
