package pagerank.hadoop.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import pagerank.hadoop.Settings;

public class Sort {
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
	    Configuration conf = new Configuration();
	    
	    FileSystem fs = FileSystem.get(conf);

	    Job job = new Job(conf, "Pagerank sort");

	    job.setJarByClass(Sort.class);
	    
	    job.setMapperClass(SortMap.class);
	    job.setReducerClass(Reducer.class);
	    
	    job.setOutputKeyClass(FloatWritable.class);
	    job.setOutputValueClass(Text.class);
	    job.setSortComparatorClass(DescendingFloatComparator.class);
	    
	    FileInputFormat.addInputPath(job, new Path(Settings.loaderFolder));
	    fs.delete(new Path(Settings.resultFolder), true);
	    FileOutputFormat.setOutputPath(job, new Path(Settings.resultFolder));
	    
	    job.waitForCompletion(true);
	}
	
	private static class DescendingFloatComparator extends FloatWritable.Comparator {  
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {  
			return -super.compare(b1, s1, l1, b2, s2, l2);
		}
		@SuppressWarnings("unchecked")
		public int compare(WritableComparable a, WritableComparable b) {
			return -super.compare(a, b);
		}
	}
}
