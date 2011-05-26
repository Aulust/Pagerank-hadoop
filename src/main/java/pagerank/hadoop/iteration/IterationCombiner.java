package pagerank.hadoop.iteration;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IterationCombiner extends Reducer<Text, FloatWritable, Text, FloatWritable> {
	private FloatWritable outputValue = new FloatWritable();
	
    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
	    float sum = 0;
	    
		for (Iterator<FloatWritable> i = values.iterator(); i.hasNext(); ) {
			FloatWritable _data = i.next();
			
			sum += _data.get();
		}
		
		outputValue.set(sum);

		context.write(key, outputValue);
	}
}
