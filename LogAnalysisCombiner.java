import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class LogAnalysisCombiner extends Reducer<Text, Text, Text, Text> {

	@Override
	  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	       int monthHits = 0;
	       
 	       for (Text months : values) {
 	    	   monthHits ++;
 	       }

 	       String value = Integer.toString(monthHits);
 	       
 	       context.write(key, new Text(value));
 	} 	

}
