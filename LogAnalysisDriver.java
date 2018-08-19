import java.io.FileNotFoundException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class LogAnalysisDriver {
	public static void main(String[] args) throws Exception {
		if(args.length != 2){
			System.err.println("Usage: Log Analyzer <input path> <output path>");
			System.exit(2);
		}
		
		Job job = new Job();
	    job.setJarByClass(LogAnalysisDriver.class);
	    job.setJobName("Log Analyzer");
 	
	    //to accept the hdfs input and output dir at run time
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapperClass(LogAnalysisMapper.class);
	    job.setPartitionerClass(LogAnalysisPartitioner.class);
	    job.setReducerClass(LogAnalysisReducer.class);
	    
	    /**
	     * Set Reduce Tasks to 12 -> for 12 Months
	     */
	    job.setNumReduceTasks(12);

	    //setting the output data type classes
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    /**
	     * Retrieve counters
	     */
	    if(job.waitForCompletion(true)){
		    /**
		     * Write counters to HDFS
		     * 
		     */
		    Configuration config = new Configuration();     
		    FileSystem fs = FileSystem.get(config); 
		    Path filenamePath = new Path("Counter Results.txt");  
		    try {
			    long jpegRecords = job.getCounters().findCounter(LogAnalysisMapper.ImageCounter.JPEG).getValue();
			    long gifRecords = job.getCounters().findCounter(LogAnalysisMapper.ImageCounter.GIF).getValue();
			    long otherRecords = job.getCounters().findCounter(LogAnalysisMapper.ImageCounter.Others).getValue();
			    
			    /**
			     * Print counter results
			     */ 
			    System.out.println("JPEG hits :" + jpegRecords);
			    System.out.println("GIF hits :" + gifRecords);
			    System.out.println("Other hits :" + otherRecords);
			    
			    
			    /**
			     * Serialize counter results as bytes
			     */
		        if (fs.exists(filenamePath)) {
		            fs.delete(filenamePath, true);
		        }

		        FSDataOutputStream fin = fs.create(filenamePath);
		        fin.writeUTF("Jpeg Hits: ");
		        fin.writeLong(jpegRecords);
		        fin.writeUTF("Gif Hits: ");
		        fin.writeLong(gifRecords);
		        fin.writeUTF("\nOther Hits: ");
		        fin.writeLong(otherRecords);
		        fin.close();
		    
		    }catch(FileNotFoundException e){	
		    	e.printStackTrace();
		    }
	    }
	    
	    //Exit
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
 	}
}
