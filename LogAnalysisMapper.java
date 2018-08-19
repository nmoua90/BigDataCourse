import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class LogAnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {
		/**
		 * Added Counter variables here 
		 */ 
		public static enum ImageCounter {GIF, JPEG, Others};
	
 	   @Override
 	   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
 		   String[] allLinesInFile = value.toString().split("\n");
 		   String[] allWordsInLine;
 		   String ipAddress;
 		   Text idParam = new Text();
 		   Text valParam = new Text();
 		    
 		   for(int i = 0; i<allLinesInFile.length; i++){
 			   //Split all words of one line
 			   allWordsInLine = allLinesInFile[i].split(" ");
 			   
 			   //Find ip address of a line
 			   ipAddress = allWordsInLine[0];  
 			   idParam.set(ipAddress);
 			   
 			   //Find out what month the line corresponds to
 			   if(allLinesInFile[i].contains("/Jan/"))
 				   valParam.set("January 1");
 			   else if(allLinesInFile[i].contains("/Feb/"))
 				   valParam.set("February 1");
 			   else if(allLinesInFile[i].contains("/Mar/"))
 				   valParam.set("March 1");
 			   else if(allLinesInFile[i].contains("/Apr/"))
 				   valParam.set("April 1");
 			   else if(allLinesInFile[i].contains("/May/"))
 				   valParam.set("May 1");
 			   else if(allLinesInFile[i].contains("/Jun/"))
 				   valParam.set("June 1");
 			   else if(allLinesInFile[i].contains("/Jul/"))
 				   valParam.set("July 1");
 			   else if(allLinesInFile[i].contains("/Aug/"))
 				   valParam.set("August 1");
 			   else if(allLinesInFile[i].contains("/Sep/"))
 				   valParam.set("September 1");
 			   else if(allLinesInFile[i].contains("/Oct/"))
 				   valParam.set("October 1");
 			   else if(allLinesInFile[i].contains("/Nov/"))
 				   valParam.set("November 1");
 			   else
 				   valParam.set("December 1");
 			   
 			   context.write(idParam, valParam);
 		    }
 	
 		   //Increment Counters
 		   for(int i = 0; i<allLinesInFile.length; i++){
		    	if(allLinesInFile[i].contains(".jpeg")){
		    		context.getCounter(ImageCounter.JPEG).increment(1);
		    	}
		    	else if(allLinesInFile[i].contains("gif"))
		    		context.getCounter(ImageCounter.GIF).increment(1);
			    else
			    	context.getCounter(ImageCounter.Others).increment(1);
		    }
 		   
 	   	}//end of map method
 }