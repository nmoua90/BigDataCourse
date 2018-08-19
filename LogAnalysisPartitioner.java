import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;

public class LogAnalysisPartitioner extends Partitioner<Text, Text>{      

	public int getPartition(Text key, Text value, int numReduceTasks) {	
		Integer reducerNumber = 0;	
		
		//0 is default for January
		if(value.toString().contains("January")){}
		
		else if(value.toString().contains("February")) {		    
			reducerNumber = 1;	
		}else if(value.toString().contains("March")) {		    
			reducerNumber = 2;	
		}else if(value.toString().contains("April")) {		    
			reducerNumber = 3;	
		}else if(value.toString().contains("May")) {		    
			reducerNumber = 4;	
		}else if(value.toString().contains("June")) {		    
			reducerNumber = 5;	
		}else if(value.toString().contains("July")) {		    
			reducerNumber = 6;	
		}else if(value.toString().contains("August")) {		    
			reducerNumber = 7;	
		}else if(value.toString().contains("September")) {		    
			reducerNumber = 8;	
		}else if(value.toString().contains("October")) {		    
			reducerNumber = 9;	
		}else if(value.toString().contains("November")) {		    
			reducerNumber = 10;	
		}else if(value.toString().contains("December")) {		    
			reducerNumber = 11;	
		}
				
		return reducerNumber;    
	}
	
}
