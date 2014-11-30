package hadoopPrograms.computeSemanticTrajectories;

/**
* @param args
*/

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import dataBases.postgres.PoiCharacteristicsTraj;
import dataBases.postgres.PostgreSQLFunctions;



public class ComputeSemanticTrajectories extends Configured implements Tool {
	    
	static class Map extends TableMapper<Text,Text>{
		private Text word = new Text();
		private Text value = new Text();
		private PoiCharacteristicsTraj poiChar = null;
		private ArrayList<PoiCharacteristicsTraj> poiList = null;
		protected void map( ImmutableBytesWritable key, Result result, Mapper.Context context) throws IOException, InterruptedException {
			poiList = new ArrayList<PoiCharacteristicsTraj>();
			boolean FLAG = false;
			boolean FLAG_X = false;
			boolean FLAG_Y = false;
			String[] temp;
			String del = "_";
			int user_id = -1;
			int counter = 0;
	            
	            
			System.out.println("Mapper");
			for (KeyValue kv : result.raw()) {
				String rowObj = new String(kv.getRow());
				String qualifierObj = new String(kv.getQualifier());
				String valueObj = new String(kv.getValue());
				word.set(rowObj);
				if (FLAG == false){
					temp = new String(kv.getRow()).split(del);
					user_id = Integer.parseInt(temp[0]);
					FLAG = true;
				}
				if ( qualifierObj.contains("x")){               //deutero to x
					if ( FLAG_X == false ){
						counter = 0;
						FLAG_X = true;
					}
					poiList.get(counter).setX(Double.parseDouble(valueObj));
				}
				else if (qualifierObj.contains("y")){
					if ( FLAG_Y == false ){
						counter = 0;
						FLAG_Y = true;
					}
					poiList.get(counter).setY(Double.parseDouble(valueObj)); //kai to trito to y kai stelnoume tin domi
				}
				else{                                           //prwta erxetai to timestamp
					poiChar = new PoiCharacteristicsTraj();
					poiChar.setArrived(valueObj);
					poiList.add(poiChar);
				}    
				counter++;
			}
	            
			for ( int i = 0 ; i < poiList.size() ; i++ ){
				context.write(word,poiList.get(i));
			}
		}	
	}
	    
	    
	public static class Reduce extends TableReducer<Text,PoiCharacteristicsTraj,ImmutableBytesWritable>{
		@Override
		protected void reduce(Text key, Iterable <PoiCharacteristicsTraj>values ,Context context) throws IOException, InterruptedException{
			PoiCharacteristicsTraj poiChar = null;
			ArrayList<PoiCharacteristicsTraj> poiList = new ArrayList<PoiCharacteristicsTraj>();
			boolean FLAG = false;
			int counter = 0;
			double distance = 0.0;
			double speed = 0.0;
			long diffTime = 0;
			PostgreSQLFunctions postgres = null;
			Connection con = null;
			String []temp;
			String del = "_";
			int user_id = -1;
			Date date = null;
			int seq_number = 1000;
	            
	            
			temp = key.toString().split(del);
			user_id = Integer.parseInt(temp[0]);
	            
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			java.util.Date parsedDate = null;
			try {
				parsedDate = dateFormat.parse(temp[1]);
			} catch (ParseException ex) {
				Logger.getLogger(ComputeSemanticTrajectories.class.getName()).log(Level.SEVERE, null, ex);
			}
			
			date = new java.sql.Date(parsedDate.getTime());
			for (PoiCharacteristicsTraj value : values ){
				poiChar = new PoiCharacteristicsTraj(value.getX(),value.getY(),value.getArrived());
				poiList.add(poiChar);
			}

			Collections.sort(poiList, new Comparator<PoiCharacteristicsTraj>() {
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				public int compare(PoiCharacteristicsTraj helper1,PoiCharacteristicsTraj helper2) {
					try {
						return dateFormat.parse(helper1.getArrived()).compareTo(dateFormat.parse(helper2.getArrived()));
					} catch (ParseException ex) {
						Logger.getLogger(ComputeSemanticTrajectories.class.getName()).log(Level.SEVERE, null, ex);
					}
					return 0;
				}
			});

			dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			while( FLAG == false){
				if ( (counter + 1) < poiList.size() ){
					parsedDate = null;
					try {
						parsedDate = dateFormat.parse(poiList.get(counter).getArrived());
					} catch (ParseException ex) {
						Logger.getLogger(ComputeSemanticTrajectories.class.getName()).log(Level.SEVERE, null, ex);
					} 
					java.sql.Timestamp first = new java.sql.Timestamp(parsedDate.getTime());
					
					try {
						parsedDate = dateFormat.parse(poiList.get(counter+1).getArrived());
					} catch (ParseException ex) {
						Logger.getLogger(ComputeSemanticTrajectories.class.getName()).log(Level.SEVERE, null, ex);
					}    
					java.sql.Timestamp second = new java.sql.Timestamp(parsedDate.getTime());
	                    
					distance = euclideanDistance(poiList.get(counter).getX(),poiList.get(counter).getY(),poiList.get(counter + 1).getX(),poiList.get(counter + 1).getY());
					diffTime = second.getTime() - first.getTime();
					diffTime = TimeUnit.MILLISECONDS.toSeconds(diffTime);
					speed = distance/diffTime;

					if ( speed <= 0.00005){
						poiList.get(counter).setOff(poiList.get(counter+1).getArrived());
						poiList.remove(counter + 1);
					}
					else{
						poiList.get(counter).setOff(poiList.get(counter+1).getArrived());
						counter ++;
					}
				}
				else{
					FLAG = true;
				}
			}

			for (int i = 0 ; i < poiList.size() ; i ++ ){
				System.out.println(poiList.get(i).toString());
			}
	            
			postgres = new PostgreSQLFunctions();
			try {
				con = postgres.OpenConnection();
			} catch (SQLException ex) {
				Logger.getLogger(ComputeSemanticTrajectories.class.getName()).log(Level.SEVERE, null, ex);
			}
	            
			int r = 50;
			for ( int i = 0 ; i < poiList.size() ; i ++){

				try {
					poiChar = postgres.findPOI(poiList.get(i),r,user_id);
				} catch (SQLException ex) {
					Logger.getLogger(ComputeSemanticTrajectories.class.getName()).log(Level.SEVERE, null, ex);
				}

				if (poiChar != null ){
					poiChar.setArrived(poiList.get(i).getArrived());
					poiChar.setOff(poiList.get(i).getOff());
					try {
						postgres.addTraj(poiChar, user_id, date,seq_number);
					} catch (SQLException ex) {
						Logger.getLogger(ComputeSemanticTrajectories.class.getName()).log(Level.SEVERE, null, ex);
					}
					seq_number = seq_number + 1000;
				} 
				else{
					System.out.println("NO POI");
				}
			}
	                    
			try {
				postgres.CloseConnection(con);
			} catch (SQLException ex) {
				Logger.getLogger(ComputeSemanticTrajectories.class.getName()).log(Level.SEVERE, null, ex);
			} 
		}
	        
		
		public double euclideanDistance(double x1, double y1, double x2, double y2){
			double euclDist = 0.0;
			double tempDist;
	        
			if ( x1 != x2 || y1 != y2 ){
				tempDist = Math.pow((x1-x2),2)+ Math.pow((y1-y2),2);
				euclDist = Math.sqrt(tempDist);
			}
			return euclDist; 
		}
	}
	    
	    
	    
	    
	    
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		/*conf.set("hbase.zookeeper.quorum", "83.212.105.7");
		conf.set("hbase.zookeeper.property.clientPort","2222");
		conf.set("hbase.master", "83.212.105.7:60000"); 
		conf.set("fs.defaultFS", "hdfs://83.212.105.7:9000");
		conf.set("mapred.job.tracker", "83.212.105.7:9001");*/
		Job job = new Job(conf, "");
		job.setJarByClass(ComputeSemanticTrajectories.class);     // class that contains mapper

		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR job// set other scan attrs

		TableMapReduceUtil.initTableMapperJob(
			"GPSTrajectories",        // input HBase table name
			scan,             // Scan instance to control CF and attribute selection
			ComputeSemanticTrajectories.Map.class,   // mapper
			Text.class,             // mapper output key
			PoiCharacteristicsTraj.class,             // mapper output value
			job
		);
	        
	        
	        
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(ComputeSemanticTrajectories.Map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setReducerClass(ComputeSemanticTrajectories.Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setNumReduceTasks(1);
	        
		TableMapReduceUtil.initTableReducerJob(
			"ProcessedGPSTrajectories",        // output table
			ComputeSemanticTrajectories.Reduce.class,    // reducer class
			job
		);
	            
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path("preprocessing"));
	        
		return job.waitForCompletion(true) ? 0 :1;
	}
	    
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ComputeSemanticTrajectories(), args);
		System.exit(res); 
	}
}
