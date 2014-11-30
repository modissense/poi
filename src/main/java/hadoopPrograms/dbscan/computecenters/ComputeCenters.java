package hadoopPrograms.dbscan.computecenters;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author jimakos
 */
public class ComputeCenters extends Configured implements Tool {

    /**
     * @param args the command line arguments
     */
    
    public static class MapComputeCenters extends Mapper<LongWritable,Text,Text,PointCoordinates>{
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            PointCoordinates pointCoor = null;
            String line = value.toString();
            
            if (line.contains(",")){
                StringTokenizer tokenizer = new StringTokenizer(line,",");
                String[] temp = null;
                int FLAG = 0;

                while (tokenizer.hasMoreTokens()) {                    
                    String token = tokenizer.nextToken();

                    if ( FLAG == 1 ){
                        pointCoor = new PointCoordinates();
                        pointCoor.setX(Double.parseDouble(token));
                    }
                    else if ( FLAG == 2 ){
                        pointCoor.setY(Double.parseDouble(token));
                    }
                    FLAG ++;
                }
                context.write(word, pointCoor);
            }
            else{
                word.set(line);
            }          
        }
    }
    
    
    public static class ReduceComputeCenters extends TableReducer<Text,PointCoordinates,ImmutableBytesWritable>{
        @Override
        protected void reduce(Text key, Iterable <PointCoordinates>values ,Context context) throws IOException, InterruptedException{
            int counter = 0;
            double counterX = 0.0;
            double counterY = 0.0;
            double pointX = 0.0;
            double pointY = 0.0;
            
            for (PointCoordinates value : values ){
                counterX = counterX + value.getX();
                counterY = counterY + value.getY();
                counter ++;    
            }
            
            pointX = counterX/counter;
            pointY = counterY/counter;
            
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.add(Bytes.toBytes("points"), Bytes.toBytes("x"), Bytes.toBytes(String.valueOf(pointX)));
            put.add(Bytes.toBytes("points"), Bytes.toBytes("y"), Bytes.toBytes(String.valueOf(pointY)));
            context.write(null, put);    
        }
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
            
        Job job = new Job(conf);
        job.setJarByClass(ComputeCenters.class);
       
        job.setOutputKeyClass(Integer.class);
        job.setOutputValueClass(PointCoordinates.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PointCoordinates.class);
        job.setMapperClass(MapComputeCenters.class);
        job.setReducerClass(ReduceComputeCenters.class);
        
        TableMapReduceUtil.initTableReducerJob(
	"DBScanResults",        // output table
	ReduceComputeCenters.class,    // reducer class
	job);
       
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        return job.waitForCompletion(true) ? 0 :1;
    }
    
    public static void main(String[] args) throws Exception {
        // TODO code application logic here
        int res = ToolRunner.run(new Configuration(), new ComputeCenters(), args);
        System.exit(res);
    }

    
}

