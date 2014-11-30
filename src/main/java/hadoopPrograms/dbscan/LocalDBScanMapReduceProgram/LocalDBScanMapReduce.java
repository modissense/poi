package hadoopPrograms.dbscan.LocalDBScanMapReduceProgram;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LocalDBScanMapReduce extends Configured implements Tool {

    /**
     * @param args the command line arguments
     */
    public static class MapperDBScan extends Mapper<LongWritable,Text,Text,PointCoordinates>{
        
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        PointCoordinates pointCoor;
        
        @Override
        protected void map( LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            //int minPts= conf.getInt("minPts",0);
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String filename = fileSplit.getPath().getName();
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            String[] temp = null;
            int clusterPartition;
            int i;
            String partitions;
            
            temp = filename.split("-|\\.");
            clusterPartition = Integer.parseInt(temp[1]);
            word.set(temp[1]);

            while (tokenizer.hasMoreTokens()) {
                temp = tokenizer.nextToken().split(",");
                partitions = temp[3];
                
                for ( i = 4 ; i < temp.length ; i ++ ){
                  partitions = partitions + "," + temp[i]; 
                }
                
                pointCoor = new PointCoordinates(Integer.parseInt(temp[0]), Double.parseDouble(temp[1]), Double.parseDouble(temp[2]),clusterPartition, partitions);
                System.out.println("Mapper = " + pointCoor.toString());
                context.write(word, pointCoor);
            }
        }
    }
   
    
    public static class ReducerDBScan extends Reducer<Text,PointCoordinates,Text,IntWritable>{
        private MultipleOutputs<Text,IntWritable> multipleOutputs;
        ArrayList <PointCoordinates> inputPoints;
        DBScan dbscan;
        int nCluster = 0;
        double eps = 0.0001;
        int minPts = 100;
        private Text word = new Text();
        
        protected void setup( Context context) {
            multipleOutputs = new MultipleOutputs<>(context);
        }
        
        protected void reduce(Text key, Iterable <PointCoordinates> values ,Context context) throws IOException, InterruptedException{
            Configuration conf = context.getConfiguration();
            //int minPts= conf.getInt("minPts",0);
            inputPoints = new ArrayList<PointCoordinates>();
            PointCoordinates p;
            
            for ( PointCoordinates value : values ){
                p = new PointCoordinates(value.getId(), value.getX(), value.getY(),value.getClusterPartition(),value.getPartitions());
                inputPoints.add(p);
                System.out.println("Reducer = " + p.toString());
            }
            System.out.println("DBSCAN Starting");
            dbscan = new DBScan(inputPoints,eps,minPts,multipleOutputs,nCluster);
            dbscan.algorithm();
            word.set("ok");
            context.write(word, null);
        }
        
        protected void cleanup(Context context) throws IOException, InterruptedException{
            multipleOutputs.close();
        }
        
    }
    
    public int run (String[] args) throws Exception{
       /* Job job = new Job(getConf());
        job.setJarByClass(getClass());

        job.setOutputKeyClass(Integer.class);
        job.setOutputValueClass(PointCoordinates.class);
        job.setMapperClass(MapperDBScan.class);
        job.setMapOutputKeyClass(Text.class);
        job.setReducerClass(ReducerDBScan.class);
        //job.setOutputKeyClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        MultipleOutputs.addNamedOutput(job, "DBScan", TextOutputFormat.class, PointCoordinates.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "BorderFile", TextOutputFormat.class, PointCoordinates.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "CorePointsFile", TextOutputFormat.class, PointCoordinates.class, Text.class);
        return job.waitForCompletion(true) ? 0 :1;*/
        return 0;
    }
    
    public static void main(String[] args) throws Exception {
        // TODO code application logic here
        int exitCode = ToolRunner.run(new Configuration(),new LocalDBScanMapReduce() , args);
        System.exit(exitCode);
    }
}

