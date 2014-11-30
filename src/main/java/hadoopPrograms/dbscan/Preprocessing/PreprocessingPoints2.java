package hadoopPrograms.dbscan.Preprocessing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
public class PreprocessingPoints2 extends Configured implements Tool {

    /**
     * @param args the command line arguments
     */
    
    public static class Map extends Mapper<LongWritable,Text,Text,CoordinateAttributes>{
        private final static IntWritable one = new IntWritable(1);
            private Text word = new Text();
            CoordinateAttributes coor = null;
            

            @Override
            protected void map( LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString();
                StringTokenizer tokenizer = new StringTokenizer(line,",| ");
                int i = 1;
                double coordinate = 0.0;
                int counter;

                while (tokenizer.hasMoreTokens()) {
                    String token = tokenizer.nextToken();
                    if ( i == 1 ){
                        word.set(token);
                    }
                    else if ( i == 2 ){
                        coordinate = Double.parseDouble(token);
                    }
                    else{
                        counter = Integer.parseInt(token);
                        coor = new CoordinateAttributes(coordinate,counter);
                    }
                    i++;
                }
                context.write(word, coor);
            }
    }
    
    
    public static class Reduce extends Reducer<Text,CoordinateAttributes,Text,PartitionsDetails>{
        private MultipleOutputs<Text,PartitionsDetails> multipleOutputs;
        
        protected void setup( Context context) {
            multipleOutputs = new MultipleOutputs<>(context);
        }
            
        protected void reduce(Text key, Iterable <CoordinateAttributes> values ,Context context) throws IOException, InterruptedException{
            CoordinateAttributes coord1 = null;
            CoordinateAttributes coord2 = new CoordinateAttributes();
            CoordinateAttributes coord3 = null;
            PartitionsDetails partitions = null;
            ArrayList <CoordinateAttributes> arr = new ArrayList<CoordinateAttributes>();
            int sum = 0;
            int numOfPartitions;
            int currentNumOfPartitions = 1;
            double firstPoint = 0;
            double maxNumOfPoints;
            boolean FLAG = true;
            boolean FLAG2 = true;
            boolean FLAGCoord = false;
            double coordinate= 0;
            int counter = 0;
            int x;
            int y;
            int numOfCoord;
                               
            Configuration conf = context.getConfiguration();
            x = conf.getInt("x",0);
            y = conf.getInt("y",0);
            numOfCoord = conf.getInt("numOfCoord",0);

            if ( key.find("X") == 0 ){
                maxNumOfPoints = (double)numOfCoord/x;
                numOfPartitions = x;
                FLAGCoord = true;
            }
            else{
                maxNumOfPoints = (double)numOfCoord/y;
                numOfPartitions = y;
                FLAGCoord = false;
            }
                
                
                
            for (CoordinateAttributes value : values ){
                coord1 = new CoordinateAttributes(value.getCoord(),value.getCounter());
                arr.add(coord1);
                
            }
            coord1 = null;
            Collections.sort(arr, new Comparator<CoordinateAttributes>() {
                public int compare(CoordinateAttributes helper1, CoordinateAttributes helper2) {
                    return Double.compare(helper1.getCoord(), helper2.getCoord());
                }
            });
               
            for (int i = 0 ; i < arr.size() ; i ++ ){
                coord3 = arr.get(i);
                coordinate = coord3.getCoord();
                counter = coord3.getCounter();
                if ( currentNumOfPartitions < numOfPartitions ){
                    if (FLAG == true){
                        firstPoint = coordinate;
                        FLAG = false;
                    }
                    
                    if (sum <= maxNumOfPoints ) {
                        sum = sum + counter;
                        FLAG2 = false;    
                    }
                    else{
                        currentNumOfPartitions ++;
                        partitions = new PartitionsDetails(firstPoint,coord2.getCoord());
                        if(FLAGCoord == true){
                            multipleOutputs.write("partitionX", null, partitions);
                        }
                        else{
                            multipleOutputs.write("partitionY", null, partitions);
                        }
                        sum = 0;
                        firstPoint = coord2.getCoord();
                        sum = sum + counter;
                        FLAG2 = true;
                        if ( i == (arr.size() - 1) ){
                            FLAG2 = false;
                            break;
                        }
                    }
                }
                else{
                    FLAG2 = false;    
                }
                coord2.setCoordinate(coordinate);
                    
            }
                
            if ( FLAG2 == false ){
                partitions = new PartitionsDetails(firstPoint,coordinate);
                if(FLAGCoord == true){
                    multipleOutputs.write("partitionX", null, partitions);
                }
                else{
                    multipleOutputs.write("partitionY", null, partitions);
                }
            }
            System.out.println("FLAG2 = " + FLAG2);           
        }
        

        protected void cleanup(Context context) throws IOException, InterruptedException{
            multipleOutputs.close();
        }
        
    }
    

    @Override
    public int run(String[] args) throws Exception {
        /*
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator"," ");
        Job job = new Job(conf);
        job.setJarByClass(PreprocessingPoints.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(PreprocessingPoints.Map.class);
        //job.setCombinerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setReducerClass(PreprocessingPoints.Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
            
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("preprocessing"));
        job.waitForCompletion(true);
        
        
        Configuration conf2 = getConf();
        conf2.setInt("x", Integer.parseInt(args[2]));
        conf2.setInt("y", Integer.parseInt(args[3]));
        conf2.setInt("numOfCoord", Integer.parseInt(args[4]));
            
            
        Job job2 = new Job(conf2);
        job2.setNumReduceTasks(2);
        job2.setJarByClass(PreprocessingPoints2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(PartitionsDetails.class);
        job2.setMapOutputValueClass(CoordinateAttributes.class);
        job2.setMapperClass(PreprocessingPoints2.Map.class);
        //job.setCombinerClass(Reduce.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setReducerClass(PreprocessingPoints2.Reduce.class);
        job2.setOutputKeyClass(Text.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        MultipleOutputs.addNamedOutput(job2, "partitionX", TextOutputFormat.class, PartitionsDetails.class, Text.class);
        MultipleOutputs.addNamedOutput(job2, "partitionY", TextOutputFormat.class, PartitionsDetails.class, Text.class);
            
        FileInputFormat.addInputPath(job2, new Path("preprocessing"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        job2.waitForCompletion(true);
            
        ////////////////////////////////////////////////////////
        double eps = 0.0;
        ComputePartitions compPart = new ComputePartitions(args[0] , args[1] + "/partitionX-r-00000",args[1] + "/partitionY-r-00000",eps,conf);
        compPart.startComputation();
        */
        return 0;
    }
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PreprocessingPoints2(), args);
        System.exit(res);
    }
}
