package hadoopPrograms.dbscan.MergeMappingMapReduceProgram;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.TreeSet;
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
/**
 *
 * @author jimakos
 */
public class MergeMappingMapReduce extends Configured implements Tool {

    /**
     * @param args the command line arguments
     */
    static public class MapperMergeMapping extends Mapper<LongWritable,Text,Text,PointCoordinates>{
        
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        PointCoordinates pointCoor = null;
        TreeSet <Integer> sortPartition = null;
        
        
        @Override
        protected void map( LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String []set = conf.getStrings("set");
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            String[] temp = null;
            String[] temp2 = null;
            String partitions = null;
            String file = null;
            int clusterPartition;
            int i;
            boolean FLAG = false;

            if ( fileName.contains("BorderFile") || fileName.contains("CorePointsFile")){
                if ( fileName.contains("BorderFile")){
                    file = "border";
                }
                else{
                    file = "core";
                }
                word.set(fileName);
                System.out.println("Before While");
                while (tokenizer.hasMoreTokens()) {
                    temp = tokenizer.nextToken().split(",");
                    for ( i = 0 ; i < temp.length ; i ++ ){
                        System.out.print(temp[i] + " ");
                    }
                    System.out.println();
                    partitions = temp[5];
                    if ( temp.length > 6 ){
                        sortPartition = new TreeSet<Integer>();
                        FLAG = true;
                    }
                    for ( i = 6 ; i < temp.length ; i ++ ){
                        partitions = partitions + "," + temp[i];
                        if ( FLAG == true ){
                            sortPartition.add(Integer.parseInt(temp[i]));
                        }
                    }
                    
                    
                    clusterPartition = Integer.parseInt(temp[3]);
                    temp2 = set[clusterPartition-1].split(" ");
                    
                    if ( file.equals("core")){
                        pointCoor = new PointCoordinates(Integer.parseInt(temp[0]), Double.parseDouble(temp[1]), Double.parseDouble(temp[2]),Integer.parseInt(temp[3]),Integer.parseInt(temp[4]), 0 , partitions + " C");
                        for ( i = 0 ; i < temp2.length ; i ++ ){
                            word.set(clusterPartition + temp2[i]);
                            context.write(word, pointCoor);
                        }
                        word.set("core");
                        context.write(word, pointCoor);
                    }
                    else{
                        pointCoor = new PointCoordinates(Integer.parseInt(temp[0]), Double.parseDouble(temp[1]), Double.parseDouble(temp[2]),Integer.parseInt(temp[3]),Integer.parseInt(temp[4]),0 ,partitions + " B");
                        for ( i = 0 ; i < temp2.length ; i ++ ){
                            word.set(temp2[i] + clusterPartition);
                            context.write(word, pointCoor);
                        }
                    }
                }
            }
        }
    }
   
    
    static public class ReducerMergeMapping extends Reducer<Text,PointCoordinates,OutputMergeDetails,OutputMergeDetails>{
        private MultipleOutputs<Text,IntWritable> multipleOutputs;
        private ArrayList<PointCoordinates> corePoints = null;
        private ArrayList<PointCoordinates> borderPoints = null;
        PointCoordinates pointCoor = null;
        PointCoordinates borderPoint = null;
        PointCoordinates corePoint = null;
        OutputMergeDetails firstCluster = null;
        OutputMergeDetails secondCluster = null;

   
        
        protected void reduce(Text key, Iterable <PointCoordinates> values ,Context context) throws IOException, InterruptedException{
            corePoints = new ArrayList<PointCoordinates>();
            boolean FLAG = false;
            int i,j;
            String partitions = null;
            
            if ( !key.toString().equals("core")){
                for ( PointCoordinates value : values ){
                    System.out.println(value.toString());
                    if ( value.getPartitions().contains("C")){
                        partitions = value.getPartitions().replace("C","");
                        value.setPartitions(partitions);
                        pointCoor = new PointCoordinates(value.getId(), value.getX(), value.getY(),value.getClusterPartition(),value.getNCLuster(), value.getPartition(),value.getPartitions());
                        corePoints.add(pointCoor);
                    }
                    else{
                        partitions = value.getPartitions().replace("B","");
                        value.setPartitions(partitions);
                        if ( FLAG == false){
                            borderPoints = new ArrayList<PointCoordinates>();
                            pointCoor = new PointCoordinates(value.getId(), value.getX(), value.getY(), value.getClusterPartition(), value.getNCLuster(),value.getPartition(),value.getPartitions());
                            borderPoints.add(pointCoor);
                            FLAG  = true;
                        }
                        else{
                            pointCoor = new PointCoordinates(value.getId(), value.getX(), value.getY(), value.getClusterPartition(), value.getNCLuster(),value.getPartition(),value.getPartitions());
                            borderPoints.add(pointCoor);
                        }
                    }
                }


                if ( borderPoints != null ){
                    for( i = 0 ; i < corePoints.size() ; i ++ ){
                        corePoint = corePoints.get(i);
                        firstCluster = new OutputMergeDetails(corePoint.getClusterPartition(),corePoint.getNCLuster());
                        for ( j = 0; j < borderPoints.size() ; j++ ){
                            borderPoint = borderPoints.get(j);
                            if ( corePoint.getId() == borderPoint.getId() ) {
                                secondCluster = new OutputMergeDetails(borderPoint.getClusterPartition(),borderPoint.getNCLuster());
                                context.write(firstCluster,secondCluster);
                                secondCluster = null;
                                borderPoints.remove(j);
                                j--;
                            }
                        }    
                    } 
                }
            }
            else{
                for ( PointCoordinates value : values ){
                    System.out.println(value.toString());
                    firstCluster = new OutputMergeDetails(value.getClusterPartition(),value.getNCLuster());
                    context.write(firstCluster,null);
                    firstCluster = null;
                }
            }   
        }   
    }
    
    public int run (String[] args) throws Exception{
        /*int sizeY = 2;
        int numberOfPartitions = 4;
        String []strArray = null;
        Configuration conf = new Configuration();
        
        
        FindNeighborPartitions neighborPart = new FindNeighborPartitions(sizeY,numberOfPartitions);
        strArray = neighborPart.find();

        conf.setStrings("set", strArray);
        conf.set("mapred.textoutputformat.separator","<->");
        Job job = new Job(conf);
        job.setJarByClass(MergeMappingMapReduce.class);

        job.setOutputKeyClass(Integer.class);
        job.setOutputValueClass(MergeMappingMapReduceProgram.OutputMergeDetails.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MergeMappingMapReduceProgram.PointCoordinates.class);
        job.setMapperClass(MergeMappingMapReduce.MapperMergeMapping.class);
        job.setReducerClass(MergeMappingMapReduce.ReducerMergeMapping.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        return job.waitForCompletion(true) ? 0 :1;*/
        return 0;
        
    }
    
    public static void main(String[] args) throws Exception {
        // TODO code application logic here
        int exitCode = ToolRunner.run(new Configuration(),new MergeMappingMapReduce() , args);
        System.exit(exitCode);
    }
}

