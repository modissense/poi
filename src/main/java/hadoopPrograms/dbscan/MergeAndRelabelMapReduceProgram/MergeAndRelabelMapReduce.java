package hadoopPrograms.dbscan.MergeAndRelabelMapReduceProgram;

/**
*
* @author jimakos
*/



import hadoopPrograms.dbscan.BuildGlobalMapping.BuildGlobalMapping;
import hadoopPrograms.dbscan.MergeMappingMapReduceProgram.FindNeighborPartitions;
import hadoopPrograms.dbscan.computepartitions.ComputePartitions;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
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



public class MergeAndRelabelMapReduce extends Configured implements Tool {

   /**
    * @param args the command line arguments
    */
   
   static class MapperMergeAndRelabel extends Mapper<LongWritable,Text,LongWritable,OutputPoints>{
       private final static IntWritable one = new IntWritable(1);
       private LongWritable word = new LongWritable() ;
       OutputPoints point = null;
        
       protected void map( LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           Configuration conf = context.getConfiguration();
           String []glClusters = conf.getStrings("glClusters");
                   
           String line = value.toString();
           StringTokenizer tokenizer = new StringTokenizer(line);
           String []temp;
           String partAndClus = null;
           String str = null;
           int i;
           
           while (tokenizer.hasMoreTokens()) {
               str = tokenizer.nextToken();
               if ( str.contains(",")){
                   temp = str.split(",");
                   point = new OutputPoints(Integer.parseInt(temp[0]),Double.parseDouble(temp[1]), Double.parseDouble(temp[2]));
                   partAndClus = temp[3] + temp[4];
                   for ( i = 0 ; i < glClusters.length ; i ++ ){
                       if (glClusters[i].contains(partAndClus)){
                           i = i + 1;
                           word.set(i);
                           break;
                       }
                   }
                   context.write(word, point);
               }
           }
       }
   }
   
   
   static class ReducerMergeAndRelabel extends Reducer<LongWritable,OutputPoints,Text,OutputPoints>{
       private MultipleOutputs<Text,Text> multipleOutputs;
       private Text word = new Text();
       
       protected void reduce(LongWritable key, Iterable <OutputPoints> values ,Context context) throws IOException, InterruptedException{
           
           word.set("cluster " + key);
           context.write(word, null);
           for ( OutputPoints value : values ){
               System.out.println("point = " + value.toString());
               context.write(null, value);
           }
       }
   }
   

   public int run(String[] args) throws Exception {
           String []glClusters = null;

           for (int i = 0; i < args.length ; i ++){
               System.out.println(i + " : " + args[i]);
           }
           System.out.println("Preprocessing");
           Configuration conf = new Configuration();
           conf.set("mapred.textoutputformat.separator"," ");
           Job job = new Job(conf, "ExampleRead");
           job.setJarByClass(hadoopPrograms.dbscan.Preprocessing.PreprocessingPoints.class);     // class that contains mapper

           Scan scan = new Scan();
           scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
           scan.setCacheBlocks(false);  // don't set to true for MR job// set other scan attrs

           TableMapReduceUtil.initTableMapperJob(
               "GPSTrajectories",        // input HBase table name
               scan,             // Scan instance to control CF and attribute selection
               hadoopPrograms.dbscan.Preprocessing.PreprocessingPoints.Map.class,   // mapper
               Text.class,             // mapper output key
               IntWritable.class,             // mapper output value
           job);
           job.setOutputKeyClass(Text.class);
           job.setOutputValueClass(IntWritable.class);
           job.setMapperClass(hadoopPrograms.dbscan.Preprocessing.PreprocessingPoints.Map.class);
           job.setMapOutputKeyClass(Text.class);
           job.setReducerClass(hadoopPrograms.dbscan.Preprocessing.PreprocessingPoints.Reduce.class);
           job.setOutputKeyClass(Text.class);

           FileInputFormat.addInputPath(job, new Path(args[0]));
           FileOutputFormat.setOutputPath(job, new Path("preprocessing"));
           job.waitForCompletion(true);
       
           System.out.println("Preprocessing");
       
           Configuration conf2 = getConf();
           conf2.setInt("x", Integer.parseInt(args[2]));
           conf2.setInt("y", Integer.parseInt(args[3]));
           conf2.setInt("numOfCoord", Integer.parseInt(args[4]));
           
           
           Job job2 = new Job(conf2);
           job2.setNumReduceTasks(2);
           job2.setJarByClass(hadoopPrograms.dbscan.Preprocessing.PreprocessingPoints2.class);
           job2.setOutputKeyClass(Text.class);
           job2.setOutputValueClass(hadoopPrograms.dbscan.Preprocessing.PartitionsDetails.class);
           job2.setMapOutputValueClass(hadoopPrograms.dbscan.Preprocessing.CoordinateAttributes.class);
           job2.setMapperClass(hadoopPrograms.dbscan.Preprocessing.PreprocessingPoints2.Map.class);
           job2.setMapOutputKeyClass(Text.class);
           job2.setReducerClass(hadoopPrograms.dbscan.Preprocessing.PreprocessingPoints2.Reduce.class);
           job2.setOutputKeyClass(Text.class);
           job2.setInputFormatClass(TextInputFormat.class);
           job2.setOutputFormatClass(TextOutputFormat.class);
           MultipleOutputs.addNamedOutput(job2, "partitionX", TextOutputFormat.class, hadoopPrograms.dbscan.Preprocessing.PartitionsDetails.class, Text.class);
           MultipleOutputs.addNamedOutput(job2, "partitionY", TextOutputFormat.class, hadoopPrograms.dbscan.Preprocessing.PartitionsDetails.class, Text.class);
           
           FileInputFormat.addInputPath(job2, new Path("/home/hduser/preprocessing"));
           FileOutputFormat.setOutputPath(job2, new Path(args[1]));
           job2.waitForCompletion(true);
           
            /////////////////////////////////////////////////////////////////////
           double eps = 0.0001;
           
           String inputPathDfs = "hdfs://master:9000" + args[0] ;
           String outputPathDfs ="hdfs://master:9000/home/hduser/output/";
           
           System.out.println("inputPathDfs = " + inputPathDfs);
           System.out.println("Compute Paritions");
           ComputePartitions compPart = new ComputePartitions(inputPathDfs , outputPathDfs + "partitionX-r-00001", outputPathDfs + "partitionY-r-00000",eps,conf);
           compPart.startComputation();
           
           Configuration conf6 = getConf();
           
           conf6.addResource(new Path("/opt/hadoop-1.1.2/conf/core-site.xml"));
           conf6.addResource(new Path("/opt/hadoop-1.1.2/conf/hdfs-site.xml"));

           FileSystem fs = FileSystem.get(conf6);
           fs.copyFromLocalFile(new Path("/opt/hadoop-1.1.2/partition"),new Path("/home/hduser/partition"));
           
           
           /////////////////////////////////////////////////////////////////////
           System.out.println("LocalDBScan");
           Configuration conf3 = getConf();     
           conf3.setInt("minPts", Integer.parseInt(args[6]));
           Job job3 = new Job(conf3);
           job3.setJarByClass(hadoopPrograms.dbscan.LocalDBScanMapReduceProgram.LocalDBScanMapReduce.class);
           job3.setNumReduceTasks(4);
           job3.setOutputKeyClass(Integer.class);
           job3.setOutputValueClass(hadoopPrograms.dbscan.LocalDBScanMapReduceProgram.PointCoordinates.class);
           job3.setMapperClass(hadoopPrograms.dbscan.LocalDBScanMapReduceProgram.LocalDBScanMapReduce.MapperDBScan.class);
           job3.setMapOutputKeyClass(Text.class);
           job3.setReducerClass(hadoopPrograms.dbscan.LocalDBScanMapReduceProgram.LocalDBScanMapReduce.ReducerDBScan.class);
           FileInputFormat.addInputPaths(job3, "hdfs://master:9000/home/hduser/partition");
           FileOutputFormat.setOutputPath(job3, new Path("/home/hduser/LocalDBScan"));
           MultipleOutputs.addNamedOutput(job3, "DBScan", TextOutputFormat.class, hadoopPrograms.dbscan.LocalDBScanMapReduceProgram.PointCoordinates.class, Text.class);
           MultipleOutputs.addNamedOutput(job3, "BorderFile", TextOutputFormat.class, hadoopPrograms.dbscan.LocalDBScanMapReduceProgram.PointCoordinates.class, Text.class);
           MultipleOutputs.addNamedOutput(job3, "CorePointsFile", TextOutputFormat.class, hadoopPrograms.dbscan.LocalDBScanMapReduceProgram.PointCoordinates.class, Text.class);

           job3.waitForCompletion(true);


           /////////////////////////////////////////////////////////////////////
           System.out.println("MergeMapping");
           int sizeY = 2;
           int numberOfPartitions = 4;
           String []strArray = null;
           
           FindNeighborPartitions neighborPart = new FindNeighborPartitions(sizeY,numberOfPartitions);
           strArray = neighborPart.find();

           Configuration conf4 = getConf();
           conf4.setStrings("set", strArray);
           conf4.set("mapred.textoutputformat.separator","<->");
           Job job4 = new Job(conf4);
           job4.setJarByClass(hadoopPrograms.dbscan.MergeMappingMapReduceProgram.MergeMappingMapReduce.class);
           job4.setOutputKeyClass(Integer.class);
           job4.setOutputValueClass(hadoopPrograms.dbscan.MergeMappingMapReduceProgram.OutputMergeDetails.class);
           job4.setMapOutputKeyClass(Text.class);
           job4.setMapOutputValueClass(hadoopPrograms.dbscan.MergeMappingMapReduceProgram.PointCoordinates.class);
           job4.setMapperClass(hadoopPrograms.dbscan.MergeMappingMapReduceProgram.MergeMappingMapReduce.MapperMergeMapping.class);
           job4.setReducerClass(hadoopPrograms.dbscan.MergeMappingMapReduceProgram.MergeMappingMapReduce.ReducerMergeMapping.class);
           FileSystem hdfs = FileSystem.get(conf4);
           Path path = new Path("/home/hduser/LocalDBScan/BorderFile-r-00000");
           boolean isExists = hdfs.exists(path);
           if ( isExists == true){
               FileInputFormat.setInputPaths(job4,"/home/hduser/LocalDBScan/BorderFile-r-00000,/home/hduser/LocalDBScan/BorderFile-r-00001,/home/hduser/LocalDBScan/BorderFile-r-00002,/home/hduser/LocalDBScan/BorderFile-r-00003,/home/hduser/LocalDBScan/CorePointsFile-r-00000,/home/hduser/LocalDBScan/CorePointsFile-r-00001,/home/hduser/LocalDBScan/CorePointsFile-r-00002,/home/hduser/LocalDBScan/CorePointsFile-r-00003");
           }
           else{
               FileInputFormat.setInputPaths(job4,"/home/hduser/LocalDBScan/CorePointsFile-r-00000,/home/hduser/LocalDBScan/CorePointsFile-r-00001,/home/hduser/LocalDBScan/CorePointsFile-r-00002,/home/hduser/LocalDBScan/CorePointsFile-r-00003");
           }
           FileOutputFormat.setOutputPath(job4, new Path("/home/hduser/MergePartitions"));
   
           job4.waitForCompletion(true);
           
           /////////////////////////////////////////////////////////////////////
           System.out.println("GlobalMapping");
           String PathDfs = "hdfs://master:9000/home/hduser/"; 
           BuildGlobalMapping buildGlMap = new BuildGlobalMapping(PathDfs + "/MergePartitions/part-r-00000",PathDfs + "/GlobalMapping", conf);
           buildGlMap.Compute();

           /////////////////////////////////////////////////////////////////////
           System.out.println("MergeMappingMapReduce");
           ReadGlobalClusters glClus = new ReadGlobalClusters(PathDfs + "GlobalMapping",conf);
           glClusters = glClus.read();
           
           Configuration conf5 = getConf();
           
           conf5.setStrings("glClusters",glClusters);
           Job job5 = new Job(conf5);
           job5.setJarByClass(MergeAndRelabelMapReduce.class);

           job5.setOutputKeyClass(Text.class);
           job5.setOutputValueClass(hadoopPrograms.dbscan.MergeAndRelabelMapReduceProgram.OutputPoints.class);
           job5.setMapOutputKeyClass(LongWritable.class);
           job5.setMapOutputValueClass(hadoopPrograms.dbscan.MergeAndRelabelMapReduceProgram.OutputPoints.class);
           job5.setMapperClass(MergeAndRelabelMapReduce.MapperMergeAndRelabel.class);
           job5.setReducerClass(MergeAndRelabelMapReduce.ReducerMergeAndRelabel.class);
           FileInputFormat.setInputPaths(job5,"/home/hduser/LocalDBScan/DBScan-r-00000,/home/hduser/LocalDBScan/DBScan-r-00001,/home/hduser/LocalDBScan/DBScan-r-00002,/home/hduser/LocalDBScan/DBScan-r-00003");
           FileOutputFormat.setOutputPath(job5, new Path("/home/hduser/DBScanClusters"));
          
           job5.waitForCompletion(true);

           Configuration conf7 = getConf();
           Job job7 = new Job(conf7);
           job7.setJarByClass(hadoopPrograms.dbscan.computecenters.ComputeCenters.class);
      
           job7.setOutputKeyClass(Integer.class);
           job7.setOutputValueClass(hadoopPrograms.dbscan.computecenters.PointCoordinates.class);
           job7.setMapOutputKeyClass(Text.class);
           job7.setMapOutputValueClass(hadoopPrograms.dbscan.computecenters.PointCoordinates.class);
           job7.setMapperClass(hadoopPrograms.dbscan.computecenters.ComputeCenters.MapComputeCenters.class);
           job7.setReducerClass(hadoopPrograms.dbscan.computecenters.ComputeCenters.ReduceComputeCenters.class);

           TableMapReduceUtil.initTableReducerJob(
           "DBScanResults",        // output table
           hadoopPrograms.dbscan.computecenters.ComputeCenters.ReduceComputeCenters.class,    // reducer class
           job);

           FileInputFormat.setInputPaths(job7,new Path(args[0]));
           FileOutputFormat.setOutputPath(job7, new Path(args[1]));

           return job7.waitForCompletion(true) ? 0 :1;
   }
    
   
   public static void main(String[] args) throws Exception {
       
       int res = ToolRunner.run(new Configuration(), new MergeAndRelabelMapReduce(), args);
       System.exit(res);
       
   }
}

