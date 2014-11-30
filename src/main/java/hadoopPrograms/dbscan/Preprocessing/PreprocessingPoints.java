package hadoopPrograms.dbscan.Preprocessing;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.StringTokenizer;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
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

    public class PreprocessingPoints extends Configured implements Tool {
     

        static public class Map extends TableMapper<Text,IntWritable>{
            private final static IntWritable one = new IntWritable(1);
            private Text word = new Text();
            CoordinateAttributes coor = null;
            
            protected void map( ImmutableBytesWritable key, Result result, Mapper.Context context) throws IOException, InterruptedException {
                System.out.println("i am here");
                System.out.println("key = " + key.get());
                
                System.out.println("value = " + result);
                for (KeyValue kv : result.raw()) {
                    String qualifier = new String(kv.getQualifier());
                    String value = new String(kv.getValue());
                    System.out.println(  new String(kv.getQualifier()) + "," + new String(kv.getValue())  );
                    if ( qualifier.contains("x")){
                        word.set("X," + value);
                        coor = new CoordinateAttributes(Double.parseDouble(value),1);
                        context.write(word,one);
                    }
                    else if (qualifier.contains("y")){
                        word.set("Y," + value);
                        coor = new CoordinateAttributes(Double.parseDouble(value),1);
                        context.write(word,one);
                    }
                }
            
            }
        }
    
      
        public static class Reduce extends Reducer<Text, IntWritable, Text,IntWritable>{

            private MultipleOutputs<Text,IntWritable> multipleOutputs;
        
            protected void setup( Context context) {
                multipleOutputs = new MultipleOutputs<>(context);
            }
            
            protected void reduce(Text key, Iterable <IntWritable> values ,Context context) throws IOException, InterruptedException{
                int sum = 0;


                for (IntWritable value : values ){
                    sum = sum + 1 ;    
                }

                context.write(key,new IntWritable(sum));
            }
            
            protected void cleanup(Context context) throws IOException, InterruptedException{
                multipleOutputs.close();
            }
        }
        
        @Override
        public int run(String[] args) throws Exception {
           /* 
            Configuration conf = new Configuration();
            conf.setInt("x", Integer.parseInt(args[2]));
            conf.setInt("y", Integer.parseInt(args[3]));
            conf.setInt("numOfCoord", Integer.parseInt(args[4]));
            
            conf.set("mapred.textoutputformat.separator"," ");
            
            Job job = new Job(conf);
            job.setJarByClass(getClass());
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setMapperClass(Map.class);
            //job.setCombinerClass(Reduce.class);
            job.setMapOutputKeyClass(Text.class);
            job.setReducerClass(Reduce.class);
            job.setOutputKeyClass(Text.class);
            job.setInputFormatClass(TextInputFormat.class);
 	    job.setOutputFormatClass(TextOutputFormat.class);
            
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            return job.waitForCompletion(true) ? 0 :1;
            
            /////////////////////////////
            /*double eps = 0.0;
            ComputePartitions compPart = new ComputePartitions(args[0] , args[1] + "/partitionX-r-00000",args[1] + "/partitionY-r-00000",eps,conf);
            compPart.startComputation();*/

            return 0;
        }
        
        
        public static void main(String[] args) throws Exception {
            int res = ToolRunner.run(new Configuration(), new PreprocessingPoints(), args);
            System.exit(res);            
        }
    }
