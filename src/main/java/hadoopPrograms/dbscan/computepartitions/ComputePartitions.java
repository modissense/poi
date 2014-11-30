package hadoopPrograms.dbscan.computepartitions;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author jimakos
 */
public class ComputePartitions {

    /**
     * @param args the command line arguments
     */
    private String dirName = null;
    private String partitionXFile = null;
    private String partitionYFile = null;
    private double eps ;
    Configuration conf;
    
    
    public ComputePartitions(String dirName, String partitionXFile, String partitionYFile, double eps, Configuration conf  ){
        this.dirName = dirName;
        this.partitionXFile = partitionXFile;
        this.partitionYFile = partitionYFile;
        this.eps = eps;
        this.conf = conf;
    }
    
    public void startComputation() throws FileNotFoundException, IOException {
        // TODO code application logic here
        String outputFile = "partition";
        int numberOfPartitions = 0;
        ArrayList <PointCoordinates> arrListData = null;
        ArrayList <PartitionCoordinates> arrListXPart = null;
        ArrayList <PartitionCoordinates> arrListYPart = null;
        
        
	File file = new File("/opt/hadoop-1.1.2/partition");
	if (!file.exists()) {
            if (file.mkdir()) {
                System.out.println("Directory is created!");
            } 
            else {
		System.out.println("Failed to create directory!");
            }
	}
               
        
        InputFilesFunctions inputFile = new InputFilesFunctions(conf,dirName);
        arrListData = inputFile.ReadDataInputFile();
        
        inputFile = new InputFilesFunctions(conf,partitionXFile);
        arrListXPart = inputFile.ReadPartitionInputFile();
        
        inputFile = new InputFilesFunctions(conf,partitionYFile);
        arrListYPart = inputFile.ReadPartitionInputFile();
        
        numberOfPartitions = arrListXPart.size() * arrListYPart.size();
     
        FindPartititions findPartitions = new FindPartititions(arrListData, arrListXPart, arrListYPart, outputFile, numberOfPartitions,eps);
        findPartitions.createOutputFiles();
        findPartitions.Calculate();
        findPartitions.WritePartitions();
    }
}

