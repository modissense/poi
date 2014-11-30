package hadoopPrograms.dbscan.computepartitions;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author jimakos
 */
public class InputFilesFunctions {
    private String dirName = null ;
    PointCoordinates pointCoor = null;
    PartitionCoordinates partitionCoor = null;
    Configuration conf;
    
    InputFilesFunctions( Configuration conf, String dirName ){
        this.dirName = dirName; 
        this.conf = conf;
    }
    
    String getDirName(){
        return dirName;
    }
    
    void setDirName( String dirName ){
        this.dirName = dirName;
    }
    
    ArrayList<PointCoordinates> ReadDataInputFile() throws FileNotFoundException, IOException{
        String str;
        String[] temp;
        String del= ",";
        double x,y;
        int counter = 1;
        ArrayList <PointCoordinates> arrList = new ArrayList<PointCoordinates>();
        
        
        System.out.println("dirName = " + dirName);
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] status = fs.listStatus(new Path(dirName));  // you need to pass in your hdfs path
        for (int i=0;i<status.length;i++){
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
            while ((str = br.readLine()) != null)   {   
                temp = str.split(del);
                if (temp.length > 2 ){
                    System.err.println("Error number of columns in InputFile"  );
                }
                else{
                    x = Double.parseDouble(temp[0]);
                    y = Double.parseDouble(temp[1]);
                    pointCoor = new PointCoordinates(counter,x,y);
                    arrList.add(pointCoor);
                    counter++;
                }
            }
        }
        return arrList;
    }
   
    
    ArrayList<PartitionCoordinates> ReadPartitionInputFile() throws FileNotFoundException, IOException{
        String str;
        String[] temp;
        String del= ",";
        BigDecimal x,y;
        ArrayList <PartitionCoordinates> arrList = new ArrayList<PartitionCoordinates>();
        
        Path pt=new Path(dirName);
        FileSystem fs = FileSystem.get(conf);
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
        
        while ((str = br.readLine()) != null)   {   
            temp = str.split(del);
            if (temp.length > 2 ){
                System.err.println("Error number of columns in InputFile");
            }
            else{             
                Double h = Double.parseDouble(temp[0]);
                BigDecimal p = new BigDecimal (h);
                
                x = new BigDecimal(Double.parseDouble(temp[0]));
                y = new BigDecimal(Double.parseDouble(temp[1]));
                partitionCoor = new PartitionCoordinates(x,y);
                arrList.add(partitionCoor);
            }
            
        }
        return arrList;
    }
    
    
    void PrintDataFileList( ArrayList <PointCoordinates> arrList){
        int i;
        double x,y;
        PointCoordinates pointCoor = null;
        
        for ( i = 0 ; i < arrList.size() ; i ++ ){
            pointCoor = arrList.get(i);
            x = pointCoor.getX();
            y = pointCoor.getY();
        }
    }
    
    
    void PrintPartitionFileList( ArrayList <PartitionCoordinates> arrList){
        int i;
        BigDecimal first,last;
        PartitionCoordinates partitionCoor = null;
        
        for ( i = 0 ; i < arrList.size() ; i ++ ){
            partitionCoor = arrList.get(i);
            first = partitionCoor.getFirst();
            last = partitionCoor.getLast();
        }
    }
    
}

