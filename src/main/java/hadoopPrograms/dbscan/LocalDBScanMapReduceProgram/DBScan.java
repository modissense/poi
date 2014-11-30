package hadoopPrograms.dbscan.LocalDBScanMapReduceProgram;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


/**
 *
 * @author jimakos
 */

/*Auti i class periexei tis sunartisei pou xreiaazomaste gia ton algorithmo DBScan*/
public class DBScan {
    double eps = 0;
    int minPts = 0;
    ArrayList <PointCoordinates> inputPoints;
    ArrayList<PointCoordinates> noise;
    MultipleOutputs<Text,IntWritable> multipleOutputs;
    int nCluster;
    
    
    /*Constructor*/
    public DBScan( ArrayList <PointCoordinates> inputPoints, double eps, int minPts, MultipleOutputs<Text,IntWritable> multipleOutputs, int nCluster) {
        this.eps = eps;
        this.minPts = minPts;
        this.multipleOutputs = multipleOutputs;
        this.inputPoints = inputPoints;
        this.nCluster = nCluster;
        noise = new ArrayList<PointCoordinates>();
    }
    
    
    /*ulopoiisi tou algorithmou DBScan*/
    public int algorithm() throws IOException, InterruptedException{
        System.out.println("DBSCAN");
        int i = 0 ;
        PointCoordinates pointCoor;
        ArrayList<PointCoordinates> neighborhood;

        for ( i = 0 ; i < inputPoints.size() ; i ++ ) {
            pointCoor = (PointCoordinates) inputPoints.get(i);
            if ( pointCoor.getVisited() == false ){
                pointCoor.setVisited();
                neighborhood = region(pointCoor);
                if ( neighborhood.size() < minPts){
                    //noise.add(pointCoor);
                }
                else{
                    nCluster ++;
                    pointCoor.setCluster();
                    pointCoor.setNCLuster(nCluster);
                    multipleOutputs.write("CorePointsFile",pointCoor,null);
                    expandCluster(pointCoor,neighborhood,nCluster);
                }
            }
        }
        return nCluster;
    }
    
    /*i sunartisi auti dimiourgei ta clusters*/
    public void expandCluster( PointCoordinates pointCoor, ArrayList<PointCoordinates> neighborhood, int nCluster ) throws IOException, InterruptedException{
        System.out.println("Expand Cluster");
        ArrayList <PointCoordinates> cluster = new ArrayList<PointCoordinates>();
        ArrayList <PointCoordinates> neighborhoodTemp = new ArrayList <PointCoordinates>();
        PointCoordinates pointCoorTemp;
        PointCoordinates pointCoorTemp1,pointCoorTemp2;
        boolean FLAG = false;
        boolean borderPoint = false;
        int i,j,k;
       
        cluster.add(pointCoor);
       
        for( i = 0 ; i < neighborhood.size() ; i ++ ){
            pointCoorTemp = neighborhood.get(i);
            
            if ( pointCoorTemp.getVisited() == false ){
                pointCoorTemp.setVisited();
                neighborhoodTemp = region(pointCoorTemp);
                if ( neighborhoodTemp.size() >= minPts ){
                    neighborhood.addAll(neighborhoodTemp);
                }
                else{//border point
                    borderPoint = true;
                }
            }
             if( pointCoorTemp.getCluster() != true ){
                pointCoorTemp.setCluster();
                pointCoorTemp.setNCLuster(nCluster);
                cluster.add(pointCoorTemp);
            }
            
            if (borderPoint == true){//borderPoint
                multipleOutputs.write("BorderFile", pointCoorTemp,null);
            }
            borderPoint = false;
        }
        writeToFile(cluster,"cluster",nCluster);
    }
    
    /*I sunartisi auti epistrefei ta geitones enos simeiou*/
    public ArrayList region( PointCoordinates pointCoor ){
       ArrayList<PointCoordinates> neighborhood = new ArrayList<PointCoordinates>();
       PointCoordinates pointCoorTemp = null;
       double euclDist = 0.0;
       int i = 0; 
       
       for ( i = 0 ; i < inputPoints.size() ; i ++ ) {
            pointCoorTemp = (PointCoordinates) inputPoints.get(i);
            euclDist = euclideanDistance( pointCoor.getX(), pointCoor.getY(), pointCoorTemp.getX(), pointCoorTemp.getY());
            if ( euclDist <= eps  ){
                neighborhood.add( pointCoorTemp );
            }
        }   
        return neighborhood;
    }
    
     /*eukleidia sunartisi*/
     public Double euclideanDistance( double x1, double y1, double x2, double y2 ) {
        double euclDist = 0.0;
        double tempDist;
        
        
        if ( x1 != x2 || y1 != y2 ){
            tempDist = Math.pow((x1-x2),2)+ Math.pow((y1-y2),2);
            euclDist = Math.sqrt(tempDist);
        }
        return euclDist;
    } 
  
    public void printList(ArrayList<PointCoordinates> list,String name){
        PointCoordinates pointCoor;
        int i ;
        
        System.out.println(name + ":");
        if (!list.isEmpty()){
            for ( i = 0 ; i < list.size() ; i ++) {
                pointCoor = (PointCoordinates) list.get(i);
                System.out.println("\tPoint: " + i + ", x = " + pointCoor.getX() + " , y = " + pointCoor.getY() + " , visited = " + pointCoor.getVisited());
            }
        }
        else{
            System.out.println("\tnone");
        }
    }
    
    public void writeToFile(ArrayList<PointCoordinates> list,String name, int nCluster ) throws IOException, InterruptedException{
        int i;
        PointCoordinates pointCoor;
        
        multipleOutputs.write("DBScan","cluster : " + nCluster ,null);
        if (!list.isEmpty()){
            for ( i = 0 ; i < list.size() ; i ++) {
                pointCoor = (PointCoordinates) list.get(i);
                multipleOutputs.write("DBScan", pointCoor,null);
            }
        }
        else{
            System.out.println("\tnone");
        }
    }
}

