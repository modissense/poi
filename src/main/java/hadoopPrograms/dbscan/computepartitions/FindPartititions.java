package hadoopPrograms.dbscan.computepartitions;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author jimakos
 */
public class FindPartititions {
    private ArrayList<PointCoordinates> arrListData = null;
    private ArrayList<PartitionCoordinates> arrListXPart = null;
    private ArrayList<PartitionCoordinates> arrListYPart = null;
    private String outputFile = null;        
    private int numberOfPartitions = 0;
    private BigDecimal eps;
    private int roundNumber = 5;
    
    
    FindPartititions(ArrayList<PointCoordinates> arrListData, ArrayList<PartitionCoordinates> arrListXPart, ArrayList<PartitionCoordinates> arrListYPart, String outputFile , int numberOfPartitions, Double eps ) throws IOException{
        this.arrListData = arrListData;
        this.arrListXPart = arrListXPart;
        this.arrListYPart = arrListYPart;
        this.outputFile = outputFile;
        this.numberOfPartitions = numberOfPartitions;
        this.eps = new BigDecimal(eps);
        this.eps = this.eps.round(new MathContext(roundNumber, RoundingMode.HALF_UP));
        
    }
    
    
    void createOutputFiles() throws IOException{
        int i;
        File file;
        for ( i = 1 ; i <= numberOfPartitions ; i ++ ){
           file = new File("/opt/hadoop-1.1.2/partition/" + outputFile +"-" + i + ".txt" );
            if (!file.exists()) {
                file.createNewFile();
            }
        }
    }
    
    
    void Calculate() throws IOException{
        int k,i,j;
        int sidePartition;
        BigDecimal x,y;
        BigDecimal first,last;
        BigDecimal first2,last2;
        PointCoordinates pointCoor;
        PartitionCoordinates partitionCoor;
        int numberOfPartition = 1;
        FileWriter fstream = null;
        BufferedWriter out = null;
        boolean FLAG = false;
        boolean FLAGX = false;
        boolean FLAGY = false;
        
        for ( k = 0 ; k < arrListData.size() ; k ++ ){
            pointCoor = arrListData.get(k);
            x = new BigDecimal(pointCoor.getX());
            x = x.round(new MathContext(roundNumber, RoundingMode.HALF_UP));
            y = new BigDecimal(pointCoor.getY());
            y = y.round(new MathContext(roundNumber, RoundingMode.HALF_UP));
            //System.out.println("i am here");
            numberOfPartition = 1;
            //System.out.println("pointttttttttttttttttttttttttttt : " + x + "," + y );
            for ( i = 0 ; i < arrListXPart.size() ; i ++ ){
                FLAGX = false;
                partitionCoor = arrListXPart.get(i);
                first = partitionCoor.getFirst();
                first = first.round(new MathContext(roundNumber, RoundingMode.HALF_UP));
                last = partitionCoor.getLast();
                last = last.round(new MathContext(roundNumber, RoundingMode.HALF_UP));
                //System.out.println("partition X1 : " + first + "," + last );
                
                //if ( first != 0.0 && first > eps){
                if ( first.compareTo(new BigDecimal(0.0)) > 0 && first.compareTo(eps) > 0 ){
                    first2 = first.subtract(eps);
                }
                else{
                    first2 = first;
                }                
                last2 = last.add(eps);
                //System.out.println("partition X2 : " + first + "," + last );
                FLAG = false;
                //System.out.println("xxxxxxxxxxxxxxxxxxxxxxxx : x = " + pointCoor.getX() + ",y = " + pointCoor.getY() + ",firtst2 = " + first2 + ",first = " + first + ", last2 = " + last2 + ", last = " + last + "\n" );
                //System.out.println("xxxxxxxxxxxxxxxxxxxxxxxx : x = " + x + ",y = " + y + ",firtst2 = " + first2 + ",first = " + first + ", last2 = " + last2 + ", last = " + last + "\n" );
                //if ( x >= first2 && x <= last2 ){
                if ( x.compareTo(first2) >= 0 && x.compareTo(last2) <= 0 ){
                    //System.out.println("X = " + x + ", y = " + y + ", first = " + first + ", last = " + last);
                    //System.out.println("Inside XXXXX");
                    //if ( x > first && x < last ){
                    if ( x.compareTo(first) > 0 && x.compareTo(last) < 0 ){
                        //pointCoor.setMerge(true);
                        //System.out.println("xxxxx : Inside the block ");
                        FLAGX = true;
                    }
                    else{
                        //if ( x >= first2 && x <= first ){
                        if ( x.compareTo(first2) >= 0 && x.compareTo(first) <= 0 ){
                            sidePartition = numberOfPartition - 2;
                           // System.out.println("i am here1111");
                        }
                        //else if ( x <= last2 && x >= last ){
                        else if ( x.compareTo(last2) <= 0 && x.compareTo(last) >= 0){
                            sidePartition = numberOfPartition + 2;
                            //System.out.println("i am here2222");
                        }
                    }
                   
                    for( j = 0 ; j < arrListYPart.size() ; j ++ ){
                       // System.out.println("i am here22");
                        FLAGY = false;
                        partitionCoor = arrListYPart.get(j);
                        first = partitionCoor.getFirst();
                        first = first.round(new MathContext(roundNumber, RoundingMode.HALF_UP));
                        last = partitionCoor.getLast();
                        last = last.round(new MathContext(roundNumber, RoundingMode.HALF_UP));
                        //System.out.println("partition Y1 : " + first + "," + last );
                        //if ( first != 0.0 && first > eps){
                        if ( first.compareTo(new BigDecimal(0.0)) > 0 && first.compareTo(eps) > 0){
                            first2 = first.subtract(eps);
                        }
                        else{
                            first2 = first;
                        }
                        last2 = last.add(eps);
                        //System.out.println("partition Y2 : " + first + "," + last );
                        FLAG = true;
                        //System.out.println("yyyyyyyyyyyyyyyyyyyyyy : x = " + x + ",y = " + y + ",firtst2 = " + first2 + ",first = " + first + ", last2 = " + last2 + ", last = " + last + "\n" );
                        //if ( y >= first2 && y <= last2 ){
                        if ( y.compareTo(first2) >= 0 && y.compareTo(last2) <= 0 ){
                            //System.out.println("Inside YYYY");
                            //if ( y > first && y < last ){
                            if ( y.compareTo(first) > 0 && y.compareTo(last) < 0 ){
                                FLAGY = true;
                                    //System.out.println("yyyyy : Inside the block ");
                            }
                            else{
                 
                                //if ( y >= first2 && y <= first ){
                                if ( y.compareTo(first2) >= 0 && y.compareTo(first) <= 0 ){
                                    //System.out.println("i am here1111");
                                    sidePartition = numberOfPartition - 2;
                                }
                                //else if ( y <= last2 && y >= last ){
                                else if ( y.compareTo(last2) <= 0 && y.compareTo(last) >= 0){
                                    //System.out.println("i am here2222");
                                    sidePartition = numberOfPartition + 2;
                                }
                            }
                           /* fstream = new FileWriter(outputFile + "-" + numberOfPartition + ".txt",true);
                            out = new BufferedWriter(fstream);
                            //System.out.println("Merge point  = " + pointCoor.getMergePoint());
                            if ( pointCoor.getMerge() == false ){
                                out.write(pointCoor.getId() + "," + x + "," + y + "," + pointCoor.getMergePoint() + "\n");
                            }
                            else{
                                out.write(pointCoor.getId() + "," + x + "," + y + "," + pointCoor.getMergePoint() + "," + numberOfPartition + "," + pointCoor.getSidePartition() + "\n");
                            }
                            out.close();*/
                            
                            if (pointCoor.getPartitions() == null){
                                pointCoor.setPartitions("" + numberOfPartition);
                            }
                            else {
                                if ( FLAGX == true && FLAGY == true){
                                    pointCoor.setPartitions (numberOfPartition + "," + pointCoor.getPartitions());
                                }
                                else{
                                    pointCoor.setPartitions(pointCoor.getPartitions() + "," + numberOfPartition);
                                }
                            }
                        }
                        numberOfPartition ++ ;                       
                        //System.out.println("numberofClusters Y = " + numberOfCluster);
                    }
                    //pointCoor.setMerge(false);
                    //break;
                }
                if ( FLAG == false ){
                    numberOfPartition = numberOfPartition + 2;
                }
                //System.out.println("numberofClusters X = " + numberOfCluster);
            }   
        }
    }
    
    public void WritePartitions() throws IOException{
        PointCoordinates pointCoor;
        int i;
        FileWriter fstream = null;
        BufferedWriter out;
        String del = ",";
        String []temp;
        String partitions = null;
        
        for( i = 0 ; i < arrListData.size() ; i ++ ){
            pointCoor = arrListData.get(i);
            //System.out.println(pointCoor.getId() + " point = " + pointCoor.getX() + "," +pointCoor.getY() + ", partition = " + pointCoor.getPartitions());
            //if ( pointCoor.getMyPartition().contains(",")){
                temp = pointCoor.getPartitions().split(del);
                for (int j = 0 ; j < temp.length ; j ++){
                    partitions = pointCoor.getPartitions();
                    
                    //System.out.println("old partition = " + partitions);
                    /*if (partitions.contains(temp[j] + ",")){
                        partitions = partitions.replaceAll( temp[j] + ",", "");
                     //   System.out.println("new partition = " + partitions);
                    }
                    else if (partitions.contains( "," + temp[j])){
                        partitions = partitions.replaceAll( "," + temp[j], "");
                       // System.out.println("new partition = " + partitions);
                    }
                    else{
                        partitions = null;
                        //System.out.println("new partition = " + partitions);
                    }*/
                    
                   fstream = new FileWriter("/opt/hadoop-1.1.2/partition/" + outputFile + "-" + temp[j] + ".txt",true);
                    out = new BufferedWriter(fstream);
                    //if ( pointCoor.getMerge() == false ){
                    if ( partitions == null ){
                        out.write(pointCoor.getId() + "," + pointCoor.getX() + "," + pointCoor.getY() + "," + temp[j] + "\n");
                    }
                    else{
                        out.write(pointCoor.getId() + "," + pointCoor.getX() + "," + pointCoor.getY() + "," + partitions + "\n");
                    }

                    out.close();
                }       
            //}
            /*else{
                fstream = new FileWriter(outputFile + "-" + pointCoor.getMyPartition() + ".txt",true);
                out = new BufferedWriter(fstream);
                if ( pointCoor.getMerge() == false ){
                    out.write(pointCoor.getId() + "," + pointCoor.getX() + "," + pointCoor.getY()  + "\n");
                }
                else{
                    out.write(pointCoor.getId() + "," + pointCoor.getX() + "," + pointCoor.getY() + "," + pointCoor.getMyPartition() + "," + pointCoor.getSidePartition() + "\n");
                }

                out.close();
            }*/
        }
    }
    
}

