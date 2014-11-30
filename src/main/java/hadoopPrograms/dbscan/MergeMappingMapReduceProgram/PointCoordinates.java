package hadoopPrograms.dbscan.MergeMappingMapReduceProgram;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author jimakos
 */
public class PointCoordinates implements Writable {
    private double x;
    private double y;
    private int id;
    private int nCluster = 0;
    private String partitions = null;
    private int partition ;
    private boolean visited = false;//ean exoume episkeftei auto to simeio
    private boolean cluster = false;//se poio cluster einai
    private boolean OnQueue = false;
    private int clusterPartition;
    
    
    public PointCoordinates(int id, double x, double y,  int clusterPartition, int cluster, int partition, String partitions){
        this.id =id;
        this.x = x;
        this.y = y;
        this.nCluster = cluster;
        this.clusterPartition = clusterPartition;
        this.partition = partition;
        this.partitions = partitions;
    }
    
    public PointCoordinates(){
        this(0,0.0,0.0,0,0,0,null);
    }
    
    public int getId(){
        return id;
    }
    
    public Double getX(){
        return x;
    }
    
    public Double getY(){
        return y;
    }
    
    public int getNCLuster(){
        return nCluster;
    }
    
    public int getPartition(){
        return partition;
    }
    
    public String getPartitions(){
        return partitions;
    }

    public boolean getCluster() {
        return cluster;
    }

    public boolean getOnQueue() {
        return OnQueue;
    }

    public int getClusterPartition() {
        return clusterPartition;
    }

    public boolean getVisited() {
        return visited;
    }
    
   
    
    
    public void setId (int id){
        this.id = id;
    }
    
    public void setX (double x){
        this.x = x;
    }
    
    public void setY (double y){
        this.y = y;
    }
    
    
    public void setNCLuster(int nCluster){
        this.nCluster = nCluster;
    }
    
    public void setPartition(int partition){
        this.partition = partition;
    }
    
    public void setPartitions(String partitions){
        this.partitions = partitions;
    }

    public void setVisited(boolean visited) {
        this.visited = visited;
    }

    public void setCluster(boolean cluster) {
        this.cluster = cluster;
    }

    public void setOnQueue(boolean OnQueue) {
        this.OnQueue = OnQueue;
    }

    public void setClusterPartition(int clusterPartition) {
        this.clusterPartition = clusterPartition;
    }
    
    
    public String toString() {
        return Integer.toString(id) + ","+ Double.toString(x) + "," + Double.toString(y) + "," + Integer.toString(clusterPartition) +"," + Integer.toString(nCluster) + "," + partitions ;        
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
       out.writeInt(id);
       out.writeDouble(x);
       out.writeDouble(y);
       out.writeInt(clusterPartition);
       out.writeInt(nCluster);  
       out.writeInt(partition);
       out.writeUTF(partitions);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
       
        id = in.readInt();
        x = in.readDouble();
        y = in.readDouble();
        clusterPartition = in.readInt();
        nCluster = in.readInt();
        partition = in.readInt();
        partitions = in.readUTF();
        
    }
    
}

