package hadoopPrograms.dbscan.LocalDBScanMapReduceProgram;

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
    private boolean visited = false;//ean exoume episkeftei auto to simeio
    private boolean cluster = false;//se poio cluster einai
    private boolean OnQueue = false;
    private int clusterPartition;
    private String partitions = null;
    private int partition;
    
    public PointCoordinates(int id, double x, double y, int clusterPartition, String partitions){
        this.id =id;
        this.x = x;
        this.y = y;
        this.clusterPartition = clusterPartition;
        this.partitions = partitions;
        
    }
    
    public PointCoordinates(){
        this(0,0.0,0.0,0,null);
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
    
    public boolean getOnQueue(){
        return OnQueue;
    }
    
    public boolean getVisited(){
        return visited;
    }
    
    public boolean getCluster(){
        return cluster;
    }
    
    public int getNCLuster(){
        return nCluster;
    }
    
    public int getClusterPartition(){
        return clusterPartition;
    }
    
    public int getPartition(){
        return partition;
    }
    
    public String getPartitions(){
        return partitions;
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
    
    
    public void setVisited(){
        this.visited = true;
    }
    
    public void setCluster(){
        this.cluster = true;
    }

    public void setOnQueue(){
        this.OnQueue = true;
    }
    
    public void setClusterPartition(int clusterPartition){
        this.clusterPartition = clusterPartition;
    }
    
    public void setPartiton( int partition ){
        this.partition = partition;
    }
    
    public void setPartitions(String partitions){
        this.partitions = partitions;
    }

    public String toString() {
        return Integer.toString(id) + ","+ Double.toString(x) + "," + Double.toString(y) + "," + Integer.toString(clusterPartition) + "," + Integer.toString(nCluster) + "," + partitions ;        
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
       out.writeInt(id);
       out.writeDouble(x);
       out.writeDouble(y);
       out.writeInt(clusterPartition);
       out.writeInt(partition);
       out.writeUTF(partitions);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
       
        id = in.readInt();
        x = in.readDouble();
        y = in.readDouble();
        clusterPartition = in.readInt();
        partition = in.readInt();
        partitions = in.readUTF();
    }
    
}

