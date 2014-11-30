package hadoopPrograms.dbscan.MergeMappingMapReduceProgram;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author jimakos
 */
public class OutputMergeDetails implements Writable {
    int partition;
    int cluster;
    boolean FLAG;//gia na valei to prwto object to '<-->'
    
    
    public OutputMergeDetails( int partition, int cluster){
        this.partition = partition;
        this.cluster = cluster;
    }
    
    
    public OutputMergeDetails(){
        this(0,0);
    }

    
    public int getPartition() {
        return partition;
    }

    public int getCluster() {
        return cluster;
    }
    

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public void setCluster(int cluster) {
        this.cluster = cluster;
    }
    
    public String toString() {
       return Integer.toString(partition) + ","+ Integer.toString(cluster);           
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
       out.writeInt(partition);
       out.writeInt(cluster);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        partition = in.readInt();
        cluster = in.readInt();
    }
    
}

