package hadoopPrograms.dbscan.computecenters;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author jimakos
 */
public class PointCoordinates implements Writable{
    private double x;
    private double y;
    private int id;
    private String partitions;
    
    PointCoordinates(){}
    
    PointCoordinates( int id, double x, double y){
        this.x = x;
        this.y = y;
        this.id = id;
        this.partitions = null;
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
    
    public String getPartitions(){
        return partitions;
    } 
    
    
    
    public void setId( int id ){
        this.id = id;
    }
    
    public void setX(Double x){
        this.x = x;
    }
    
    public void setY(Double y){
        this.y = y;
    }
    
    public void setPartitions(String partition){
        this.partitions = partition;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(x);
        out.writeDouble(y);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        x = in.readDouble();
        y = in.readDouble();
    }
    
    public String toString() {
        return Double.toString(x) + ", "+ Double.toString(y) ;
    }
}

