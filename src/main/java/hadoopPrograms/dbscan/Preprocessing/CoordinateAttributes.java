package hadoopPrograms.dbscan.Preprocessing;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author jimakos
 */
public class CoordinateAttributes implements Writable{
    public double coord;
    public int counter;

    public CoordinateAttributes(double coord, int counter){
        this.coord = coord;
        this.counter = counter;
    }
    
    public CoordinateAttributes(){
        this(0.0,0);
    }

    public Double getCoord(){
        return coord;
    }

    public Integer getCounter(){
        return counter;
    }

    public void setCounter(){
        this.counter ++;
    }
           
    public void setCoordinate(double coord ){
        this.coord = coord;
    }

     @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(coord);
        out.writeInt(counter);
    }

    @Override   
    public void readFields(DataInput in) throws IOException {
        coord = in.readDouble();
        counter = in.readInt();
    }

    public String toString() {
        return Double.toString(coord) + ","+ Integer.toString(counter) ;
    }
}

