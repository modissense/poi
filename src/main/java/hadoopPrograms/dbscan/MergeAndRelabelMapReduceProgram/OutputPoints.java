package hadoopPrograms.dbscan.MergeAndRelabelMapReduceProgram;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author jimakos
 */
public class OutputPoints implements Writable {
    private int id;
    private double x;
    private double y;
    
    public OutputPoints( int id, double x, double y) {
        this.id = id;
        this.x = x;
        this.y = y;
    }

    public OutputPoints(){
        this(0,0.0,0.0);
    }
    
    public int getId() {
        return id;
    }

    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }

    
    public void setId(int id) {
        this.id = id;
    }

    public void setX(double x) {
        this.x = x;
    }

    public void setY(double y) {
        this.y = y;
    }

    public String toString() {
        return Integer.toString(id) + ","+ Double.toString(x) + "," + Double.toString(y) ;        
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeDouble(x);
        out.writeDouble(y);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readInt();
        x = in.readDouble();
        y = in.readDouble();
    }
    
}
