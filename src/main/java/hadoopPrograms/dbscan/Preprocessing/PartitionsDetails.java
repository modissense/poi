package hadoopPrograms.dbscan.Preprocessing;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author jimakos
 */
public class PartitionsDetails implements Writable{
    public double startPoint;
    public double finalPoint;
            
    public PartitionsDetails(){
        this.startPoint = 0;
        this.finalPoint = 0;
    }
            
    public PartitionsDetails(double startPoint, double finalPoint){
        this.startPoint = startPoint;
        this.finalPoint = finalPoint;
    }
            
    public void setStartPoint(double starPoint){
        this.startPoint = starPoint;
    }
           
    public void setFinalPoint(double finalPoint){
        this.finalPoint = finalPoint;
    }
            
    public Double getStartPoint(){
        return startPoint;
    }
            
    public Double getFinalPoint(){
        return finalPoint;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(startPoint);
        out.writeDouble(finalPoint);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        startPoint = in.readDouble();
        finalPoint = in.readDouble();
    }
            
    public String toString() {
        return Double.toString(startPoint) + "," + Double.toString(finalPoint);
    }
}

