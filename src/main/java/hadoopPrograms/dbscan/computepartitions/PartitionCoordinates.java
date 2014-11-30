package hadoopPrograms.dbscan.computepartitions;

import java.math.BigDecimal;

public class PartitionCoordinates {
    BigDecimal first;
    BigDecimal last;
    
    PartitionCoordinates( BigDecimal first, BigDecimal last){
        this.first = first;
        this.last = last;
    }
    
    public BigDecimal getFirst(){
        return first;
    }
    
    public BigDecimal getLast(){
        return last;
    }
    
    public void setFirst( BigDecimal first){
        this.first = first;
    }
    
    public void setLast(BigDecimal last){
        this.last = last;
    }
}

