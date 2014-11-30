package hadoopPrograms.dbscan.MergeMappingMapReduceProgram;

/**
*
* @author jimakos
*/
public class FindNeighborPartitions {
   private int sizeY;
   private int nOfPartitions;
   String[] strArray = null;
   
   public FindNeighborPartitions( int sizeY, int nOfPartitions ){
       this.sizeY = sizeY;
       this.nOfPartitions = nOfPartitions;
       strArray = new String[4];
   }
   
   
   public String[] find(){ 
       if ( nOfPartitions == 4){
           strArray[0] = "2 3 4";
           strArray[1] = "1 3 4";
           strArray[2] = "1 2 4";
           strArray[3] = "1 2 3";
       }

       return strArray;
   }

   
   //////////////Get///////////////
   public int getSizeY() {
       return sizeY;
   }
   
 
   public int getnOfPartitions() {
       return nOfPartitions;
   }
   
  
   /////////////Set///////////////
   public void setSizeY(int sizeY) {
       this.sizeY = sizeY;
   }

   public void setnOfPartitions(int nOfPartitions) {
       this.nOfPartitions = nOfPartitions;
   }
   
}

