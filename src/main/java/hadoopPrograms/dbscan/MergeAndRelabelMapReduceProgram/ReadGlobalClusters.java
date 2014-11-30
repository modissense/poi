package hadoopPrograms.dbscan.MergeAndRelabelMapReduceProgram;

import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author jimakos
 */
public class ReadGlobalClusters {
    private String inputFile = null;
    ArrayList<String> glClusters = null;
    String []globalClusters;
    Configuration conf;
    
    public ReadGlobalClusters( String inputFile, Configuration conf ){
        this.inputFile = inputFile;
        glClusters = new ArrayList<String>();
        this.conf = conf;
    }
    
    public String[] read() throws FileNotFoundException, IOException{
        
        Path ptInput =new Path(inputFile);
        FileSystem fsInput = FileSystem.get(conf);
        BufferedReader br=new BufferedReader(new InputStreamReader(fsInput.open(ptInput)));
        String strLine = null;
        String []temp = null;
        String glCluster = null;
        String clusters = null;
        boolean FLAG = false;
        int i = 0;
        
        while ((strLine = br.readLine()) != null)   {
            temp = strLine.split(",");
            if( FLAG == false ){
                glCluster = temp[2];
                clusters = temp[0] + temp[1];
                FLAG = true;
            }
            else{
                if ( glCluster.equals(temp[2])){
                    clusters = clusters + " " + temp[0] + temp[1];
                }
                else{
                    glClusters.add(clusters);
                    glCluster = temp[2];
                    clusters = temp[0] + temp[1];
                }
            }
        }

        glClusters.add(clusters);
        globalClusters = new String[glClusters.size()];
        
        for ( i = 0 ; i < glClusters.size() ; i ++ ){
            globalClusters[i] = glClusters.get(i);
        }
      
        return globalClusters;
    }
    
}

