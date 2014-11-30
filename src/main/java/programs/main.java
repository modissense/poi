package programs;

import java.io.IOException;
import java.sql.SQLException;


public class main {

	/**
	 * @param args
	 * @throws SQLException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException, SQLException {
		// TODO Auto-generated method stub
		System.out.println("merge");
		MergeHbasePostgresPOIs merge = new MergeHbasePostgresPOIs();
		merge.compute();
	}

}
