package bigquery_csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;

public class BigQuery_CSV_main {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		int column = 2048;
		int input [][] = new int[column][column+1];
		BigQuery_CSV_lib blib = new BigQuery_CSV_lib("/Users/mizuno/Downloads/closedqueue-929a267e03b8.json", column, "mznfe");
		
		BigQuery_CSV_main bmain = new BigQuery_CSV_main();
		
		for(int i = 2; i <= 5; i++) {
			String tableName = "Fe_OFF_10000"+i;
			String fileName = "csv/"+tableName+".csv";
			bmain.getCSV2(fileName, column, column+1, input);
			//System.out.println("Input Data" +Arrays.deepToString(input));
			
			//BigQueryに格納する場合、テーブルを作成する
			blib.createTable(tableName);
			
			//OK
			System.out.println("テーブル完了");
		
			//データInsert
			//blib.insertCSV(input);
		}
	}
	
	//複数種類のデータを一度に取り込む場合
	public void getCSV2(String path, int row, int column, int[][] input ) {
		//CSVから取り込み
		try {
			File f = new File(path);
			BufferedReader br = new BufferedReader(new FileReader(f));
					 
			String[][] data = new String[row][column]; 
			String line = br.readLine();
			for (int i = 0; line != null; i++) {
				data[i] = line.split(",", 0);
				line = br.readLine();
			}
			br.close();
			
			// CSVから読み込んだ配列の中身を処理
			for(int i = 0; i < data.length; i++) {
				for(int j = 0; j < data[0].length; j++) {
					input[i][j] = Integer.parseInt(data[i][j]);
				}
			} 

		} catch (IOException e) {
				System.out.println(e);
		}
			//CSVから取り込みここまで	
	}


	
}
