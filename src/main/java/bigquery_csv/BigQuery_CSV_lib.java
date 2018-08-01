package bigquery_csv;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;

public class BigQuery_CSV_lib {
	
	BigQuery bigQuery;
	private int column;
	private String datasetId;
	TableId tableId;
	
	//コンストラクタ
	public BigQuery_CSV_lib(String key, int column, String datasetId) {
		this.column = column;
		try {
			this.bigQuery = getClientWithJsonKey(key);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.datasetId = datasetId;
	}
	
	public void createTable(String tableName) { //BigQueryにテーブルを作成
		this.tableId = TableId.of(datasetId, tableName);
		Field row[] = new Field[column+1];
	    for(int i = 0; i < column + 1; i++) { //最後の列はindex
	    		row[i] = Field.of("int64_field_"+i, LegacySQLTypeName.INTEGER);
	    }
	    Schema schema = Schema.of(row);
	    StandardTableDefinition tableDefinition = StandardTableDefinition.of(schema);
	    bigQuery.create(TableInfo.of(tableId, tableDefinition));
	}

	public void insertCSV(int input[][]) {
		for(int i = 0; i < input.length; i++) {
			Map<String, Object> rowvalue = new HashMap<>();
			for(int j = 0; j < input[0].length; j++) {
	    			rowvalue.put("int64_field_"+j, input[i][j]);
			}
			InsertAllRequest insertRequest = InsertAllRequest.newBuilder(tableId).addRow(rowvalue).build();
			InsertAllResponse insertResponse = bigQuery.insertAll(insertRequest);
		}
		
	}
	
	//jsonファイル認証関数
	public static BigQuery getClientWithJsonKey(String key) throws IOException {
		return BigQueryOptions.newBuilder()
				.setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(key)))
				.build()
				.getService();
	}
}
