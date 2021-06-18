package testSparkApp;

import java.awt.Color;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

import gui.GraphicUi;
import util.Util;

public class elasticJoin {
	
	public GraphicUi gui; 
	int count = 0;
	SparkSession sc = null;
	
	public elasticJoin(GraphicUi gui) {
		this.gui = gui;
	}
	
	public String getTableEle(String data, ArrayList<String> table) {
		data = data.toLowerCase();
		String result = "";
		String copyData = "";
		
		if(data.contains("from")) {
			copyData = data.substring(data.indexOf("from") + 4 , data.length()).trim();
			if(copyData.startsWith("(")) {
				copyData = copyData.substring(copyData.indexOf("from") + 4, copyData.length()).trim();
			}
		}else if(data.contains("join")) {
			copyData = data.substring(data.indexOf("join") + 4, data.length()).trim();
		}
		
		if(!copyData.contains("join") && !copyData.contains("from")) {
			copyData = copyData + " ";
			String tableData = copyData.substring(0, copyData.indexOf(" ")).trim();
			if(!table.contains(tableData)) {
				table.add(tableData);
			}
			result = table.toString();
			return result;
		} else {
			String tableData = copyData.substring(0, copyData.indexOf(" ")).trim();
			if(!table.contains(tableData)) {
				table.add(tableData);
			}
			copyData = copyData.substring(copyData.indexOf(" "), copyData.length()).trim();
			return getTableEle(copyData, table);
		}
	}
	
	public void runEsToFile(String data) {
		
		String host = data.substring(0, data.indexOf("□")).trim();
		String query = Util.replace(data.substring(data.indexOf("□")+1, data.length()), "\n", " ").trim();
		query = Util.replace(query, "\r", " ").trim();
		
		String tableList = getTableEle(query, new ArrayList<String>());
		
		try {
			gui.b1.setText("실행 중");
			gui.b1.setForeground(new Color(255,255,255));
			gui.b1.setBackground(new Color(57,82,140));
			
			System.out.println(host + " " + query);
			runQuery(host, query, tableList);
			sc.stop();
			
			gui.b1.setText("실행");
			gui.b1.setForeground(new Color(0,0,0));
			gui.b1.setBackground(new Color(38,136,225));
			gui.isRunning = false;
			
		} catch (Exception e) {
			gui.b1.setText("실행");
			gui.b1.setForeground(new Color(0,0,0));
			gui.b1.setBackground(new Color(38,136,225));
			
			sc.stop();
			
			if(e.getMessage().contains("hostname")) {
				gui.callAlert("호스트를 찾을수 없습니다.");
			} else if(e.getMessage().contains("No read")){
				gui.callAlert("쿼리문 입력 형식이 잘못되었습니다.");
			} else if(e.getMessage().contains("Cannot find mapping")){
				gui.callAlert("elastic에서 해당 인덱스를 찾을 수 없습니다.");
			} else {
				gui.callAlert(e.getMessage());
			}
			
			gui.resultText.setText("오류가 발생하여 파일 생성에 실패하였습니다.");
			gui.resultText.setForeground(Color.red);
			gui.isRunning = false;
		}
	}
	
	public void runQuery(String host, String query, String tableListRow) {
		
		System.out.println(tableListRow);
		tableListRow = Util.replace(tableListRow, "[", "").trim();
		tableListRow = Util.replace(tableListRow, "]", "").trim();
		System.out.println(tableListRow);
		String[] tableList = tableListRow.split(",");
		
		System.out.println(host);
		
		sc = SparkSession
			      .builder()
			      .config("spark.master", "local")
			      .config("es.nodes", host)
			      .config("spark.sql.shuffle.partitions", 2)
			      //.config("es.net.http.auth.user", "elastic")
			      //.config("es.net.http.auth.pass", "1q2w3e4r")
			      .appName("jointest")
			      .getOrCreate();
		
		SQLContext sqc = new SQLContext(sc);

		query = query.toLowerCase();
		query = Util.replace(query, "\n", " ");
		query = Util.replace(query, "\r", " ");
		query = Util.replace(query, "\t", " ");
		
		int index = 0;
		HashMap<Integer, Dataset<Row>> indexList = new HashMap<>();

		for(String tableName : tableList) {
			indexList.put(index, JavaEsSparkSQL.esDF(sqc, Util.replace(tableName,"`", "")));
			indexList.get(index).createOrReplaceTempView(tableName);
			index++;
		}
		
		Dataset<Row> res = sqc.sql(query);
		List<String> jarr = res.toJSON().collectAsList();
		
		res.show();

		jarr.forEach(val -> {
				FileWriter fw;
				try {
					fw = new FileWriter(gui.filePath + "\\ResultFile.txt", true);
					val.replace('"', '\"');
					fw.write(val + "\n");
					fw.flush();
					fw.close();
					
					if(jarr.size() > 100) {
						count++;
						if(count >= jarr.size() / 100) {
							count = 0;
							gui.progressVal += 1;
						}
					} else {
						count += (100/jarr.size());
						gui.progressVal = count;
					}
					System.out.println(gui.progressVal);
					gui.progress.setValue(gui.progressVal);
					
				} catch (IOException e) {
					e.printStackTrace();
					//System.out.println(e.getMessage());
					if(e.getMessage().contains("액세스가 거부되었습니다")) {
						gui.callAlert("해당 디렉토리에 대한 파일 생성 권한이 없습니다.");
					}
					sc.stop();
				}
		});
		sc.stop();
	}
}
