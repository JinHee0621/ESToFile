package testSparkApp;

import java.awt.Color;
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
	
	
	public elasticJoin(GraphicUi gui) {
		this.gui = gui;
	}
	
	public void runEsToFile(String data) {
		
		String host = data.substring(0, data.indexOf("□")).trim();
		String query = Util.replace(data.substring(data.indexOf("□")+1, data.length()), "\n", " ").trim();
		query = Util.replace(query, "\r", " ").trim();
		
		try {
			gui.b1.setText("실행 중");
			gui.b1.setForeground(new Color(255,255,255));
			gui.b1.setBackground(new Color(57,82,140));
			
			System.out.println(host + " " + query);
			runQuery(host, query);

			
		} catch (Exception e) {
			gui.b1.setText("실행");
			gui.b1.setForeground(new Color(0,0,0));
			gui.b1.setBackground(new Color(38,136,225));
			gui.resultText.setText("오류가 발생하여 파일 생성에 실패하였습니다.");
			gui.resultText.setForeground(Color.red);
			gui.isRunning = false;
		}
		
		gui.b1.setText("실행");
		gui.b1.setForeground(new Color(0,0,0));
		gui.b1.setBackground(new Color(38,136,225));
		gui.resultText.setForeground(Color.black);
		gui.resultText.setText("파일 생성을 성공적으로 완료했습니다.");
		gui.isRunning = false;
	}
	
	public void runQuery(String host, String query) {
		SparkSession sc = SparkSession
			      .builder()
			      .config("spark.master", "local")
			      .config("es.nodes", host)
			      .config("spark.sql.shuffle.partitions", 2)
			      //.config("es.net.http.auth.user", "elastic")
			      //.config("es.net.http.auth.pass", "1q2w3e4r")
			      .appName("jointest")
			      .getOrCreate();
		
		SQLContext sqc = new SQLContext(sc);

		Dataset<Row> people = JavaEsSparkSQL.esDF(sqc, "test"); 
		people.createOrReplaceTempView("test");
	
		
		Dataset<Row> res = sqc.sql(query);
		List<String> jarr = res.toJSON().collectAsList();
		
		res.show();

		jarr.forEach(val -> {
			try {
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
			}catch(Exception e) {
				e.printStackTrace();
			}
		});
		
		sc.stop();
	}
}
