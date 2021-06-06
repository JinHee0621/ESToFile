package testSparkApp;

import java.awt.Color;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

import gui.GraphicUi;

public class elasticJoin {
	
	public GraphicUi gui; 
	
	public elasticJoin(GraphicUi gui) {
		this.gui = gui;
	}
	
	public void runEsToFile(int size) {
		try {
			gui.b1.setText("실행 중");
			gui.b1.setForeground(new Color(255,255,255));
			gui.b1.setBackground(new Color(57,82,140));
			int progressVal = 1;
			
			//ProgressBar Update
			//---------------------------------------------------
			for(int i = 0; i < size; i += (size/100)) {
				gui.progress.setValue(progressVal++);
		        Thread.sleep(20);
			}
			progressVal = 0;
			//---------------------------------------------------
			//throw new Exception();
			gui.b1.setText("실행");
			gui.b1.setForeground(new Color(0,0,0));
			gui.b1.setBackground(new Color(38,136,225));
			gui.resultText.setForeground(Color.black);
			gui.resultText.setText("파일 생성을 성공적으로 완료했습니다.");
			gui.isRunning = false;
			
		} catch (Exception e) {
			gui.b1.setText("실행");
			gui.b1.setForeground(new Color(0,0,0));
			gui.b1.setBackground(new Color(38,136,225));
			gui.resultText.setText("오류가 발생하여 파일 생성에 실패하였습니다.");
			gui.resultText.setForeground(Color.red);
			gui.isRunning = false;
		}
		
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		SparkSession sc = SparkSession
			      .builder()
			      .config("spark.master", "spark://192.168.0.101:7077")
			      .config("es.nodes", "http://192.168.0.101:9200")
			      .config("spark.sql.shuffle.partitions", 10)
			      .appName("jointest")
			      .getOrCreate();
		
		SQLContext sqc = new SQLContext(sc);

		Dataset<Row> people = JavaEsSparkSQL.esDF(sqc, "xfile"); 
		people.createOrReplaceTempView("xfile");
	
		
		Dataset<Row> res = sqc.sql("SELECT OID FROM xfile");
		
		
		//���
		res.show();
		
		
//		System.out.println(res.as(Encoders.STRING()).collectAsList());
//		System.out.println(res.map(row -> row.mkString(), Encoders.STRING()).collectAsList());
		
		
//		for(Iterator<Row> iter = res.toLocalIterator(); iter.hasNext();) {
//		    String item = (iter.next()).toString();
//		    System.out.println(item.toString());    
//		}
		
//		System.out.println(res.toJSON().collectAsList());
//		List<String> jarr = res.toJSON().collectAsList();
	
//		JSONObject jsonObject = new JSONObject("{\"phonetype\":\"N95\",\"cat\":\"WP\"}");
//		String str = "{\"phonetype\":\"N95\",\"cat\":\"WP\"}";
//		System.out.println(str);
//		System.out.println(jsonObject.getString("cat"));
		
		
		sc.stop();
		//����
		//res.write()
		//	.format("es")
		//	.save("testnew/doc");
	}

}
