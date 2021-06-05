package testSparkApp;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

public class elasticJoin {
  
	public elasticJoin() {
		System.out.println("조인 실행");
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		SparkSession sc = SparkSession
			      .builder()
			      //.config("spark.master", "spark://192.168.0.101:7077")
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
