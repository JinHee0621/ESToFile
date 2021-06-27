package testSparkApp;

import java.awt.Color;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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

	public void checkTimeOutJoin(String[] tableList, String hostData, String query, long time)
			throws InterruptedException, ExecutionException, TimeoutException {
		ExecutorService threadPool = Executors.newCachedThreadPool();

		@SuppressWarnings("unchecked")
		FutureTask task = new FutureTask(new Callable() {
			public Boolean call() {
				runQuery(hostData, query, tableList);
				return true;
			}
		});
		threadPool.execute(task);
		Boolean result = false;
		result = (Boolean) task.get(time, TimeUnit.MICROSECONDS);
	}

	public String getTableEle(String data, ArrayList<String> table) {
		data = data.toLowerCase();
		String result = "";
		String copyData = "";
		
		gui.resultText.setText("쿼리문으로부터 테이블명 확인중..");
		gui.resultText.setForeground(Color.black);
		
		if (data.contains("from")) {
			copyData = data.substring(data.indexOf("from") + 4, data.length()).trim();
			if (copyData.startsWith("(")) {
				copyData = copyData.substring(copyData.indexOf("from") + 4, copyData.length()).trim();
			}
		} else if (data.contains("join")) {
			copyData = data.substring(data.indexOf("join") + 4, data.length()).trim();
		}

		if (!copyData.contains("join") && !copyData.contains("from")) {
			copyData = copyData + " ";
			String tableData = copyData.substring(0, copyData.indexOf(" ")).trim();
			if (!table.contains(tableData)) {
				table.add(tableData);
			}
			result = table.toString();
			return result;
		} else {
			String tableData = copyData.substring(0, copyData.indexOf(" ")).trim();
			if (!table.contains(tableData)) {
				table.add(tableData);
			}
			copyData = copyData.substring(copyData.indexOf(" "), copyData.length()).trim();
			return getTableEle(copyData, table);
		}
	}

	public void runEsToFile(String data, long time) {

		System.out.println(data);

		String host = data.substring(0, data.indexOf("□")).trim();
		String query = Util.replace(data.substring(data.indexOf("□") + 1, data.length()), "\n", " ").trim();
		query = Util.replace(query, "\r", " ").trim();

		String tableListRow = getTableEle(query, new ArrayList<String>());

		gui.b1.setText("실행 중");
		gui.b1.setForeground(new Color(255, 255, 255));
		gui.b1.setBackground(new Color(57, 82, 140));

		System.out.println(host + " " + query);

		tableListRow = Util.replace(tableListRow, "[", "").trim();
		tableListRow = Util.replace(tableListRow, "]", "").trim();
		String[] tableList = tableListRow.split(",");

		try {
			checkTimeOutJoin(tableList, host, query, time);
			sc.stop();
			
			gui.resultText.setText("파일이 정상적으로 생성되었습니다..");
			gui.resultText.setForeground(Color.black);
			
			gui.b1.setText("실행");
			gui.b1.setForeground(new Color(0, 0, 0));
			gui.b1.setBackground(new Color(38, 136, 225));
			gui.isRunning = false;
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			if (e.getMessage().contains("hostname")) {
				gui.callAlert("호스트를 찾을수 없습니다.");
			} else if (e.getMessage().contains("No read")) {
				gui.callAlert("쿼리문 입력 형식이 잘못되었습니다.");
			} else if (e.getMessage().contains("Cannot find mapping")) {
				gui.callAlert("elastic에서 해당 인덱스를 찾을 수 없습니다.");
			} else {
				gui.callAlert(e.getMessage());
			}

			gui.resultText.setText("오류가 발생하여 파일 생성에 실패하였습니다.");
			gui.resultText.setForeground(Color.red);
			gui.isRunning = false;
			sc.stop();
			
			gui.b1.setText("실행");
			gui.b1.setForeground(new Color(0, 0, 0));
			gui.b1.setBackground(new Color(38, 136, 225));

			e.printStackTrace();
		}
	}

	public void runQuery(String host, String query, String[] tableList) {

		gui.resultText.setText("Spark 세션 실행중..");
		gui.resultText.setForeground(Color.black);

		sc = SparkSession.builder().config("spark.master", "local").config("es.nodes", host)
				.config("spark.sql.shuffle.partitions", 2)
				// .config("es.net.http.auth.user", "elastic")
				// .config("es.net.http.auth.pass", "1q2w3e4r")
				.appName("jointest").getOrCreate();

		SQLContext sqc = new SQLContext(sc);

		query = query.toLowerCase();
		query = Util.replace(query, "\n", " ");
		query = Util.replace(query, "\r", " ");
		query = Util.replace(query, "\t", " ");

		int index = 0;
		HashMap<Integer, Dataset<Row>> indexList = new HashMap<>();

		for (String tableName : tableList) {
			indexList.put(index, JavaEsSparkSQL.esDF(sqc, Util.replace(tableName, "`", "")));
			indexList.get(index).createOrReplaceTempView(tableName);
			index++;
		}
		gui.resultText.setText("Sql 실행중..");
		gui.resultText.setForeground(Color.black);
		Dataset<Row> res = sqc.sql(query);
		List<String> jarr = res.toJSON().collectAsList();

		gui.resultText.setText( jarr.size() + " 개의 데이터를 하나의 파일로 변환중..");
		gui.resultText.setForeground(Color.black);
		
		res.show();

		jarr.forEach(val -> {
			FileWriter fw;
			try {
				fw = new FileWriter(gui.filePath + "\\ResultFile.txt", true);
				val.replace('"', '\"');
				fw.write(val + "\n");
				fw.flush();
				fw.close();

				if (jarr.size() > 100) {
					count++;
					if (count >= jarr.size() / 100) {
						count = 0;
						gui.progressVal += 1;
					}
				} else {
					count += (100 / jarr.size());
					gui.progressVal = count;
				}
				System.out.println(gui.progressVal);
				gui.progress.setValue(gui.progressVal);

			} catch (IOException e) {
				e.printStackTrace();
				// System.out.println(e.getMessage());
				if (e.getMessage().contains("액세스가 거부되었습니다")) {
					gui.callAlert("해당 디렉토리에 대한 파일 생성 권한이 없습니다.");
				}
				sc.stop();
			}
		});
		sc.stop();
	}
}
