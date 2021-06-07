package util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Vector;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

public class Util {
	
	public static String fileEncoding = "utf-8";
	public static String LOG_PATH = "C:\\test\\log\\<INFO>\\<TABLE>_[YYYY-MM-DD].log";
	public static String EXCEPT_PATH = "";
	public static String TABLE = "TEST";
	public static String javaid = "none";
	
	public static int deleteCount = 0;
	
	public static void subDirList(String source, java.util.Vector<String> filelist, String exe){
		java.io.File dir = new java.io.File(source); 
		java.io.File[] fileList = dir.listFiles(); 
		try{
			for(int i = 0 ; i < fileList.length ; i++){
				java.io.File file = fileList[i]; 
				if(file.isFile()){
					if(file.getName().startsWith(("."))) continue;
					if(file.getName().endsWith("."+exe)) {
						filelist.addElement(file.getAbsolutePath());
					}
				}else if(file.isDirectory()){
					subDirList(file.getCanonicalPath().toString(), filelist); 
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	public static String getPrintStackTrace(Exception e) {
        
        java.io.StringWriter errors = new java.io.StringWriter();
        e.printStackTrace(new PrintWriter(errors));
         
        return errors.toString();
         
    }
	
	public static void subDirList(String source, java.util.Vector<String> filelist){
		java.io.File dir = new java.io.File(source); 
		java.io.File[] fileList = dir.listFiles(); 
		try{
			for(int i = 0 ; i < fileList.length ; i++){
				java.io.File file = fileList[i]; 
				if(file.isFile()){
					filelist.addElement(file.getAbsolutePath());
				}else if(file.isDirectory()){
					subDirList(file.getCanonicalPath().toString(), filelist); 
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static Document stringToDocument(String xmlString) {
		Document doc = null;
		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory
					.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			doc = builder.parse(new ByteArrayInputStream(xmlString
					.getBytes(fileEncoding)));
		} catch (java.lang.Exception e) {
			e.printStackTrace();
			doc = null;
		}
		return doc;
	}
	
	public static String getFileData(java.io.File finput) {
		String line = "";
		String retStr = "";
		try {
			FileInputStream fis = new FileInputStream(finput);
			BufferedReader reader = new BufferedReader(new java.io.InputStreamReader(fis,fileEncoding));
			while ((line = reader.readLine()) != null) {
				retStr += line + "\n";
			}
			reader.close();
			
		} catch (java.lang.Exception e) {
			//e.printStackTrace();
			retStr = getFileData(finput, "utf-8");
		}
		return retStr;
	}

	public static String getFileData(java.io.File finput, String fileEncoding) {
		String line = "";
		String retStr = "";
		try {
			FileInputStream fis = new FileInputStream(finput);			
			BufferedReader reader = new BufferedReader(new java.io.InputStreamReader(fis,fileEncoding));
			
			while ((line = reader.readLine()) != null) {
				retStr += line + "\n";
			}
			reader.close();
			
		} catch (java.lang.Exception e) {
			e.printStackTrace();
			retStr = "";
		}
		return retStr;
	}
	
	public static void createFile(String filePath, String contentData) {
		java.io.BufferedWriter output = null;
		try {
			if(filePath != null){
				java.io.File targetFile = new java.io.File(filePath);
				targetFile.getParentFile().mkdirs();
				if (targetFile.isFile()) {
					targetFile.delete();
				} else {
					targetFile.createNewFile();
				}
				output = new java.io.BufferedWriter(new java.io.OutputStreamWriter(
						new java.io.FileOutputStream(targetFile.getPath()),fileEncoding));
				output.write(contentData);
			}
		} catch (java.io.FileNotFoundException e1) {
			e1.printStackTrace();
			System.out.println(e1.toString());
		} catch (java.lang.Exception e2) {
			e2.printStackTrace();
			System.out.println(e2.toString());
		} finally {
			if (output != null)
				try {
					output.close();
				} catch (IOException e) {
					//e.printStackTrace();
				}
		}
	}
	
	public static String xmlNodeFirst(Document doc, String xPathCondition) {
		String retStr = "";
		try{
			Vector<String> retV = xmlNode(doc, xPathCondition, fileEncoding);
			retStr = retV.firstElement().toString();
			retStr = retStr.trim();
		}catch(java.lang.Exception e){
			e.printStackTrace();
			retStr = "";
		}
		return retStr;
	}
	
	public static Vector<String> xmlNode(Document doc, String xPathCondition, String charSet){
		Vector<String> retV = new Vector<String>();
		XPathFactory xPathFactory = XPathFactory.newInstance();
		XPath xPath = xPathFactory.newXPath();
		XPathExpression expr = null;
		try {
			if(xPathCondition.startsWith("string(") || xPathCondition.startsWith("count(")){
				String str = xPath.evaluate(xPathCondition, doc, XPathConstants.STRING).toString();
				str = new String(str.getBytes(charSet));
				retV.add(str);
			}else{
				expr = xPath.compile(xPathCondition);
				java.lang.Object result = expr.evaluate(doc, XPathConstants.NODESET);
				NodeList node = (NodeList)result;
				for(int i=0;i<node.getLength();i++){
					String b = node.item(i).getNodeValue();
					if(xPathCondition.trim().endsWith("*")) {
						b = node.item(i).getNodeName();
					}
					try {
						b = new String(b.getBytes(charSet));
					} catch (java.io.UnsupportedEncodingException e) {
						e.printStackTrace();
						b = "";
					}
					retV.add(b);
				}
				if(retV.size() == 0){
					retV.add("");
				}
			}
		} catch (java.lang.Exception e) {
			e.printStackTrace();
			retV = new Vector<String>();
		}
		return retV;
	}
	
	public static String replace(String mainString, String oldString, String newString) {
		if (mainString == null) {
			return null;
		}
		if (oldString == null || oldString.length() == 0) {
			return mainString;
		}
		if (newString == null) {
			newString = "";
		}
		int i = mainString.lastIndexOf(oldString);
		if (i < 0)
			return mainString;
		StringBuffer mainSb = new StringBuffer(mainString);
		while (i >= 0) {
			mainSb.replace(i, (i + oldString.length()), newString);
			i = mainString.lastIndexOf(oldString, i - 1);
		}
		return mainSb.toString();
	}
	
	public static String getCheckRunTimeRun(long startTime) {
		long endTime = System.currentTimeMillis();
        long runtimeL = endTime - startTime;
        double runtime = runtimeL;
        runtime = runtime / (double)1000;
        return "" + runtime + " sec";
	}	
	
	public static String getTime() {
		long time = System.currentTimeMillis();
		java.text.SimpleDateFormat dayTime = new java.text.SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss");
		String str = dayTime.format(new java.util.Date(time));
		dayTime = null;
		return "[" + str + "] ";
	}
	
	public static java.sql.Connection getConnection(Document configDocument) {
		java.sql.Connection conn = null;
		String driver = Util.xmlNodeFirst(configDocument,
				"/Config/Database/ClassName/text()");
		String url = Util.xmlNodeFirst(configDocument,
				"/Config/Database/Url/text()");
		try {
			Class.forName(driver);
			String id = Util.xmlNodeFirst(configDocument,
					"/Config/Database/User/text()");
			String pw = Util.xmlNodeFirst(configDocument,
					"/Config/Database/Password/text()");
			System.out.println(driver);
			System.out.println(url+" / "+id+" / "+pw);
			conn = java.sql.DriverManager.getConnection(url, id, pw);
			conn.setAutoCommit(false);
		} catch (java.lang.ClassNotFoundException e) {
			e.printStackTrace();
			conn = (Connection) new Object();
		} catch (java.lang.Exception e) {
			e.printStackTrace();
			conn = (Connection) new Object();
		}
		return conn;
	}

	public static java.sql.Connection getConnectionURL(String dbname, String jdbcurl, String user_id, String user_pw) {
		java.sql.Connection conn = null;
		try {
			Class.forName(getClassName(dbname));
			conn = java.sql.DriverManager.getConnection(jdbcurl, user_id, user_pw);
			conn.setAutoCommit(false);
		} catch (java.lang.ClassNotFoundException e) {
			e.printStackTrace();
			conn = (Connection) new Object();
		} catch (java.lang.Exception e) {
			e.printStackTrace();
			conn = (Connection) new Object();
		}
		return conn;
	}	
	
	private static java.sql.Date getCurrentDate() {
	    java.util.Date today = new java.util.Date();
	    return new java.sql.Date(today.getTime());
	}
	
	public static String getClassName(String dbName){
		
		String className = "";
		if("oracle".equalsIgnoreCase(dbName))
			className = "oracle.jdbc.driver.OracleDriver";
//		else if("mssql".equalsIgnoreCase(dbName))
//			className = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
//		else if("mysql".equalsIgnoreCase(dbName))
//			className = "com.mysql.jdbc.Driver";
//		else if("cubrid".equalsIgnoreCase(dbName))
//			className = "cubrid.jdbc.driver.CUBRIDDriver";
//		else if("cassandra".equalsIgnoreCase(dbName))
//			className = "org.apache.cassandra.cql.jdbc.CassandraDriver";
		else if("tibero".equalsIgnoreCase(dbName))
			className = "com.tmax.tibero.jdbc.TbDriver";
		return className;
	}	
	
	public static void closeConnection(java.sql.Connection conn) {
		if (conn != null) {
			try {
				conn.commit();
				conn.close();
			} catch (java.lang.Exception e) {
				e.printStackTrace();
				conn = (Connection) new Object();
			}
		}
	}
	
	public static java.util.HashMap<String, String> selectTableColumn(java.sql.Connection conn, String tableName) {
		
		java.util.HashMap<String, String> columnData = null; 
		
		String sql = "SELECT * FROM ALL_TAB_COLS WHERE 1=1 AND table_name = '" + tableName + "'";
		
		java.sql.ResultSet rs = null;
        java.sql.Statement stmt = null;
     
        try {
        	
        	stmt = conn.createStatement();
        	rs = stmt.executeQuery(sql);
        	
        	columnData = new java.util.HashMap<String, String>();
        	
        	while(rs.next()) {
        		columnData.put(rs.getString("COLUMN_NAME"), rs.getString("DATA_TYPE"));
        	}
        	  	
        }catch(Exception e) {
        	e.printStackTrace();
        }finally {
        	try {
				rs.close();
				stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
        }
		return columnData;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static int insertDataTableOrg(java.sql.Connection conn, String tableName, java.util.HashMap<String,String> rowData){
		int retInt = 0;
		String sql = " INSERT INTO <TABLENAME>(<FIELDNAMES>) VALUES (<DATAS>) ";
		sql = replace(sql, "<TABLENAME>", tableName);
		java.util.Set keyset = rowData.keySet();
		java.util.Iterator it = keyset.iterator();
		String key = "";
		String fieldNames = "";
		String datas = "";
		java.util.Vector valuesVec = new java.util.Vector();
		
		while(it.hasNext()){
			key = (String)it.next();
			fieldNames = fieldNames + key + ",";
			datas = datas + "?" + ",";
			String value = rowData.get(key);
			if(value.indexOf("\\r\\n") != -1) {
				value = replace(value, "\\r\\n", "\\n");
			}
			if(value.indexOf("\\\\") != -1) {
				value = replace(value, "\\\\", "\\");
			}
			valuesVec.add(value);
		}

		if(fieldNames.indexOf(",") != -1){
			fieldNames = fieldNames.trim();
			fieldNames = fieldNames.substring(0,fieldNames.length()-1);
		}
		if(datas.indexOf(",") != -1){
			datas = datas.trim();
			datas = datas.substring(0,datas.length()-1);
		}
		sql = replace(sql, "<FIELDNAMES>", fieldNames);
		sql = replace(sql, "<DATAS>", datas);
		//System.out.println(sql);
		
		java.sql.ResultSet rs = null;
        java.sql.PreparedStatement ptmt = null;
        
        String data = "";
        try {
			ptmt = conn.prepareStatement(sql);
			for(int i=0;i<valuesVec.size();i++){
				data = valuesVec.get(i)+"";
				if("SYSDATE".equals(data)) {
					ptmt.setDate((i+1), getCurrentDate());
				}else if(data.startsWith("DATE:")){
					data = Util.replace(data, "DATE:", "");
					java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyyMMdd"); 
					java.util.Date date = sdf.parse(data);
					ptmt.setTimestamp((i+1), new java.sql.Timestamp(date.getTime()));
				}else if(data.startsWith("NIFIDATE:")){
					data = Util.replace(data, "NIFIDATE:", "");
					java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
					java.util.Date date = sdf.parse(data);
					ptmt.setTimestamp((i+1), new java.sql.Timestamp(date.getTime()));
				}else if(data.startsWith("FULLDATE:")){
					data = Util.replace(data, "FULLDATE:", "");
					java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd a HH:mm:ss"); 
					java.util.Date date = sdf.parse(data);
					ptmt.setTimestamp((i+1), new java.sql.Timestamp(date.getTime()));
				}else if(data.startsWith("INT:")){
					data = Util.replace(data, "INT:", "");
					ptmt.setInt((i+1),Integer.parseInt(data));
				}else {
					ptmt.setString((i+1),data);
				}
			}
			retInt = ptmt.executeUpdate();
        } catch (java.sql.SQLTimeoutException se1) {
            se1.printStackTrace();
        	System.out.println(se1.getMessage());
        	println(" [Exception4] "+se1.getMessage(), "err");
			println(" [Exception4] " + Util.getPrintStackTrace(se1), "err");
            retInt = 0;
        } catch (java.sql.SQLException se) {
        	
        	//들어갈 데이터의 크기가 해당 컬럼의 최대 크기보다 큰경우 발생하는 예외
        	if(se.getMessage().indexOf("too large") != -1) {
            	println(" [ValueLarge Exception] " + rowData.toString(), "err");
        	}
        	
        	//기본키 제약조건 : 기본키에 해당하는 기존 데이터를 지우고 새로운 데이터를 삽입하는 과정
        	if(se.getMessage().indexOf("PK") != -1){
        		deleteCount += 1;		
        		println("[PK Exception]", "err");
                retInt = deleteInsertDataTable(conn, tableName, rowData);
                retInt = 1;
        	}
        	
        } catch (java.lang.Exception se2) {
            se2.printStackTrace();
        	println(" [Exception6] "+se2.getMessage(), "err");
        	println(" [Exception6] " + Util.getPrintStackTrace(se2), "err");
        	
            System.out.println(rowData);
            retInt = 0;
        } finally {
            if(rs != null) {try{ rs.close(); }catch(java.lang.Exception e){retInt = 0;}};
            if(ptmt != null) {try{ ptmt.close(); }catch(java.lang.Exception e){retInt = 0;}};
            if(valuesVec != null) valuesVec.clear();
            if(rowData != null) rowData.clear();
        }
        return retInt;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static int deleteInsertDataTable(java.sql.Connection conn, String tableName, java.util.HashMap<String,String> rowData){
		int retInt = 0;
		String sql = "insert into <TABLENAME>(<FIELDNAMES>) values (<DATAS>)";
		sql = replace(sql, "<TABLENAME>", tableName);
		java.util.Set keyset = rowData.keySet();
		java.util.Iterator it = keyset.iterator();
		String key = "";
		String fieldNames = "";
		String datas = "";
		java.util.Vector valuesVec = new java.util.Vector();
		
		String[] primaryKey = Util.getPrimaryKey(conn, tableName).split("/");
		StringBuilder whereCondition = new StringBuilder();
		
		for(String priColumn : primaryKey) {
			if(rowData.get(priColumn).indexOf("NIFIDATE:") != -1) {
				String newData = Util.replace(rowData.get(priColumn), "NIFIDATE:","");
				
				/* 
				 * 기본키가 복합키이고, 복합키중에 날짜데이터를 포함하는 테이블에 대한 switch 구분
				 * 
				 * 급하게 수정한 부분으로 테이블 목록을 하드 코딩하여 사용중 - 차후 수정예정
				 * 
				 */
				
				switch(tableName) {
					case "NCM_BSNS_CHRGR":
					case "NCM_MBRS_SYS_ROLE":
					case "NPM_ECMSN_SPCL_FILD":
					case "NPM_ECMSN_TECL":
					case "NPM_SBJT_ADMN_MBR":
						whereCondition.append( "TO_CHAR(VLID_END_DE, 'YYYY-MM-DD hh24:mi:ss') = '"+ newData +"' AND ");
						break;
					case "NPM_PLDOC_ECVE_LOG":
						whereCondition.append( "TO_CHAR(RCVE_LOG_DT, 'YYYY-MM-DD hh24:mi:ss') = '"+ newData +"' AND ");
						break;
					case "NPM_SBJT_PADMSP_PTCP":
					case "NPM_SBJT_RCHSP_PTCP":
						whereCondition.append( "TO_CHAR(PTCP_RT_VLID_STR_DE, 'YYYY-MM-DD hh24:mi:ss') = '"+ newData +"' AND ");
						break;
					case "NPM_EVAL_CMIT_PRG":
						whereCondition.append( "TO_CHAR(PRG_DE, 'YYYY-MM-DD hh24:mi:ss') = '"+ newData +"' AND ");
						break;
					case "NPM_SBJT_AGRT_BUSIPP":
						whereCondition.append( "TO_CHAR(MODF_RQS_DT, 'YYYY-MM-DD hh24:mi:ss') = '"+ newData +"' AND ");
						break;	
					case "NPM_SBJT_MEMO":
						whereCondition.append( "TO_CHAR(MEMO_REG_DT, 'YYYY-MM-DD hh24:mi:ss') = '"+ newData +"' AND ");
						break;
					case "NPM_ECMIT_ECMSN_FRTEX":
						whereCondition.append( "TO_CHAR(EVAL_DE, 'YYYY-MM-DD hh24:mi:ss') = '"+ newData +"' AND ");
						break;
					case "NPM_SBJT_CHNG_PRG":
						whereCondition.append( "TO_CHAR(CHNG_PRG_DT, 'YYYY-MM-DD hh24:mi:ss') = '"+ newData +"' AND ");
						break;
					case "NPM_SBJT_AGRT_PRG":
						whereCondition.append( "TO_CHAR(AGRT_PRG_DT, 'YYYY-MM-DD hh24:mi:ss') = '"+ newData +"' AND ");
						break;
				}
			}else {
				whereCondition.append(priColumn + " = '" +rowData.get(priColumn) + "' AND ");
			}
		}
		
		String condition = whereCondition.toString().substring(0, whereCondition.toString().lastIndexOf("AND"));
		
		deleteHoldCheckDataRow(conn, tableName, condition); //키로부터 찾은 데이터를 Delete
		
		while(it.hasNext()){
			key = (String)it.next();
			fieldNames = fieldNames + key + ",";
			datas = datas + "?" + ",";
			valuesVec.add(rowData.get(key));
		}

		if(fieldNames.indexOf(",") != -1){
			fieldNames = fieldNames.trim();
			fieldNames = fieldNames.substring(0,fieldNames.length()-1);
		}
		if(datas.indexOf(",") != -1){
			datas = datas.trim();
			datas = datas.substring(0,datas.length()-1);
		}
		sql = replace(sql, "<FIELDNAMES>", fieldNames);
		sql = replace(sql, "<DATAS>", datas);

		//키에대한 데이터가 더이상 없는지 확인하고 데이터 삽입
		if(selectHoldCheckDataRow(conn, tableName, condition) == 0){
			java.sql.ResultSet rs = null;
	        java.sql.PreparedStatement ptmt = null;
	        String data = "";
	        try {
				ptmt = conn.prepareStatement(sql);
				
				for(int i=0;i<valuesVec.size();i++){
					data = valuesVec.get(i)+"";

					//날짜 데이터 변환
					if("SYSDATE".equals(data)) {
						ptmt.setDate((i+1), getCurrentDate());
					}else if(data.startsWith("DATE:")){
						data = Util.replace(data, "DATE:", "");
						ptmt.setDate((i+1), java.sql.Date.valueOf(data));
					}else if(data.startsWith("NIFIDATE:")){
						data = Util.replace(data, "NIFIDATE:", "");
						java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
						java.util.Date date = sdf.parse(data);
						
						ptmt.setTimestamp((i+1), new java.sql.Timestamp(date.getTime()));
					}else if(data.startsWith("FULLDATE:")){
						data = Util.replace(data, "FULLDATE:", "");
						java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd a HH:mm:ss"); 
						java.util.Date date = sdf.parse(data);
						
						ptmt.setTimestamp((i+1), new java.sql.Timestamp(date.getTime()));
					}else if(data.startsWith("INT:")){
						data = Util.replace(data, "INT:", "");
						ptmt.setInt((i+1),Integer.parseInt(data));
					}else {
						ptmt.setString((i+1),data);
					}
				}
				
				retInt = ptmt.executeUpdate();
				
	        } catch (java.lang.Exception se) {
	            //se.printStackTrace();
	            println(" [Exception5] "+se.getMessage(), "err");
	            retInt = 0;
	        } finally {
	            if(rs != null) {
	            	try{ 
	            		rs.close(); 
	            	}catch(java.lang.Exception e){
	            		retInt = 0;
	            	}
	            };
	            if(ptmt != null) {
	            	try{ 
	            		ptmt.close(); 
	            	}catch(java.lang.Exception e){
	            		retInt = 0;
	            	}
	            };
	            if(valuesVec != null) valuesVec.clear();
	            if(rowData != null) rowData.clear();
	        }
		}
		return retInt;
	}	
	
	public static void deleteHoldCheckDataRow(java.sql.Connection conn, String tableName, String paramCondition){
		//String sql = " DELETE FROM <TABLENAME> WHERE 1=1 AND WISE_ROW_CHANGE_HOLD_YN = 'N' AND NID = ? ; ";
		String sql = " DELETE FROM <TABLENAME> WHERE 1=1 AND " + paramCondition;
		sql = replace(sql, "<TABLENAME>", tableName);
		
		java.sql.ResultSet rs = null;
        java.sql.PreparedStatement ptmt = null;
        try {
			ptmt = conn.prepareStatement(sql);
            rs = ptmt.executeQuery();
            conn.commit();
        } catch (java.lang.Exception se) {
            se.printStackTrace();
        } finally {
            if(rs != null) {
            	try{ 
            		rs.close(); 
            	}catch(java.lang.Exception e){
            		e.printStackTrace();
            	}
            };
            if(ptmt != null) {
            	try{ 
            		ptmt.close(); 
            	}catch(java.lang.Exception e){
            			e.printStackTrace();
            	}
            };
        }
	}	
	
	public static int selectHoldCheckDataRow(java.sql.Connection conn, String tableName, String paramCondition){
		String sql = " SELECT count(*) as cnt FROM <TABLENAME> WHERE 1=1 AND " + paramCondition;
		
		sql = replace(sql, "<TABLENAME>", tableName);
		java.sql.ResultSet rs = null;
        java.sql.PreparedStatement ptmt = null;
        String selectData = "0";
        try {
			ptmt = conn.prepareStatement(sql);
            rs = ptmt.executeQuery();
            while(rs.next()){
            	selectData = rs.getString(1);
            }
            conn.commit();
        } catch (java.lang.Exception se) {
            se.printStackTrace();
            selectData = "0";
        } finally {
            if(rs != null) {
            	try{
            		rs.close(); 
            	}catch(java.lang.Exception e){
            		selectData = "0";
            	}
            };
            if(ptmt != null) {
            	try{ 
            		ptmt.close(); 
            	}catch(java.lang.Exception e){
            		selectData = "0";
            	}
            };
        }
        return Integer.parseInt(selectData);
	}	
	
	
	public static void println(String out){
		println(out, "info");
	}	
	
	public static void println(String out, String info){
		out = getTimeLog()+" ["+javaid+"] "+out;
		try{
			if ((System.getProperty("os.name")).indexOf("Windows") > -1) {
				//out = new String(out.getBytes("utf-8"),"8859_1");
			}
			System.out.println(out);
			
			String path1 = LOG_PATH;
			path1 = Util.replace(path1,"[YYYY-MM-DD]", Util.getDate());
			path1 = Util.replace(path1,"<TABLE>", TABLE);
			path1 = Util.replace(path1,"<INFO>", "info");
			writeFile(path1, out+"\n");
			
			if("err".equals(info)) {
				String path2 = LOG_PATH;
				path2 = Util.replace(path2,"[YYYY-MM-DD]", Util.getDate());
				path2 = Util.replace(path2,"<TABLE>", TABLE);
				path2 = Util.replace(path2,"<INFO>", info);
				writeFile(path2, out+"\n");
			}
			
		}catch(java.lang.Exception e){
			e.printStackTrace();
			out = "";
		}
	}

	public static String getTimeLog() {
		long time = System.currentTimeMillis();
		java.text.SimpleDateFormat dayTime = new java.text.SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss E", java.util.Locale.ENGLISH);
		String str = dayTime.format(new java.util.Date(time));
		dayTime = null;
		return "[" + str.toUpperCase() + "] ";
	}

	public static String getDate() {
		long time = System.currentTimeMillis();
		java.text.SimpleDateFormat dayTime = new java.text.SimpleDateFormat(
				"yyyy-MM-dd", java.util.Locale.ENGLISH);
		String str = dayTime.format(new java.util.Date(time));
		dayTime = null;
		return "" + str.toUpperCase() + "";
	}	
	
	public static void writeFile(String filePath, String contentData) {
		java.io.BufferedWriter output = null;
		java.io.File targetFile = null;
		try {
			if(filePath != null){
				targetFile = new java.io.File(filePath);
				targetFile.getParentFile().mkdirs();
				if (targetFile.isFile()) {
					//targetFile.delete();
				} else {
					targetFile.createNewFile();
				}
				output = new java.io.BufferedWriter(new java.io.OutputStreamWriter(
						new java.io.FileOutputStream(targetFile.getPath(), true),
						fileEncoding));
				output.write(contentData);
			}
		} catch (java.io.FileNotFoundException e1) {
			e1.printStackTrace();
			System.out.println(e1.toString());
		} catch (java.lang.Exception e2) {
			e2.printStackTrace();
			System.out.println(e2.toString());
		} finally {
			if (output != null)
				try {
					output.close();
				} catch (IOException e) {
					//e.printStackTrace();
				}
			if(targetFile != null) {
				targetFile = null;
			}
		}
	}
	
	public static String putTimeName() {
		long time = System.currentTimeMillis();
		java.text.SimpleDateFormat dayTime = new java.text.SimpleDateFormat(
				"yyyy-MM-dd-HH-mm-ss", java.util.Locale.ENGLISH);
		String str = dayTime.format(new java.util.Date(time));
		dayTime = null;
		return "" + str.toUpperCase() + "";
	}
	
	
	public static String getInfoFile(String source, String exe) {
		String result = "";
		java.io.File dir = new java.io.File(source);
		try {
			java.io.File[] file = dir.listFiles();
				for(int i = 0 ; i < file.length; i++) {
					if(file[i].isFile() && file[i].getName().endsWith(exe)){
						result = file[i].getAbsolutePath();
						break;
					}else if(file[i].isDirectory()){
						getInfoFile(file[i].getCanonicalPath().toString(), exe);
					}
				}
		}catch(Exception e){
			e.printStackTrace();
		}
		return result;
	}
	

	
	public static void endInfoFile(String source) {
		java.io.File infoFile = new java.io.File(source); 
		
		try{
			if(infoFile.isFile()){
				//System.out.println(infoFile.getName().substring(0,infoFile.getName().indexOf(".")));
				infoFile.renameTo(new File(infoFile.getAbsoluteFile() + "_end"));
				//System.out.println("Info end..");	
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	
	public static String createInfoFile(String source, String exe, String tableName, String column){
		String createFileName = tableName +"@" + column + "@" + Util.putTimeName() + "." + exe; 
		Util.createFile(source + "/" + createFileName, "");
		System.out.println("file Create");
		
		return source + "/" + createFileName;
	}
	
	public static String createInfoFile(String source, String exe, String tableName, String column, String timeStr){
		String createFileName = tableName +"@" + column + "@" + timeStr + "." + exe; 
		Util.createFile(source + "/" + createFileName, "");
		System.out.println("file Create");
		
		return source + "/" + createFileName;
	}
	
	public static String createInfoFile(String source, String exe, String timeStr){
		String createFileName = timeStr + "." + exe; 
		Util.createFile(source + "/" + createFileName, "");	
		return source + "/" + createFileName;
	}
	
	
	public static String maxValueFromSQL(java.sql.Connection conn, String table, String column) {
		String result = "";
		
		java.sql.ResultSet rs = null;
		java.sql.Statement stmt = null;
		
		String query = "SELECT MAX("+column+") FROM "+ table;
		
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			
			while(rs.next()) {
				result = rs.getString(1);
			}
			
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			try {
				rs.close();
				stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return result;
	}
	
	public static java.util.HashMap<String, String> checkTableType(Connection conn, String tableName) {
		java.util.HashMap<String, String> columns = new java.util.HashMap<String, String>();
		
		columns = Util.selectTableColumn(conn, tableName);
		
		return columns;
	}
	

	//테이블에 대한 기본키를 조회하는 쿼리를 실행
	public static String getPrimaryKey(Connection conn, String tableName) {
		//java.sql.Connection conn = Util.getConnectionURL(SubUtil.db_name, SubUtil.db_curl, SubUtil.db_id, SubUtil.db_pw);
		java.sql.PreparedStatement ptmt = null;
		java.sql.ResultSet rset = null;
		
		String sql = "SELECT C.COLUMN_NAME FROM USER_CONS_COLUMNS C, USER_CONSTRAINTS S WHERE C.CONSTRAINT_NAME = S.CONSTRAINT_NAME AND S.CONSTRAINT_TYPE = 'P' AND C.TABLE_NAME = '" + tableName + "'";
		StringBuilder result = new StringBuilder();
		
		try {
			ptmt = conn.prepareStatement(sql);
			rset = ptmt.executeQuery();
			
			while(rset.next()) {
				result.append(rset.getString(1) + "/");
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				ptmt.close();
				rset.close();
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
		
		// 키 대신 인덱스가 존재할수도 있으므로 인덱스를 통해 키에 해당하는 컬럼 조회
		 if("".equals(result.toString()) && "TBL0068".equals(tableName)) {
			 sql = "SELECT COLUMN_NAME FROM USER_IND_COLUMNS WHERE INDEX_NAME = ( SELECT INDEX_NAME FROM USER_INDEXES WHERE TABLE_NAME = '"+ tableName +"' )";
			 result = new StringBuilder();
			 
			 try {
				ptmt = conn.prepareStatement(sql);
				rset = ptmt.executeQuery();
				while(rset.next()) {
					result.append(rset.getString(1) + "/");
				}
			} catch (SQLException e) {
				e.printStackTrace();		
				}
		 }
		return result.toString();
	}
	
	
	public static int checkFileName(String tname, String element) {
		int count = 0;
		for( int i = 0; i < tname.length(); i++) {
			if(element.equals(Character.toString(tname.charAt(i)))) {
				count +=1;
			}
		}
		// element에 해당하는 문자 포함 개수를  return
		return count;
	}
	

	
	
	
}
