package oracle;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import common.JdbcUtils;


public class OracleToOracle_10M {
	
	public static void main(String [] args){
		Connection connect = null;
//        Statement statement = null;
        PreparedStatement preState = null;
        ResultSet resultSet = null;
		try {
//           Class.forName("oracle.jdbc.OracleDriver");
			// 进行参数连接
			connect = JdbcUtils.getJdbcConnection();
			
            System.out.println(connect);  
           
//         statement = connect.createStatement();
          
           String sql = "select * from SMDTV_21 ";
//           PreparedStatement preState = connect.prepareStatement("select  * from tb1_dept where id = ?");
           preState = connect.prepareStatement(sql);
           
//         resultSet = statement.executeQuery("select  * from RISKINFO_CLAIM where SERIALNO ='26'");
//         resultSet = statement.executeQuery("select * from SMDTV_45 where SMID=3");
           	
           resultSet = preState.executeQuery();        

          
           while (resultSet.next()){
//        	   byte[] SMGEOMETRY = null;
               Integer  SMID = resultSet.getInt("SMID");
               Integer SMKEY = resultSet.getInt("SMKEY");
               BigDecimal SMSDRIW = new BigDecimal(resultSet.getString("SMSDRIW"));
               BigDecimal SMSDRIN = new BigDecimal(resultSet.getString("SMSDRIN"));
               BigDecimal SMSDRIE = new BigDecimal(resultSet.getString("SMSDRIE"));
               BigDecimal SMSDRIS = new BigDecimal(resultSet.getString("SMSDRIS"));
               BigDecimal SMGRANULE = new BigDecimal(resultSet.getString("SMGRANULE"));
               byte[] SMGEOMETRY =resultSet.getBytes("SMGEOMETRY");
               Integer  SMUSERID = resultSet.getInt("SMUSERID");
               Integer  SMLIBTILEID = resultSet.getInt("SMLIBTILEID");
               BigDecimal SMAREA = new BigDecimal(resultSet.getString("SMAREA"));
               BigDecimal SMPERIMETER = new BigDecimal(resultSet.getString("SMPERIMETER"));
               
               String  ADMINCODE = resultSet.getString("ADMINCODE");
               String  KIND = resultSet.getString("KIND");
               String  NAME = resultSet.getString("NAME");
               String  PY = resultSet.getString("PY");
               String  CITYADCODE = resultSet.getString("CITYADCODE");
               String  PROADCODE = resultSet.getString("PROADCODE");
               BigDecimal CENTERX = new BigDecimal(resultSet.getString("CENTERX"));
               BigDecimal CENTERY = new BigDecimal(resultSet.getString("CENTERY"));
               BigDecimal LEVELFLAG = new BigDecimal(resultSet.getString("LEVELFLAG"));
               
               String  PROVINCENAME = resultSet.getString("PROVINCENAME");
               String  CITYNAME = resultSet.getString("CITYNAME");
               System.out.println(SMID+"   "+SMKEY+"   "+SMSDRIW); 
               
               OracleToOracle_10M.insertData(SMID,SMKEY,SMSDRIW,SMSDRIN,SMSDRIE,SMSDRIS,SMGRANULE,SMGEOMETRY,
            		   SMUSERID,SMLIBTILEID,SMAREA,SMPERIMETER,
            		  ADMINCODE,KIND, NAME,PY,CITYADCODE,PROADCODE,CENTERX,CENTERY,LEVELFLAG,PROVINCENAME, CITYNAME);
           }
       } catch (Exception e) {
           e.printStackTrace();
       }finally {
    	   /* 关闭数据库资源*/
    	   JdbcUtils.closeResource( resultSet, preState,connect);
       }
	}
//	public static void 	insertData(Integer SMID,Integer SMKEY,BigDecimal SMSDRIW,BigDecimal SMSDRIN,BigDecimal SMSDRIE,
//			BigDecimal SMSDRIS,BigDecimal SMGRANULE,byte[] SMGEOMETRY,Integer  SMUSERID,Integer  SMLIBTILEID,
//			BigDecimal SMAREA,BigDecimal SMPERIMETER,Integer PID,String TFBH){	
	public static void 	insertData(Integer SMID,Integer SMKEY,BigDecimal SMSDRIW,BigDecimal SMSDRIN,BigDecimal SMSDRIE,
			BigDecimal SMSDRIS,BigDecimal SMGRANULE,byte[] SMGEOMETRY,Integer  SMUSERID,Integer  SMLIBTILEID,
			BigDecimal SMAREA,BigDecimal SMPERIMETER,
			String  ADMINCODE,String  KIND, String  NAME,String  PY,String  CITYADCODE, String  PROADCODE,BigDecimal CENTERX,
			BigDecimal CENTERY,BigDecimal LEVELFLAG,String  PROVINCENAME,String  CITYNAME){	
		Connection connect = null;
//      Statement statement = null;
		PreparedStatement preState = null;
		ResultSet resultSet = null;
		 try {
			 // 进行参数连接
			connect = common.JdbcUtils.getJdbcConnection();

			String sql = "insert into SMDTV_172(SMID,SMKEY,SMSDRIW,SMSDRIN,SMSDRIE,SMSDRIS,SMGRANULE,SMGEOMETRY,SMUSERID,SMLIBTILEID,SMAREA,SMPERIMETER,"
					+ "ADMINCODE,KIND, NAME,PY,CITYADCODE,PROADCODE,CENTERX,CENTERY,LEVELFLAG,PROVINCENAME, CITYNAME)"
					+ " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
//           PreparedStatement preState = connect.prepareStatement("select  * from tb1_dept where id = ?");
           preState = connect.prepareStatement(sql);
          
           preState.setInt(1, SMID);
           preState.setInt(2, SMKEY);
           preState.setBigDecimal(3, SMSDRIW);
           preState.setBigDecimal(4, SMSDRIN);
           preState.setBigDecimal(5, SMSDRIE);
           preState.setBigDecimal(6, SMSDRIS);
           preState.setBigDecimal(7, SMGRANULE);
           preState.setBytes(8, SMGEOMETRY);
           preState.setInt(9, SMUSERID);
           preState.setInt(10, SMLIBTILEID);
           preState.setBigDecimal(11, SMAREA);
           preState.setBigDecimal(12, SMPERIMETER);
           
           preState.setString(13, ADMINCODE);
           preState.setString(14, KIND);
           preState.setString(15, NAME);
           preState.setString(16, PY);
           preState.setString(17, CITYADCODE);
           preState.setString(18, PROADCODE);
           preState.setBigDecimal(19, CENTERX);
           preState.setBigDecimal(20, CENTERY);
           preState.setBigDecimal(21, LEVELFLAG);
           preState.setString(22, PROVINCENAME);
           preState.setString(23, CITYNAME);
           
           preState.executeUpdate();
		   System.out.println("========success============");
		}catch (Exception e) {
	           e.printStackTrace();
        }finally {
        	/* 关闭数据库资源*/
     	   JdbcUtils.closeResource( resultSet, preState,connect);
       }

	}
	
}
