package mutualTransfer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import vo.Gps;

import common.MapTransferUtils;
	/**
 * @author  作者 E-mail: 
 * @date 创建时间：2019年5月10日 下午1:56:27
 * @version 1.0 
 * @parameter 
 * @since  
 * @return  */
public class OracleTransferTo84 {
	
	public static String driverClassName = "oracle.jdbc.OracleDriver";
	public static String oracleName="jdbc:oracle:thin:@10.10.68.248:1521:orcl";
	public static String username="riskcontrol_freeze";
	public static String password="riskcontrol_freeze";
	
	public static void main (String []args){
		
//		OracleTransferTo84.connectOracle(driverClassName,oracleName,username,password);
		
		System.out.println("hello");
		int sum = 900178;		
		for(int i =0 ;i<10;i++){
			int pageNo= i;
			int endCount =0; 
			if(i!=9){
				endCount = (pageNo+1)*sum/10;
			}else {
				endCount = (pageNo+1)*sum/10+sum%10;
			}
//			MoreThread  mThread = new MoreThread(4666,4829);
//			MoreThread  mThread = new MoreThread(i*6500*2+1,(i+1)*6500*2);
			// start 的值大于end的值
			MoreThreadTo84  mThread = new MoreThreadTo84(endCount,pageNo*sum/10);
			mThread.start();
		}		
	}
	
	
	
	
	public  static void connectOracle(String driverClassName,String oracleName,String username,String password,int start,int end){
		  Connection connect = null;
	      PreparedStatement preState = null;
	      ResultSet resultSet = null;
			try {
	//         Class.forName("oracle.jdbc.OracleDriver");
//			   Class.forName("oracle.jdbc.driver.OracleDriver");
			   Class.forName(driverClassName);
	
	//         Driver driver = new OracleDriver();
	//         DriverManager.deregisterDriver(driver);
	         
//	         connect = DriverManager.getConnection("jdbc:oracle:thin:@10.10.68.248:1521:orcl", "riskcontrol_freeze", "riskcontrol_freeze");           
	         connect = DriverManager.getConnection(oracleName, username, password);           
	     	
	//         statement = connect.createStatement();
	
	        
//	         String sql = "select POINTX_02,POINTY_02,SMID from SMDTV_2  where VALIDSTATUS ='9' and SMID = 894";
	         String sql = "select POINTX_02,POINTY_02,SMID  from(select a.*,ROWNUM rn from(select* from  SMDTV_2 where VALIDSTATUS ='9' order by SMID ) a where ROWNUM<=(?)) where rn>?";

	//         PreparedStatement preState = connect.prepareStatement("select  * from tb1_dept where id = ?");
	         preState = connect.prepareStatement(sql);
	         preState.setInt(1, start);
	         preState.setInt(2, end);
	//         resultSet = statement.executeQuery("select  * from RISKINFO_CLAIM where SERIALNO ='26'");
	//         resultSet = statement.executeQuery("select * from SMDTV_45 where SMID=3");
	         resultSet = preState.executeQuery();        
	
	         for (int i=1;resultSet.next();i++){
	             double POINTX_02 = Double.parseDouble(resultSet.getString("POINTX_02"));
	             double POINTY_02 = Double.parseDouble(resultSet.getString("POINTY_02"));
	             Integer SMID =Integer.parseInt(resultSet.getString("SMID"));
	             Gps gps =  MapTransferUtils.gcj_To_Gps84(POINTY_02,POINTX_02);
	             updateOracle(gps, SMID);
	             
	             System.out.println("=============="+POINTX_02+":"+POINTY_02+":"+gps.getWgLon()+":"+gps.getWgLat());
	             System.out.println("---------------------第"+i+"条数据转换成功-----------------------");
	         }
	     } catch (Exception e) {
	         e.printStackTrace();
	     }finally {
	        
	             try {
	                 if (resultSet!=null) resultSet.close();
	//                 if (statement!=null) statement.close();
	                 if (preState!=null) preState.close();
	                 if (connect!=null) connect.close();
	             } catch (SQLException e) {
	                 e.printStackTrace();
	             }
	     }
			
	}
	
	public static void 	updateOracle(Gps gps,Integer SMID){	
		Connection connect = null;
//      Statement statement = null;
		PreparedStatement preState = null;
		ResultSet resultSet = null;
		 try {
			 Class.forName(driverClassName);
			connect = DriverManager.getConnection(oracleName, username, password);
			
			String sql = "update SMDTV_2 set POINTX_2000 =? ,POINTY_2000 =?,VALIDSTATUS='8' where SMID =?";
//           PreparedStatement preState = connect.prepareStatement("select  * from tb1_dept where id = ?");
           preState = connect.prepareStatement(sql);
           double  lon = MapTransferUtils.stringToDouble(gps.getWgLon());
           double lat = MapTransferUtils.stringToDouble(gps.getWgLat());
           preState.setDouble(1, lon);
           preState.setDouble(2, lat);
           preState.setInt(3, SMID);
           
           preState.executeUpdate();
		   System.out.println("========success============:"+SMID);
		}catch (Exception e) {
	           e.printStackTrace();
        }finally {
               try {
                   if (resultSet!=null) resultSet.close();
                   
//	                   if (statement!=null) statement.close();
//	                  
                   if (preState!=null) preState.close();
                   if (connect!=null) connect.close();
               } catch (SQLException e) {
                   e.printStackTrace();
               }
       }

	}
	
}

class MoreThreadTo84 extends Thread {
	
	private int start;
	private int end;
	
//	private static final String userName = "riskcontrol_freeze";
//	private static final String passWord = "riskcontrol_freeze";
//	private static final String riskMapAddress = "SMDTV_2";
//	private static final String serverNameAddress = "http://10.10.1.156:8090/";
	
	public static String driverClassName = "oracle.jdbc.OracleDriver";
	public static String oracleName="jdbc:oracle:thin:@10.10.68.248:1521:orcl";
	public static String username="riskcontrol_freeze";
	public static String password="riskcontrol_freeze";
	
	public MoreThreadTo84() {
		super();
	}
	public MoreThreadTo84(int start, int end) {
		super();
		this.start = start;
		this.end = end;
	}
	
	public  void run(){
//		this.updateDtvData(start,end);
	   OracleTransferTo84.connectOracle(driverClassName,oracleName,username,password,start,end);
	}
}
