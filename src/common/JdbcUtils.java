package common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
	/**
 * @author  作者 E-mail: 
 * @date 创建时间：2019年5月28日 上午11:05:22
 * @version 1.0 
 * @parameter 
 * @since  
 * @return  */
public class JdbcUtils {
	//连接参数获取
	private static final String oracleDriver,oracleUrl,javaUser,javaPassword;
    
    static {
		 // 定义数据源连接信息，假设以下所有数据源设置都存在
    	oracleDriver = "oracle.jdbc.driver.OracleDriver";
		oracleUrl = "jdbc:oracle:thin:@10.10.68.248:1521:orcl";
		javaUser =  "riskcontrol";
		javaPassword =  "riskcontrol";
    }
    
    /*进行oracle数据库的连接*/
    public static Connection getJdbcConnection(){
    	Connection connect = null;
    	try {
			Class.forName(oracleDriver);
			connect = DriverManager.getConnection(oracleUrl, javaUser, javaPassword);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		} 
    	return connect;
    }
   /* 关闭数据库资源*/
    public static void closeResource(ResultSet resultSet,PreparedStatement preState,Connection connect){
    	 try {
             if (resultSet!=null) resultSet.close();
             if (preState!=null) preState.close();
             if (connect!=null) connect.close();
         } catch (SQLException e) {
             e.printStackTrace();
         }
    }
    
    
    
    
    
    
    
	
}
