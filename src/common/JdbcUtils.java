package common;
	/**
 * @author  作者 E-mail: 
 * @date 创建时间：2019年5月28日 上午11:05:22
 * @version 1.0 
 * @parameter 
 * @since  
 * @return  */
public class JdbcUtils {
	//连接参数获取
    private static final String iobjectJavaServer,iobjectJavaDatabase,iobjectJavaUser,iobjectJavaPassword;
    
    static {
		 // 定义数据源连接信息，假设以下所有数据源设置都存在
		iobjectJavaServer = "10.10.68.248:1521/orcl";
		iobjectJavaDatabase = "riskcontrol";
		iobjectJavaUser =  "riskcontrol";
		iobjectJavaPassword =  "riskcontrol";
    }
    
    
    
	
}
