
package createDataSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import vo.WzTFLslj;

import com.supermap.data.CursorType;
import com.supermap.data.DatasetVector;
import com.supermap.data.Datasource;
import com.supermap.data.DatasourceConnectionInfo;
import com.supermap.data.GeoCircle;
import com.supermap.data.GeoRegion;
import com.supermap.data.Geometry;
import com.supermap.data.Point2D;
import com.supermap.data.Recordset;
import com.supermap.data.Workspace;
import common.MapUtils;
	/**
 * @author  作者 E-mail: 
 * @date 创建时间：2019年5月7日 上午9:42:20
 * @version 1.0 
 * @parameter 
 * @since  
 * @return  */
public class OperateDataSetByOracle {
	/*静态变量*/
	private static final  String  iobjectJavaServer = "10.10.68.248:1521/orcl";
	private static final  String  iobjectJavaDatabase = "riskcontrol";
	private static final  String  iobjectJavaUser = "riskcontrol";
	private static final  String  iobjectJavaPassword = "riskcontrol";
	private static final  String  riskMap_address  = "SMDTV_60";
	
	public static void main(String[]args){
		/*创建oracle数据库连接*/
		createOracleConnection();
		/*创建圆形 的面数据集*/
//		createRegionDataSet();
		
//		WzTFLslj wzTFLslj =new WzTFLslj();
//		wzTFLslj.setRadius7("11");
//		wzTFLslj.setRadius10("22");
//		String data = MapUtils.getRadius("radius7",wzTFLslj).toString();
//		String data1= MapUtils.getRadius("radius10",wzTFLslj).toString();
//		System.out.println("================="+data+":"+data1);
		
	}
	
	public static void createOracleConnection(){
		Connection connect = null;
//      Statement statement = null;
		PreparedStatement preState = null;
		ResultSet resultSet = null;
		
		 try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			connect = DriverManager.getConnection("jdbc:oracle:thin:@10.10.68.248:1521:orcl", "riskcontrol", "riskcontrol");
			
//			String sql = "insert into TF_7M(SMID,SMKEY,SMSDRIW,SMSDRIN,SMSDRIE,SMSDRIS,SMGRANULE,SMGEOMETRY,SMUSERID,SMLIBTILEID,SMAREA,SMPERIMETER,PID,TFBH)"
//					+ " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			String sql = "select * from WZ_TFLSLJ where TFBH in ('201310','201822')";
//           PreparedStatement preState = connect.prepareStatement("select  * from tb1_dept where id = ?");
           preState = connect.prepareStatement(sql);
           resultSet = preState.executeQuery(); 
           /*历史台风路径*/
           List<WzTFLslj>  wzTFLsljsList = new ArrayList<WzTFLslj>();
           
           while (resultSet.next()){
        	   
        	   WzTFLslj wzTFLslj =new WzTFLslj();
        	   wzTFLslj.setTfbh(resultSet.getString("TFBH"));
        	   wzTFLslj.setRqsj(resultSet.getDate("RQSJ"));
        	   wzTFLslj.setWd(resultSet.getString("WD"));
        	   wzTFLslj.setJd(resultSet.getString("JD"));
        	   wzTFLslj.setZxqy(resultSet.getString("ZXQY"));
        	   wzTFLslj.setZxfs(resultSet.getString("ZXFS"));
        	   wzTFLslj.setYdsd(resultSet.getString("YDSD"));
        	   wzTFLslj.setYdfx(resultSet.getString("YDFX"));
        	   wzTFLslj.setRadius7(resultSet.getString("RADIUS7"));
        	   wzTFLslj.setRadius10(resultSet.getString("RADIUS10"));
        	   wzTFLslj.setDepict(resultSet.getString("DEPICT"));
        	   wzTFLslj.setBedit(resultSet.getString("BEDIT"));
        	   wzTFLsljsList.add(wzTFLslj);
           }
           createRegionDataSet(wzTFLsljsList);
		   System.out.println("========success============");
		}catch (Exception e) {
	           e.printStackTrace();
        }finally {
           try {
               if (resultSet!=null) resultSet.close();
//	               if (statement!=null) statement.close();
               if (preState!=null) preState.close();
               if (connect!=null) connect.close();
           } catch (SQLException e) {
               e.printStackTrace();
           }
       }
	}
	
	/*创建圆形 的面数据集-----七级风圈和十级风圈的面数据集各一个*/
	public static void createRegionDataSet(List<WzTFLslj>  wzTFLsljsList){
		
		Workspace workspace = new Workspace();
		// 定义数据源连接信息，假设以下所有数据源设置都存在
	    DatasourceConnectionInfo datasourceconnection = new  DatasourceConnectionInfo();
		//进行数据源的连接
		Datasource datasource =MapUtils.connectDataSource(workspace,datasourceconnection,iobjectJavaServer,iobjectJavaDatabase,iobjectJavaUser,iobjectJavaPassword);
	    // 创建面数据集
		String dataSetName_7 = "TF_7M",dataSetName_10="TF_10M";
		// 创建七级风圈数据集
		DatasetVector datasetVector_7 =  MapUtils.createDataSet(dataSetName_7,datasource);
		// 创建十级风圈数据集
		DatasetVector datasetVector_10 =  MapUtils.createDataSet(dataSetName_10,datasource);
		
		//为面数据集增加vo类中的字段
		WzTFLslj wzTFLslj =new WzTFLslj();
		MapUtils.addFieldInfo(datasetVector_7,wzTFLslj);
		MapUtils.addFieldInfo(datasetVector_10,wzTFLslj);
		
        /**添加多个面数据集的方式*/
        double  [][]dataList = {{22,116,36},{10,110,22}};
        /*给面数据集增加数据*/
        Recordset recordset_7= MapUtils.addDataToDatasetVector(datasetVector_7,wzTFLsljsList,"radius7");
        
        Recordset recordset_10= MapUtils.addDataToDatasetVector(datasetVector_10,wzTFLsljsList,"radius10");
        
//       Recordset recordsetNew = addMoreRecordset(datasetVector_7 ,dataList);
        
//       if(recordsetNew!=null){
//    	   recordsetNew.close();
//    	   recordsetNew.dispose();
//       }
        if(recordset_10!=null){
        	recordset_10.close();
        	recordset_10.dispose();
		}
        if(recordset_7!=null){
        	recordset_7.close();
        	recordset_7.dispose();
		}
//	    if(fieldInfoNew!=null){
//	    	fieldInfoNew.dispose();
//		}
		if(datasetVector_10!=null){
			datasetVector_10.close();
		}
		if(datasetVector_7!=null){
			datasetVector_7.close();
		}
	    if(datasource!=null){
			datasource.close();
		}
	    if(datasourceconnection!=null){
			datasourceconnection.dispose();
		}
		if(workspace!=null){
			// 关闭工作空间
			workspace.close();
			// 释放该对象所占用的资源
			workspace.dispose();
		}
		
	}
	
	/**添加多个面数据集的方式*/
	public static Recordset addMoreRecordset(DatasetVector datasetVector ,double  [][]dataList){
//		BatchEditor editor = recordset.getBatch();
//		// 设置批量更新每次提交的记录数目
//        editor.setMaxRecordCount(50);
//        // 从 World 数据集中读取几何对象和字段值，批量更新到 example 数据集中
//        editor.begin();
		Recordset recordset = datasetVector.getRecordset(false, CursorType.DYNAMIC);
		
		if(dataList.length>0){
			for(int i=0;i<dataList.length;i++){
				Map<String , Object> map =new HashMap<String, Object>();
				
				recordset.edit();
				/*初始化半径*/
				double radius = dataList[i][0];
				/*设置中心点坐标*/
				Point2D  point2D =new Point2D(dataList[i][1], dataList[i][2]);
				
				GeoCircle geoCircle =new GeoCircle();
				geoCircle.setRadius(radius);
				geoCircle.setCenter(point2D);
				
				/*将圆几何对象转换为面几何对象。*/
				GeoRegion geoRegion = geoCircle.convertToRegion(50);
				Geometry geometry = (Geometry)geoRegion;
				
				recordset.update();
				recordset.addNew(geometry,map);
				// 没有这个数据集更新不能够成功
				recordset.update();
				
				if(geoRegion!=null){
					geoRegion.dispose();
				}
			}
		}
		 // 批量操作统一提交
//        editor.update();

		return  recordset;
	}

}
