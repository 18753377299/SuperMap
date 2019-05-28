package createDataSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import vo.MapData;

import com.supermap.data.CursorType;
import com.supermap.data.DatasetVector;
import com.supermap.data.Datasource;
import com.supermap.data.DatasourceConnectionInfo;
import com.supermap.data.Feature;
import com.supermap.data.Geometry;
import com.supermap.data.Point2D;
import com.supermap.data.Recordset;
import com.supermap.data.Rectangle2D;
import com.supermap.data.Workspace;

import common.MapTransferUtils;
import common.MapUtils;
	/**
 * @author  作者 E-mail: 
 * @date 创建时间：2019年5月28日 上午9:09:41
 * @version 1.0 
 * @parameter 
 * @since  
 * @return  */
public class UpdateCenterPoint {
	public static void main(String []args){
		getCenterPoint();
	}
	
	public static void getCenterPoint(){
		
		Workspace  workspace =new Workspace();
		// 定义数据源连接信息，假设以下所有数据源设置都存在
	    DatasourceConnectionInfo datasourceconnection = new  DatasourceConnectionInfo();
		//进行数据源的连接
		Datasource datasource =MapUtils.connectDataSource(workspace,datasourceconnection);
		// 获取的点数据集
		DatasetVector datasetVector = (DatasetVector)datasource.getDatasets().get("china_province");
//		String filter = "";
//		Recordset recordset_7 = datasetVector.query(filter,CursorType.DYNAMIC );
		
		Recordset recordset_new = datasetVector.getRecordset(false, CursorType.DYNAMIC);
		
		Map<Integer,Feature> features = recordset_new.getAllFeatures();
		List<MapData> mapDataList = new ArrayList<MapData>();
		for (Feature feature : features.values()){
			MapData mapData =new MapData();
			Geometry geometry= feature.getGeometry(); 
			String adminCode =  feature.getString("AdminCode");
			Point2D  point2D = geometry.getInnerPoint();
			// 返回数据集中包含所有对象的最小外接矩形。
//			Rectangle2D rectangle2D = geometry.getBounds();
//			Point2D  point2D = rectangle2D.getCenter();
			double  centerX = MapTransferUtils.stringToDouble(point2D.getX());
			double centerY = MapTransferUtils.stringToDouble( point2D.getY());
			
			mapData.setAdminCode(adminCode);
			mapData.setLon(centerX);
			mapData.setLat(centerY);
			mapDataList.add(mapData);
		}
		UpdateCenterPointInfo(mapDataList);
		
	}
	/*批量更新sql信息*/
	public  static void  UpdateCenterPointInfo(List<MapData> mapDataList){
		Connection connect = null;
//      Statement statement = null;
        PreparedStatement preState = null;
        ResultSet resultSet = null;
		try {
//         Class.forName("oracle.jdbc.OracleDriver");
			Class.forName("oracle.jdbc.driver.OracleDriver");
         
		    connect = DriverManager.getConnection("jdbc:oracle:thin:@10.10.68.248:1521:orcl", "riskcontrol", "riskcontrol");
			String sql = "update SMDTV_222 set CENTERX=? ,CENTERY=? where ADMINCODE=?";
			
//           PreparedStatement preState = connect.prepareStatement("select  * from tb1_dept where id = ?");
           preState = connect.prepareStatement(sql);
           if(mapDataList!=null&&mapDataList.size()>0){
        	   for(MapData mapData:mapDataList){
        		   preState.setDouble(1, mapData.getLon());
        		   preState.setDouble(2, mapData.getLat());
        		   preState.setString(3, mapData.getAdminCode());
        		   // 添加批处理SQL
        		   preState.addBatch();
        	   }
           }
           preState.executeBatch();
         
         }catch(Exception e){
        	 e.printStackTrace();
         }finally{
        	 try {
                 if (resultSet!=null) resultSet.close();
                 if (preState!=null) preState.close();
                 if (connect!=null) connect.close();
             } catch (SQLException e) {
                 e.printStackTrace();
             }
         }          

		
	}
	
}
