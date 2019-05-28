package common;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;

import org.apache.commons.lang3.StringUtils;

import vo.WzTFLslj;

import com.supermap.data.CursorType;
import com.supermap.data.DatasetType;
import com.supermap.data.DatasetVector;
import com.supermap.data.DatasetVectorInfo;
import com.supermap.data.Datasets;
import com.supermap.data.Datasource;
import com.supermap.data.DatasourceConnectionInfo;
import com.supermap.data.EngineType;
import com.supermap.data.FieldInfo;
import com.supermap.data.FieldInfos;
import com.supermap.data.FieldType;
import com.supermap.data.GeoCircle;
import com.supermap.data.GeoRegion;
import com.supermap.data.Geometry;
import com.supermap.data.Point2D;
import com.supermap.data.PrjCoordSys;
import com.supermap.data.PrjCoordSysType;
import com.supermap.data.Recordset;
import com.supermap.data.Workspace;

	/**
 * @author  作者 E-mail: 
 * @date 创建时间：2019年5月8日 下午5:31:18
 * @version 1.0 
 * @parameter 
 * @since  
 * @return  */

public class MapUtils {
	//连接参数获取
    private static final String iobjectJavaServer,iobjectJavaDatabase,iobjectJavaUser,iobjectJavaPassword;

    static {
    	/*获取连接iobjectjava的数据信息*/
//		ResourceBundle filePath = ResourceBundle.getBundle("config.map", Locale.getDefault());
		 // 定义数据源连接信息，假设以下所有数据源设置都存在
		iobjectJavaServer = "10.10.68.248:1521/orcl";
		iobjectJavaDatabase = "riskcontrol";
		iobjectJavaUser =  "riskcontrol";
		iobjectJavaPassword =  "riskcontrol";
		
//		iobjectJavaServer = filePath.getString("iobjectJavaServer");
//		iobjectJavaDatabase = filePath.getString("iobjectJavaDatabase");
//		iobjectJavaUser = filePath.getString("iobjectJavaUser");
//		iobjectJavaPassword = filePath.getString("iobjectJavaPassword");
    }
	
	/**连接数据源*/
//	public static Datasource connectDataSource(Workspace workspace,DatasourceConnectionInfo datasourceconnection,
//			String iobjectJavaServer,String iobjectJavaDatabase,String iobjectJavaUser,String iobjectJavaPassword){
	public static Datasource connectDataSource(Workspace workspace,DatasourceConnectionInfo datasourceconnection){
	
		   datasourceconnection.setEngineType(EngineType.ORACLEPLUS);
	       datasourceconnection.setServer(iobjectJavaServer);
	       datasourceconnection.setDatabase(iobjectJavaDatabase);
	       datasourceconnection.setUser(iobjectJavaUser); // riskcontrol_freeze
	       datasourceconnection.setPassword(iobjectJavaPassword);
		   datasourceconnection.setAlias("ORACLE");
	      // 打开数据源
	      Datasource datasource = workspace.getDatasources().open(datasourceconnection);
	      
	      // 获取的点数据集
//		  DatasetVector datasetVector = (DatasetVector)datasource.getDatasets().get("RISKMAP_ADDRESS");
	      
	      if (datasource == null) {
	          System.out.println("打开数据源失败");
	          return null;
	      } else {
	         System.out.println("数据源打开成功！");
	         return datasource;
	      }
	}
	// 创建面数据集
	public static DatasetVector createDataSet(String dataSetName,Datasource datasource){
		  
		  Datasets datasets = datasource.getDatasets();
	      DatasetVectorInfo datasetVectorInfo = new DatasetVectorInfo();
	      //面数据集类型
	      datasetVectorInfo.setType(DatasetType.REGION);
	      datasetVectorInfo.setName(dataSetName);
		  
		  DatasetVector datasetVector = datasets.create(datasetVectorInfo);
		  
		  PrjCoordSys prjCoordSys = new PrjCoordSys();
		  //地理经纬坐标
		  prjCoordSys.setType(PrjCoordSysType.PCS_EARTH_LONGITUDE_LATITUDE );
//			  GeoCoordSys geoCoordSys = new GeoCoordSys(GeoCoordSysType.GCS_CHINA_2000,GeoSpatialRefType.SPATIALREF_EARTH_LONGITUDE_LATITUDE );
//			  prjCoordSys.setCoordUnit(Unit.KILOMETER); // 千米
		  datasetVector.setPrjCoordSys(prjCoordSys);
		  
//		  Recordset recordset = datasetVector.getRecordset(false, CursorType.DYNAMIC);
//		  recordset.edit();
		  
	      return datasetVector;
	}
	
	/*给面数据集增加数据*/
	public static Recordset addDataToDatasetVector (DatasetVector datasetVector,List<WzTFLslj>  wzTFLsljsList,String radius){
		
		Recordset recordset = datasetVector.getRecordset(false, CursorType.DYNAMIC);
//		BatchEditor editor = recordset.getBatch();
//		// 设置批量更新每次提交的记录数目
//        editor.setMaxRecordCount(50);
//        // 从 World 数据集中读取几何对象和字段值，批量更新到 example 数据集中
//        editor.begin();
		double  [][]dataList = {{22,116,36},{10,110,22}};
		
		if(null!=wzTFLsljsList&&wzTFLsljsList.size()>0){
			for (WzTFLslj wzTFLslj :wzTFLsljsList){
				// 获取半径的值,如果半径为空，则不给相应的面数据集添加数据
				Object object = getRadius(radius,wzTFLslj);
				String jd = wzTFLslj.getJd();
				String wd = wzTFLslj.getWd();
				if(null != object&&StringUtils.isNotBlank(jd)&&StringUtils.isNotBlank(wd)){
					/*初始化半径*/
					double radiusN = Double.parseDouble(object.toString());
					
					/*半径应该大于0*/
					if (radiusN<=0){
						continue;
					}
					double distance =  getDistance(wzTFLslj);
					radiusN = radiusN*1000/distance;
					
					// 通过反射把vo类中字段整合到map中
					Map<String , Object> map =getResultByReflect(wzTFLslj);
//					Map<String , Object> map = new HashMap<String, Object>();
					
					recordset.edit();
					/*设置中心点坐标*/
					Point2D  point2D =new Point2D(Double.parseDouble(jd), Double.parseDouble(wd));
					
					GeoCircle geoCircle =new GeoCircle();
					geoCircle.setRadius(radiusN);
					geoCircle.setCenter(point2D);
					
					/*将圆几何对象转换为面几何对象。*/
					GeoRegion geoRegion = geoCircle.convertToRegion(50);
					Geometry geometry = (Geometry)geoRegion;
					
					recordset.update();
					recordset.addNew(geometry,map);
					// 没有这个数据集更新不能够成功
					recordset.update();
					
					if(geoCircle!=null){
						geoCircle.dispose();
					}
					if(geometry!=null){
						geometry.dispose();
					}
					if(geoRegion!=null){
						geoRegion.dispose();
					}
				}
				
				
			}
			
		}
		
		 // 批量操作统一提交
//        editor.update();

		return  recordset;
	}
	/**通过get请求来获取某个类中的字段的值*/
	public static Object getRadius(String radius,Object wzTFLslj){
		Object  radiusData = "";
		try {
			Class wzclass = wzTFLslj.getClass();
			String radiusName = "get"+radius.substring(0,1).toUpperCase()+radius.substring(1);
			Method method= wzclass.getDeclaredMethod(radiusName,new Class[]{});
			method.setAccessible(true);
			radiusData = (String)method.invoke(wzTFLslj);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		return radiusData;
	}
	/** 给面数据添加字段信息*/
	public static void addFieldInfo(DatasetVector datasetVector,Object wzTFLslj){
		  FieldInfos  fieldInfosold = datasetVector.getFieldInfos();
		  
		  Class clas = wzTFLslj.getClass(); 
		  Field []  fields =  clas.getDeclaredFields();
		  if (fields.length>0){
			  for (Field field : fields){
				  String  type = field.getType().getName();
				  String fieldName = field.getName();
				  if(fieldName.equals("serialVersionUID")||type.equals("java.util.List")){
					  continue;
				  }
				  FieldInfo fieldInfoNew = new FieldInfo();
				  fieldInfoNew.setName(fieldName);
				  // 别名
				  fieldInfoNew.setCaption(fieldName);
				  // 设置默认值
//				      fieldInfoNew.setDefaultValue("0");
				  if(type.equals("java.lang.String")){
					  fieldInfoNew.setType(FieldType.TEXT);
				  }else if(type.equals("java.util.Date")){
					  fieldInfoNew.setType(FieldType.DATETIME);
				  }
				  // 设置是否必录
//				      fieldInfoNew.setRequired(true);
			      fieldInfosold.add(fieldInfoNew);
			  } 
		  }
	}
		
	/**
     * @Description 通过java反射生成map数据
     * @Author 
     * @param obj
     * @return java.util.Map<java.lang.String,java.lang.Object>
     * @Date 20190509
     */
    public static Map<String,Object> getResultByReflect(Object obj){

        Map<String, Object> map = new HashMap<>();
        Field[] fields = obj.getClass().getDeclaredFields();
        for(Field field:fields){
            if (field.getName().equals("serialVersionUID")
                    || field.getType().getName().equals("java.util.List")){
                continue;
            }
            String fieldName = field.getName();
            fieldName = fieldName.substring(0, 1).toUpperCase()+fieldName.substring(1);
            try {
                Method method = obj.getClass().getMethod("get"+fieldName, new Class[]{});
                Object invokeResult = method.invoke(obj);
                //假如为日期类型，需要处理后再进行放入map中
                if(field.getType().getName().equals("java.util.Date")){
                	SimpleDateFormat  format = new SimpleDateFormat("yyyy/MM/dd");
     				try {
     					invokeResult = format.parse(format.format(invokeResult));
     				} catch (ParseException e) {
     					// TODO Auto-generated catch block
     					e.printStackTrace();
     				}
                }
                map.put(field.getName(), invokeResult);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("构建插值数据异常");
            }
        }
        return map;
    }
    
   /** 计算相差一个经度和一个纬度之间的距离*/
    public static double getDistance(WzTFLslj wzTFLslj){
    	int  EARTH_RADIUS = 6378137; 
    	String lng1 = wzTFLslj.getJd();
    	String lat1 = wzTFLslj.getWd();
    	double  latDouble1 = rad(Double.parseDouble(lat1));
    	double  latDouble2 = rad(Double.parseDouble(lat1));
    	
    	double latSub = latDouble1 - latDouble2;
    	
//    	double  lngDouble1 = rad(Double.parseDouble(lng1));
//    	double  lngDouble2 = rad(Double.parseDouble(lng1)+1);
    	double lonSub =rad(Double.parseDouble(lng1)) - rad(Double.parseDouble(lng1)+1);
    	double distance = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(latSub / 2), 2) + Math.cos(latDouble1) * Math.cos(latDouble2) * Math.pow(Math.sin(lonSub / 2), 2)));
    	distance = distance*EARTH_RADIUS;
    	distance = Math.round(distance * 10000) / 10000;
//        var b = this.rad(lng1) - this.rad(lng2);
//        var s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2) + Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2)));
//        s = s * EARTH_RADIUS;
//        s = Math.round(s * 10000) / 10000;
//        return s;
    	
    	return distance;
    }
    
    public static double rad(double distance){
    	return distance * Math.PI / 180.0;
    }
	
	
}
