package dissolveDatasetVector;

import ins.framework.dao.database.DatabaseDao;
import ins.framework.dao.database.support.QueryRule;
import ins.framework.rpc.annotation.Rpc;
import ins.framework.web.AjaxResult;

import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.picc.riskctrl.common.schema.WzCorporeLs;
import com.picc.riskctrl.common.schema.WzCorporeLsId;
import com.picc.riskctrl.common.schema.WzCorporeLsP;
import com.picc.riskctrl.common.schema.WzCorporeLsPId;
import com.picc.riskctrl.common.schema.WzCorporeYjP;
import com.picc.riskctrl.common.util.BeanUtils;
import com.picc.riskctrl.common.util.MapUtils;
import com.picc.riskctrl.map.service.facade.MapService;
import com.picc.riskctrl.map.service.facade.MapWarnService;
import com.picc.riskctrl.map.vo.RiskMapAddressField;
import com.picc.riskctrl.map.vo.WzTFLsljVo;
import com.picc.riskctrl.map.vo.WzTFYbljVo;
import com.picc.riskctrl.map.vo.request.WZ_tfbh;
import com.picc.riskctrl.map.vo.response.Wz_Corporels_byMid;
import com.supermap.data.CursorType;
import com.supermap.data.DatasetVector;
import com.supermap.data.Datasource;
import com.supermap.data.DatasourceConnectionInfo;
import com.supermap.data.GeoRegion;
import com.supermap.data.Geometry;
import com.supermap.data.QueryParameter;
import com.supermap.data.Recordset;
import com.supermap.data.SpatialQueryMode;
import com.supermap.data.Workspace;


@Rpc
@Service(value = "mapWarnService")
public class MapWarnServiceSpringImpl implements  MapWarnService{
	
	public static final Logger LOGGER = LoggerFactory.getLogger("RISKCONTROLLOG");
	
	@Autowired
	JdbcTemplate slaveJdbcTemplate;
	
	@Autowired
	DatabaseDao databaseDao;
	@Autowired
	MapService mapService;
	
	// 台风基本信息，台风的状态是未结束的
	private static final String sql_TFBH ="select * from WZ_TFBH where substr(ISUSE,1,1) ='1'";
	//查询历史台风信息
	private static final String sql_TFLSLJ ="select *  from WZ_TFLSLJ where substr(ISUSE,1,1) ='1' and TFBH=";
	// 更新信息
	private static final String update_TFLSLJ ="update WZ_TFLSLJ set ISUSE=substr(ISUSE,0,length(ISUSE)-1)||'0' where  substr(ISUSE,1,1) ='1'";
	private static final String update_TFBH ="update WZ_TFBH set ISUSE=substr(ISUSE,0,length(ISUSE)-1)||'0' where  substr(ISUSE,1,1) ='1'";
	private static final String update_TFYJLJ ="update WZ_TFBH set ISUSE=substr(ISUSE,0,length(ISUSE)-1)||'0' where  substr(ISUSE,1,1) ='1'";
	//查询预警台风信息
	private static final String sql_TFYBLJ ="select *  from WZ_TFYBLJ where substr(ISUSE,1,1) ='1' and TFBH= ";
//		and YBSJ = to_date(?,'yyyy-mm-dd hh24:mi:ss')
	 // 面数据集名称
	private static final String dataSetUnioin_7 = "TFUNION_7M",dataSetUnion_10="TFUNION_10M",dataSet_new="TF_New";
	private static final String dataSetYJUnioin_7="TFYJUNION_7M",dataSetYJUnioin_10="TFYJUNION_10M",dataSetYJ_new="TFYJ_New";
	private static final String radiusName_7="radius7",radiusName_10="radius10";
	// 点数据集名称
	private static final String riskmap_address_point="RISKMAP_ADDRESS_POINT";
	
	//查询该台风标的表中是否有该信息
//	private static final String query_CORPORELS_count ="select count(*) from WZ_CORPORELS where TFBH=? and MID=?";
	
	/**
	 * @功能：iobjectjava 
	 * @param void
	 * @return AjaxResult
	 * @author liqiankun
	 * @时间：20190515
	 * @修改记录：
	 */	
	public void  createRegionDataSet(){
		try {
			/**查询台风编号基本信息*/
			List<WZ_tfbh>  wzTfbhList =this.slaveJdbcTemplate.query(sql_TFBH, new Object[]{}, new BeanPropertyRowMapper<WZ_tfbh>(WZ_tfbh.class));
			// 若有信息则进行更新
			if(wzTfbhList!=null&&wzTfbhList.size()>0){
				for (WZ_tfbh wz_tfbh:wzTfbhList){
					// 台风编号
					String tfbh  = wz_tfbh.getTfbh();
					String sql = sql_TFLSLJ+ tfbh + " order by RQSJ asc ";
//					查询历史台风信息
					List<WzTFLsljVo>  wzTFLsljList =this.slaveJdbcTemplate.query(sql, new Object[]{}, new BeanPropertyRowMapper<WzTFLsljVo>(WzTFLsljVo.class));
					if(wzTFLsljList!=null&&wzTFLsljList.size()>0){
						// 取最后一条新增的历史数据，来求响应的的预警台风数据
						WzTFLsljVo wzTFLslj = wzTFLsljList.get(wzTFLsljList.size()-1);
						// 创建历史台风面数据
						Workspace workspace = new Workspace();
						// 定义数据源连接信息，假设以下所有数据源设置都存在
					    DatasourceConnectionInfo datasourceconnection = new  DatasourceConnectionInfo();
						//进行数据源的连接
						Datasource datasource =MapUtils.connectDataSource(workspace,datasourceconnection);
						// 组织省的map数据
						Map<String,String> mapProCenterPoint = getMapProCenterPoint(datasource);
						
						this.createOrUpdateRegionDataSetLs(wzTFLsljList,workspace,datasourceconnection,datasource,mapProCenterPoint);
						
						List<WzTFYbljVo>  wzTFYbljList = new ArrayList<WzTFYbljVo>();
						//预警台风数据处理
						if(wzTFLslj.getRqsj()!=null){
//							String sql_YJ = sql_TFYBLJ + tfbh + "and YBSJ = to_date('"+wzTFLslj.getRqsj() +"','yyyy-mm-dd hh24:mi:ss') and TM='中国'";
							String sql_YJ = sql_TFYBLJ + tfbh + " and YBSJ = to_date('2018-09-16 23:00:00','yyyy-mm-dd hh24:mi:ss') and TM='中国'";
							System.out.println("========success");
							wzTFYbljList=this.slaveJdbcTemplate.query(sql_YJ, new Object[]{}, new BeanPropertyRowMapper<WzTFYbljVo>(WzTFYbljVo.class));
							if(wzTFYbljList!=null&&wzTFYbljList.size()>0){
								this.createOrUpdateRegionDataSetYJ(wzTFYbljList,workspace,datasourceconnection,datasource,mapProCenterPoint);
							}
						}
						// 关闭地图资源
					    MapUtils.closeMapResource(null,null,null,null,null,
					    		null,datasource,datasourceconnection,workspace);
						
					}
					
				}
				// 执行完之后，修改表中标志位
//				this.slaveJdbcTemplate.update(update_TFLSLJ);
//				this.slaveJdbcTemplate.update(update_TFYJLJ);
//				this.slaveJdbcTemplate.update(update_TFBH);
				
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/**
	 * @功能：iobjectjava 对预警台风数据进行更新或增加,并整理面数据和提取标的信息
	 * @param void
	 * @return AjaxResult
	 * @author liqiankun
	 * @时间：20190515
	 * @修改记录：
	 */
	public void  createOrUpdateRegionDataSetYJ(List<WzTFYbljVo>  wzTFYbljList,Workspace workspace,
			DatasourceConnectionInfo datasourceconnection,Datasource datasource,Map<String,String> mapProCenterPoint){
//		Workspace workspace = new Workspace();
//		// 定义数据源连接信息，假设以下所有数据源设置都存在
//	    DatasourceConnectionInfo datasourceconnection = new  DatasourceConnectionInfo();
//		//进行数据源的连接
//		Datasource datasource =MapUtils.connectDataSource(workspace,datasourceconnection);
		/**数据源不能为空*/
		if(datasource!=null){
			// 创建预警的面数据集
			WzTFYbljVo wzTFYblj =new WzTFYbljVo();
			DatasetVector datasetVectorYJ_7 = MapUtils.createDataSet(dataSetYJUnioin_7,datasource,wzTFYblj);
			DatasetVector datasetVectorYJ_10 = MapUtils.createDataSet(dataSetYJUnioin_10,datasource,wzTFYblj);
			// 新建一个面数据集，用于存储新的整合的面数据信息
			DatasetVector datasetVector_new = MapUtils.createDataSet(dataSetYJ_new,datasource,wzTFYblj);
			
			/*通过面数据提取标的信息*/
			this.getCorporeYJInfo(datasource,datasetVectorYJ_7,datasetVectorYJ_10,datasetVector_new,wzTFYbljList,radiusName_7,radiusName_10,mapProCenterPoint);
			
			/*给面数据集增加融合数据，七级面数据*/
			Recordset recordsetYJ_7= MapUtils.addUnionDataToDatasetVector(datasetVectorYJ_7,wzTFYbljList,radiusName_7,"YJ");
			/*给面数据集增加融合数据，十级面数据*/
			Recordset recordsetYJ_10= MapUtils.addUnionDataToDatasetVector(datasetVectorYJ_10,wzTFYbljList,radiusName_10,"YJ");
			
			// 关闭地图资源
		    MapUtils.closeMapResource(recordsetYJ_10,recordsetYJ_7,null,datasetVector_new,datasetVectorYJ_10,datasetVectorYJ_7,
		    		null,null,null);
			
		}
	}
	/**
	 * @功能：iobjectjava 对台风数据进行更新或增加,并整理面数据和提取标的信息
	 * @param void
	 * @return AjaxResult
	 * @author liqiankun
	 * @时间：20190515
	 * @修改记录：
	 */
	public void createOrUpdateRegionDataSetLs(List<WzTFLsljVo>  wzTFLsljList,Workspace workspace,
			DatasourceConnectionInfo datasourceconnection,Datasource datasource,Map<String,String> mapProCenterPoint){
//		Workspace workspace = new Workspace();
//		// 定义数据源连接信息，假设以下所有数据源设置都存在
//	    DatasourceConnectionInfo datasourceconnection = new  DatasourceConnectionInfo();
//		//进行数据源的连接
//		Datasource datasource =MapUtils.connectDataSource(workspace,datasourceconnection);
		/**数据源不能为空*/
		if(datasource!=null){
			// 创建预警和历史的面数据集
			WzTFLsljVo wzTFLslj =new WzTFLsljVo();
			DatasetVector datasetVector_7 = MapUtils.createDataSet(dataSetUnioin_7,datasource,wzTFLslj);
			DatasetVector datasetVector_10 = MapUtils.createDataSet(dataSetUnion_10,datasource,wzTFLslj);
			// 新建一个面数据集，用于存储新的整合的面数据信息
			DatasetVector datasetVector_new = MapUtils.createDataSet(dataSet_new,datasource,wzTFLslj);
			
			/*通过面数据提取标的信息*/
			this.getCorporeLsInfo(datasource,datasetVector_7,datasetVector_10,datasetVector_new,wzTFLsljList,radiusName_7,radiusName_10,mapProCenterPoint);
			
			/*给面数据集增加融合数据，七级面数据*/
			Recordset recordset_7= MapUtils.addUnionDataToDatasetVector(datasetVector_7,wzTFLsljList,radiusName_7,"LS");
			/*给面数据集增加融合数据，十级面数据*/
			Recordset recordset_10= MapUtils.addUnionDataToDatasetVector(datasetVector_10,wzTFLsljList,radiusName_10,"LS");
			
			// 关闭资源
		    MapUtils.closeMapResource(recordset_10,recordset_7,null,datasetVector_new,datasetVector_10,datasetVector_7,
		    		null,null,null);
		}
		
				
	}
	
	/**
	 * @功能：iobjectjava 提取标的信息向标的表中
	 *  将新增七级和十级台风面数据进行融合，然后和标的数据进行相交 ，然后在将融合的数据和面进行相交，
	 *	 将相交得出的不同的省或者市的数据和求出的标的数据相交 求出 不同的省的标的信息，插入库中的时候判断是否有该点的信息，没有的时候插入
	 * @param RiskMapInsuredModify
	 * @return AjaxResult
	 * @author liqiankun
	 * @时间：20190515
	 * @修改记录：
	 */
	@SuppressWarnings({ "unused", "unchecked" })
	public  void  getCorporeLsInfo(Datasource datasource,DatasetVector datasetVector_7,DatasetVector datasetVector_10,
			 DatasetVector datasetVector_new,Object objectList,String radiusName_7,String radiusName_10,
			 Map<String,String> mapProCenterPoint){
		try {
			/*转换成list集合*/
			List<Object> list = (List)objectList;
			/*获取编号信息*/
			String tfbh = MapUtils.getMethod(list.get(0),"tfbh").toString();
			String filter = "TFBH ="+tfbh;
			Map<String , Object> map = new HashMap<String, Object>();
			map.put("tfbh", tfbh);
			// 点矢量集
			DatasetVector datasetVector_address = (DatasetVector)datasource.getDatasets().get("RISKMAP_ADDRESS");
			// 该台风编号的记录集
			Recordset recordset_7 = datasetVector_7.query(filter,CursorType.DYNAMIC );
			Recordset recordset_10 = datasetVector_10.query(filter,CursorType.DYNAMIC );
			// 新增的数据集
			Recordset recordset_new = datasetVector_new.getRecordset(false, CursorType.DYNAMIC);
			
			List<Geometry> geometryList= new ArrayList<Geometry>();
			// 通过半径计算
			List<Geometry> geoList_7= MapUtils.getGeometryList(list,radiusName_7,"");
			List<Geometry> geoList_10=MapUtils.getGeometryList(list,radiusName_10,"");
			
			geometryList.addAll(geoList_7);
			geometryList.addAll(geoList_10);
			
			// 将新增的数据集进行整合成一个面数据
			MapUtils.unionGeometryList(geometryList,recordset_new,map);
			
			// 获取需要插入标的表中的省市县的信息集合
			if(datasetVector_new.getRecordCount()>0){
				// 获取省的面数据信息
				DatasetVector datasetVector_p = (DatasetVector)datasource.getDatasets().get("china_province");
				// 获取市的面数据信息
				DatasetVector datasetVector_c = (DatasetVector)datasource.getDatasets().get("china_city");
				List<WzCorporeLs> wzCorporeLsList = new ArrayList<WzCorporeLs>();
				// 查询出所有的需要新增的省市的标的信息
				long startDate = System.currentTimeMillis();
				// 创建新的点数据集
				RiskMapAddressField riskMapAddressField =new RiskMapAddressField();
				DatasetVector riskmap_address_new = MapUtils.createDataSet(riskmap_address_point,datasource,riskMapAddressField);
				// 获取整理各个省/市的标的信息 集合
//				List<WzCorporeLs> wzCorporeLsLis_p =  getWzCorporeLsList(riskmap_address_new,datasetVector_address,datasetVector_p,datasetVector_new,tfbh,"1","WZ_CORPORELS");
				long amongDate = System.currentTimeMillis();
				System.out.println("========第一次时间："+(amongDate-startDate));
				Map<String, ArrayList<WzCorporeLs>>  mapLs=  getWzCorporeLsList(riskmap_address_new,datasetVector_address,datasetVector_c,datasetVector_new,tfbh,"2","WZ_CORPORELS",mapProCenterPoint);
				long endDate = System.currentTimeMillis();
				System.out.println("========第二次时间："+(endDate-startDate));
				// 获取台风七级风圈的标的信息 WZ_CORPORELS_FQ
				MapUtils.unionGeometryList(geoList_7,recordset_new,map);
				ArrayList<WzCorporeLs> wzCorporeLsFqList = getWzCorporeLsFqList(riskmap_address_new,datasetVector_address,datasetVector_new,tfbh,"3");
				long endDateFq = System.currentTimeMillis();
				System.out.println("========第三次时间："+(endDateFq-startDate));
				mapLs.put("corporeLSF", wzCorporeLsFqList);
				List<WzCorporeLs> wzCorporeLsLis_c = mapLs.get("corporeLSC");
//				wzCorporeLsList.addAll(wzCorporeLsLis_p);
				wzCorporeLsList.addAll(wzCorporeLsLis_c);
				// 生成标的组织信息mapService.
				this.sumAmount(mapLs);
				
				// 插入风圈历史标的的sql
				String sql_insert ="insert into WZ_CORPORELS(TFBH,MID,CITYCODE,ADDRESSNAME,VALIDSTATUS,CENTERX,CENTERY,POINTX_2000,POINTY_2000,POINTX_02,POINTY_02,CITYNAME,PRONAME) values (?,?,?,?,?,?,?,?,?,?,?,?,?)";
				// 插入风圈标的的sql
				String sql_insert_fq ="insert into WZ_CORPORELS_FQ(TFBH,MID,CITYCODE,ADDRESSNAME,VALIDSTATUS,CENTERX,CENTERY,POINTX_2000,POINTY_2000,POINTX_02,POINTY_02) values (?,?,?,?,?,?,?,?,?,?,?)";
				
				String sql_delete = "delete from WZ_CORPORELS_FQ where TFBH= '"+tfbh+"'";
				List<Object[]> insertList = new ArrayList<>();
				
				if( wzCorporeLsList!=null&& wzCorporeLsList.size()>0){
					for (WzCorporeLs wzCorporeLs : wzCorporeLsList) {
			            insertList.add(new Object[]{
			            		wzCorporeLs.getId().getTfbh(), wzCorporeLs.getId().getMid(), wzCorporeLs.getCityCode(),
			            		wzCorporeLs.getAddressName(), wzCorporeLs.getValidStatus(),wzCorporeLs.getCenterX(),
			            		wzCorporeLs.getCenterY(),wzCorporeLs.getPointx_2000(),wzCorporeLs.getPointy_2000(),
			            		wzCorporeLs.getPointx_02(),wzCorporeLs.getPointy_02(),wzCorporeLs.getCityName(),wzCorporeLs.getProName()});
			        }
					// 执行批量插入标的数据
					this.slaveJdbcTemplate.batchUpdate(sql_insert,insertList);
				}
				// 从列表中移除所有元素
				insertList.clear();
				if( wzCorporeLsFqList!=null&& wzCorporeLsFqList.size()>0){
					for (WzCorporeLs wzCorporeLsFq : wzCorporeLsFqList) {
			            insertList.add(new Object[]{
			            		wzCorporeLsFq.getId().getTfbh(), wzCorporeLsFq.getId().getMid(), wzCorporeLsFq.getCityCode(),
			            		wzCorporeLsFq.getAddressName(), wzCorporeLsFq.getValidStatus(),
			            		wzCorporeLsFq.getCenterX(), wzCorporeLsFq.getCenterY(),wzCorporeLsFq.getPointx_2000(),wzCorporeLsFq.getPointy_2000(),
			            		wzCorporeLsFq.getPointx_02(),wzCorporeLsFq.getPointy_02()});
			        }
					//先删除我们的台风编号对应的风圈标的信息
					this.slaveJdbcTemplate.update(sql_delete);
					// 执行批量插入标的数据
					this.slaveJdbcTemplate.batchUpdate(sql_insert_fq,insertList);
				}
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("=====获取预警标的信息====="+datasetVector_new.getRecordCount());
		
	}
	/**
	 * @功能：iobjectjava 提取标的信息向标的表中， 预警信息
	 *  将新增七级和十级台风面数据进行融合，然后和标的数据进行相交 ，然后在将融合的数据和面进行相交，
	 *	 将相交得出的不同的省或者市的数据和求出的标的数据相交 求出 不同的省的标的信息，插入库中的时候判断是否有该点的信息，没有的时候插入
	 * @param void
	 * @return AjaxResult
	 * @author liqiankun
	 * @时间：20190515
	 * @修改记录：
	 */
	@SuppressWarnings({ "unused", "unchecked" })
	public  void  getCorporeYJInfo(Datasource datasource,DatasetVector datasetVectorYJ_7,DatasetVector datasetVectorYJ_10,
			 DatasetVector datasetVector_new,Object objectList,String radiusName_7,String radiusName_10,
			 Map<String,String> mapProCenterPoint){
		try {
			/*转换成list集合*/
			List<Object> list = (List)objectList;
			// 点矢量集
			DatasetVector datasetVector_address = (DatasetVector)datasource.getDatasets().get("RISKMAP_ADDRESS");
			// 获取省的面数据信息
			DatasetVector datasetVector_p = (DatasetVector)datasource.getDatasets().get("china_province");
			// 获取市的面数据信息
			DatasetVector datasetVector_c = (DatasetVector)datasource.getDatasets().get("china_city");
			
			// 新增的数据集
			Recordset recordset_new = datasetVector_new.getRecordset(false, CursorType.DYNAMIC);
			/*进行map数据组织*/
			Map<String,List<Object>> mapObject = MapUtils.getObjectjMap(list);
			for(Map.Entry<String, List<Object>>  mList : mapObject.entrySet()){
				List<Object> objList= mList.getValue();
				if(objList!=null&&objList.size()>0){
					/*获取编号信息*/
					String tfbh = MapUtils.getMethod(list.get(0),"tfbh").toString();
					String filter = "TFBH ="+tfbh+" and TM='"+mList.getKey()+"'";
					Map<String , Object> map = new HashMap<String, Object>();
					map.put("tfbh", tfbh);
					map.put("TM", mList.getKey());
					// 该台风编号的记录集
					Recordset recordset_7 = datasetVectorYJ_7.query(filter,CursorType.DYNAMIC );
					Recordset recordset_10 = datasetVectorYJ_10.query(filter,CursorType.DYNAMIC );
					
					List<Geometry> geometryList= new ArrayList<Geometry>();
					// 通过半径计算
					List<Geometry> geoList_7= MapUtils.getGeometryList(objList,radiusName_7,"");
					List<Geometry> geoList_10=MapUtils.getGeometryList(objList,radiusName_10,"");
					geometryList.addAll(geoList_7);
					geometryList.addAll(geoList_10);
					// 将新增的数据集进行整合成一个面数据
					MapUtils.unionGeometryList(geometryList,recordset_new,map);
					// 获取需要插入标的表中的省市县的信息集合
					if(datasetVector_new.getRecordCount()>0){
						// 历史标的的字段和预警标的的字段一致，现在使用历史标的存储预警标的的数据
						List<WzCorporeLs> wzCorporeLsList = new ArrayList<WzCorporeLs>();
						// 查询出所有的需要新增的省市的标的信息
						long startDate = System.currentTimeMillis();
						// 创建新的点数据集
						RiskMapAddressField riskMapAddressField =new RiskMapAddressField();
						DatasetVector riskmap_address_new = MapUtils.createDataSet(riskmap_address_point,datasource,riskMapAddressField);
						// 获取整理各个省/市的标的信息 集合
//						List<WzCorporeLs> wzCorporeLsLis_p =  getWzCorporeLsList(riskmap_address_new,datasetVector_address,datasetVector_p,datasetVector_new,tfbh,"1","WZ_CORPOREYJ");
						long amongDate = System.currentTimeMillis();
						System.out.println("========第一次时间："+(amongDate-startDate));
						Map<String, ArrayList<WzCorporeLs>>  mapLs =  getWzCorporeLsList(riskmap_address_new,datasetVector_address,datasetVector_c,datasetVector_new,tfbh,"2","WZ_CORPOREYJ",mapProCenterPoint);
						long endDate = System.currentTimeMillis();
						System.out.println("========第二次时间："+(endDate-startDate));
						// 获取台风七级风圈的标的信息 WZ_CORPORELS_FQ
						MapUtils.unionGeometryList(geoList_7,recordset_new,map);
						ArrayList<WzCorporeLs> wzCorporeLsFqList = getWzCorporeLsFqList(riskmap_address_new,datasetVector_address,datasetVector_new,tfbh,"3");
						long endDateFq = System.currentTimeMillis();
						System.out.println("========第三次时间："+(endDateFq-startDate));
						List<WzCorporeLs> wzCorporeLsLis_c = mapLs.get("corporeYJC");
						mapLs.put("corporeYJF", wzCorporeLsFqList);
						
//						wzCorporeLsList.addAll(wzCorporeLsLis_p);
						wzCorporeLsList.addAll(wzCorporeLsLis_c);
						// mapService.
						this.sumAmount(mapLs);
						
						// 插入风圈历史标的的sql
						String sql_insert ="insert into WZ_CORPOREYJ(TFBH,MID,CITYCODE,ADDRESSNAME,VALIDSTATUS,CENTERX,CENTERY,POINTX_2000,POINTY_2000,POINTX_02,POINTY_02,CITYNAME,PRONAME) values(?,?,?,?,?,?,?,?,?,?,?,?,?)";
						// 插入风圈标的的sql
						String sql_insert_fq ="insert into WZ_CORPOREYJ_FQ(TFBH,MID,CITYCODE,ADDRESSNAME,VALIDSTATUS,CENTERX,CENTERY,POINTX_2000,POINTY_2000,POINTX_02,POINTY_02) values(?,?,?,?,?,?,?,?,?,?,?)";
						
						String sql_delete = "delete from WZ_CORPOREYJ_FQ where TFBH= '"+tfbh+"'";
						
						List<Object[]> insertList = new ArrayList<>();
						
						if( wzCorporeLsList!=null&& wzCorporeLsList.size()>0){
							for (WzCorporeLs wzCorporeLs : wzCorporeLsList) {
					            insertList.add(new Object[]{
					            		wzCorporeLs.getId().getTfbh(), wzCorporeLs.getId().getMid(), wzCorporeLs.getCityCode(),
					            		wzCorporeLs.getAddressName(), wzCorporeLs.getValidStatus(),wzCorporeLs.getCenterX(),
					            		wzCorporeLs.getCenterY(),wzCorporeLs.getPointx_2000(),wzCorporeLs.getPointy_2000(),
					            		wzCorporeLs.getPointx_02(),wzCorporeLs.getPointy_02(),wzCorporeLs.getCityName(),wzCorporeLs.getProName()});
					        }
							// 执行批量插入标的数据
							this.slaveJdbcTemplate.batchUpdate(sql_insert,insertList);
						}
						// 从列表中移除所有元素
						insertList.clear();
						if( wzCorporeLsFqList!=null&& wzCorporeLsFqList.size()>0){
							for (WzCorporeLs wzCorporeLsFq : wzCorporeLsFqList) {
					            insertList.add(new Object[]{
					            		wzCorporeLsFq.getId().getTfbh(), wzCorporeLsFq.getId().getMid(), wzCorporeLsFq.getCityCode(),
					            		wzCorporeLsFq.getAddressName(), wzCorporeLsFq.getValidStatus(),
					            		wzCorporeLsFq.getCenterX(), wzCorporeLsFq.getCenterY(),wzCorporeLsFq.getPointx_2000(),wzCorporeLsFq.getPointy_2000(),
					            		wzCorporeLsFq.getPointx_02(),wzCorporeLsFq.getPointy_02()});
					        }
							//先删除我们的台风编号对应的风圈标的信息
							this.slaveJdbcTemplate.update(sql_delete);
							// 执行批量插入标的数据
							this.slaveJdbcTemplate.batchUpdate(sql_insert_fq,insertList);
						}
						
					}
				}
				
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("=====获取预警标的信息====="+datasetVector_new.getRecordCount());
	}
	
	/**
	 * @功能：提取七级风圈的台风影响的标的信息
	 * @param 新建点数据集 用于存储新增点、点数据集、新增的面数据信息、台风编号、省市标志位
	 * @return 
	 * @author liqiankun
	 * @时间：20190520
	 * @修改记录：
	 */
	public ArrayList<WzCorporeLs>  getWzCorporeLsFqList(DatasetVector riskmap_address_new,DatasetVector datasetVector_address,
			DatasetVector datasetVector_new,String  tfbh,String proCityFlag){
		ArrayList<WzCorporeLs> wzCorporeLsFqList = new ArrayList<WzCorporeLs>();
		
		QueryParameter parameter_p = new QueryParameter();
		parameter_p.setSpatialQueryObject(datasetVector_new);
		parameter_p.setAttributeFilter("VALIDSTATUS=1 and SMID<10000");
		parameter_p.setSpatialQueryMode(SpatialQueryMode.INTERSECT);
		Recordset queryRecordset_address = datasetVector_address.query(parameter_p);
		System.out.println("======查出的相交的标的的个数======"+proCityFlag+":"+queryRecordset_address.getRecordCount());
		/*对七级台风影响的标的进行处理*/
		int j=0;
		while (!queryRecordset_address.isEOF()){
			j++;
			// 应该循环添加
//			Integer  smid = Integer.parseInt(queryRecordset_address.getFieldValue("SmID")+"");
			Integer  smid = Integer.parseInt(queryRecordset_address.getFieldValue("ADDRESSID")+"");
			String addressName = queryRecordset_address.getFieldValue("ADDRESSNAME")+"";
			BigDecimal pointx_2000 = new BigDecimal(queryRecordset_address.getString("POINTX_2000"));
			BigDecimal pointy_2000 = new BigDecimal(queryRecordset_address.getString("POINTY_2000"));
			BigDecimal pointx_02 = new BigDecimal(queryRecordset_address.getString("POINTX_02"));
			BigDecimal pointy_02 = new BigDecimal(queryRecordset_address.getString("POINTY_02"));
			
			WzCorporeLs  wzCorporeLsFq =new WzCorporeLs();
			WzCorporeLsId id= new WzCorporeLsId();
			id.setTfbh(tfbh);
			id.setMid(smid);
			wzCorporeLsFq.setId(id);
//			wzCorporeLsFq.setProcityFlag(proCityFlag);
			wzCorporeLsFq.setPointx_2000(pointx_2000);
			wzCorporeLsFq.setPointy_2000(pointy_2000);
			wzCorporeLsFq.setPointx_02(pointx_02);
			wzCorporeLsFq.setPointy_02(pointy_02);
			wzCorporeLsFq.setAddressName(addressName);
			// 状态标志位为2 ，则是未生成标的保额信息
			wzCorporeLsFq.setValidStatus("2");
			wzCorporeLsFqList.add(wzCorporeLsFq);
			
			System.out.println(tfbh+":"+proCityFlag+"=================="+j);
			queryRecordset_address.moveNext();
		}
		
		return wzCorporeLsFqList;
	}
	/**
	 * @功能：查询表中标的的数量
	 * @param 
	 * @return 
	 * @author liqiankun
	 * @时间：20190520
	 * @修改记录：
	 */
	public long getCorporeTotalCount(String tfbh ,Integer mid,String proCityFlag,String tableName){
		Connection conn = null;
		PreparedStatement stat = null;
		ResultSet rs = null;
		long totalCount = 0L;
		String sql =" select count(*) from "+tableName+" where TFBH='"+tfbh+"' and MID="+mid;
		try {
			conn = slaveJdbcTemplate.getDataSource().getConnection();
			stat = conn.prepareStatement(sql);
//			stat = conn.prepareStatement(query_CORPORELS_count);
//			stat.setString(1, tfbh);
//			stat.setInt(2, mid);
//			stat.setString(3, proCityFlag);
			rs = stat.executeQuery();
			for(;rs.next();) {
				totalCount = rs.getLong(1);
			}
		} catch (SQLException e) {
			LOGGER.info("查询异常：" + e.getMessage() ,e);
			e.printStackTrace();
			throw new RuntimeException("查询异常:"+e);
		}finally {
			releaseResources(stat, conn, rs);
		}
		
		return totalCount;
	}
	
	/**
	 * @功能：iobjectjava 整理各个省/市的标的信息 集合
	 * @param 新建点数据集 用于存储新增点,点数据集、省/市的面数据信息、新增的面数据信息、台风编号、省市标志位
	 * @return List<WzCorporeLs>
	 * @author liqiankun
	 * @时间：20190520
	 * @修改记录：
	 */
	public  Map<String, ArrayList<WzCorporeLs>>  getWzCorporeLsList(DatasetVector riskmap_address_new,DatasetVector datasetVector_address,DatasetVector datasetVector,
			DatasetVector datasetVector_new,String  tfbh,String proCityFlag,String tableName,Map<String,String> mapProCenterPoint){
		
		Map<String, ArrayList<WzCorporeLs>> map = new HashMap<String, ArrayList<WzCorporeLs>>();
		
		ArrayList<WzCorporeLs> wzCorporeLsList = new ArrayList<WzCorporeLs>();
		ArrayList<WzCorporeLs> wzCorporeLsPList = new ArrayList<WzCorporeLs>();
		
		QueryParameter parameter_p = new QueryParameter();
		parameter_p.setSpatialQueryObject(datasetVector_new);
		parameter_p.setSpatialQueryMode(SpatialQueryMode.INTERSECT);
		
		//查询省市面信息和新增的面数据相交的信息
		Recordset queryRecordset = datasetVector.query(parameter_p);
		System.out.println(tfbh+"======查出的省份的个数======"+proCityFlag+":"+queryRecordset.getRecordCount());
		
		//通过点数据集和新增面数据集的交集的有效的点的个数
		parameter_p.setAttributeFilter("VALIDSTATUS=1  and SMID<10000");
		Recordset queryRecordset_address_intersect = datasetVector_address.query(parameter_p);
		//清除矢量数据集中的所有记录,清空成功之后，添加新的查询出来的记录
		boolean riskmapFlag =riskmap_address_new.truncate();
		boolean appendFlag  =false;
		if(riskmapFlag){
			appendFlag = riskmap_address_new.append(queryRecordset_address_intersect);
		}
		System.out.println("======查出的相交的标的的个数======"+proCityFlag+":"+queryRecordset_address_intersect.getRecordCount());
		// 省份或者市进行循环
		while (!queryRecordset.isEOF()){
			String adminCode ="",proName="";
			if(proCityFlag.equals("1")){
				adminCode = queryRecordset.getString("AdminCode");
			}else if(proCityFlag.equals("2")){
				adminCode = queryRecordset.getString("CityAdCode");
				proName = queryRecordset.getString("PRONAME");
			}
			
			// 中心点
			BigDecimal centerX = new BigDecimal(queryRecordset.getString("centerX"));
			BigDecimal centerY = new BigDecimal(queryRecordset.getString("centerY"));
			// 提取省市的中心点
			String adminCodeP = adminCode.substring(0, 2)+"0000";
			String  mapProCenterPointValue = mapProCenterPoint.get(adminCodeP);
			String [] mapProCenterArray = mapProCenterPointValue.split(",");
			
			String  cityName = queryRecordset.getString("Name");
			// 单个省或者市的面矢量数据集
//			DatasetVector datasetVectorP_single = queryRecordset.getDataset();
			GeoRegion geoRegion=(GeoRegion)queryRecordset.getGeometry();
			
//			DatasetVector datasetVector_address_intersect = queryRecordset_address_intersect.getDataset();
			
			QueryParameter parameterP_address = new QueryParameter();
			parameterP_address.setSpatialQueryObject(geoRegion);
			parameterP_address.setAttributeFilter("VALIDSTATUS=1");
			parameterP_address.setSpatialQueryMode(SpatialQueryMode.INTERSECT);
			//查询出各省或者市的标的信息     queryRecordset_address_intersect
//			Recordset queryRecordset_address = datasetVector_address.query(parameterP_address);
			
			Recordset queryRecordset_address = riskmap_address_new.query(parameterP_address);
			
			System.out.println(adminCode+"=====省的标的个数======="+queryRecordset_address.getRecordCount());
			// 对标的点进行循环
			int j=0;
			while (!queryRecordset_address.isEOF()){
				j++;
				// 应该循环添加
//				Integer  smid = Integer.parseInt(queryRecordset_address.getFieldValue("SmID")+"");
				Integer  smid = Integer.parseInt(queryRecordset_address.getFieldValue("ADDRESSID")+"");
				String addressName = queryRecordset_address.getFieldValue("ADDRESSNAME")+"";
				BigDecimal pointx_2000 = new BigDecimal(queryRecordset_address.getString("POINTX_2000"));
				BigDecimal pointy_2000 = new BigDecimal(queryRecordset_address.getString("POINTY_2000"));
				BigDecimal pointx_02 = new BigDecimal(queryRecordset_address.getString("POINTX_02"));
				BigDecimal pointy_02 = new BigDecimal(queryRecordset_address.getString("POINTY_02"));
				
//				String validStatus = queryRecordset_address.getFieldValue("VALIDSTATUS")+"";
				// 求标的表中是否存在该信息,若不存在则进行添加
				long totalCount =  getCorporeTotalCount(tfbh ,smid,proCityFlag,tableName);
				if(totalCount==0){
					WzCorporeLs wzCorporeLs =new WzCorporeLs();
					WzCorporeLsId  wzCorporeLsId = new WzCorporeLsId();
					wzCorporeLsId.setTfbh(tfbh);
					wzCorporeLsId.setMid(smid);
					wzCorporeLs.setId(wzCorporeLsId);
					
					wzCorporeLs.setCenterX(centerX);
					wzCorporeLs.setCenterY(centerY);
					wzCorporeLs.setCityCode(adminCode);
					wzCorporeLs.setAddressName(addressName);
					// 状态标志位为2 ，则是未生成标的保额信息
					wzCorporeLs.setValidStatus("2");
					
					wzCorporeLs.setPointx_2000(pointx_2000);
					wzCorporeLs.setPointy_2000(pointy_2000);
					wzCorporeLs.setPointx_02(pointx_02);
					wzCorporeLs.setPointy_02(pointy_02);
					wzCorporeLs.setCityName(cityName);
					wzCorporeLs.setProName(proName);
					
					wzCorporeLsList.add(wzCorporeLs);
					WzCorporeLs wzCorporeLsNew =new WzCorporeLs();
					try {
						BeanUtils.copyProperties(wzCorporeLsNew, wzCorporeLs);
					} catch (IllegalAccessException e) {
						e.printStackTrace();
					} catch (InvocationTargetException e) {
						e.printStackTrace();
					}
					if(mapProCenterArray.length>0){
						BigDecimal  centerPX =  new BigDecimal(mapProCenterArray[0]);
						BigDecimal  centerPY =  new BigDecimal(mapProCenterArray[1]);
						wzCorporeLsNew.setCenterX(centerPX);
						wzCorporeLsNew.setCenterY(centerPY);
						wzCorporeLsNew.setCityCode(adminCodeP);
						wzCorporeLsNew.setCityName(proName);
						wzCorporeLsPList.add(wzCorporeLsNew);
					}
					
				}
				System.out.println(tfbh+":"+proCityFlag+"=================="+j);
				queryRecordset_address.moveNext();
			}
			
			queryRecordset.moveNext();
		}
		if(tableName.indexOf("YJ")!=-1){
			map.put("corporeYJP", wzCorporeLsPList);
			map.put("corporeYJC", wzCorporeLsList);
		}else {
			map.put("corporeLSP", wzCorporeLsPList);
			map.put("corporeLSC", wzCorporeLsList);
		}
		
		
		return map;
	}
	
	/**
	 * @功能：iobjectjava 预警推送信息组织
	 * @param 
	 * @return map
	 * @author liqiankun
	 * @时间：20190523
	 * @修改记录：
	 */
	public Map<String , List<WzCorporeLs>> generateMapByObjectJava(List<WzTFYbljVo> wzTFYbljList,String radius,String comCode){
		 Map<String , List<WzCorporeLs>> map = new HashMap<String, List<WzCorporeLs>>();
		 /*获取编号信息*/
		 if(wzTFYbljList!=null&&wzTFYbljList.size()>0){
			 String tfbh =wzTFYbljList.get(0).getTfbh();
			 Workspace workspace = new Workspace();
			 // 定义数据源连接信息，假设以下所有数据源设置都存在
		     DatasourceConnectionInfo datasourceconnection = new  DatasourceConnectionInfo();
			 //进行数据源的连接
			 Datasource datasource =MapUtils.connectDataSource(workspace,datasourceconnection);
			 
			 if(datasource!=null){
					// 新建一个面数据集，用于存储新的整合的面数据信息
					WzTFYbljVo wzTFYblj =new WzTFYbljVo();
					DatasetVector datasetVector_new = MapUtils.createDataSet(dataSet_new,datasource,wzTFYblj);
					// 点矢量集
					DatasetVector datasetVector_address = (DatasetVector)datasource.getDatasets().get("RISKMAP_ADDRESS");
					// 新增的数据集
					Recordset recordset_new = datasetVector_new.getRecordset(false, CursorType.DYNAMIC);
					
					List<Object> objList = (List)(Object)wzTFYbljList;
					 /*需要整合面数据的集合*/
					List<Geometry> geoList = MapUtils.getGeometryList(objList,radius,"YJ");
					
					// 创建新的点数据集
					RiskMapAddressField riskMapAddressField =new RiskMapAddressField();
					DatasetVector riskmap_address_new = MapUtils.createDataSet(riskmap_address_point,datasource,riskMapAddressField);
					/*查询省市面积的过滤条件*/
					String filter = "";
					String cityCode = comCode.substring(0, 4)+"00";
					String proCode = comCode.substring(0, 2)+"0000";
					Map<String,Object> mapNew =  new HashMap<String, Object>();
					/*对geometry中list数据进行融合,并添加到新的数据集中*/
					MapUtils.unionGeometryList(geoList,recordset_new,mapNew);
					// 获取需要插入标的表中的省市县的信息集合
					if(datasetVector_new.getRecordCount()>0){
						List<WzCorporeLs> wzCorporeLsList = new ArrayList<WzCorporeLs>();
						if("2102,3302,3502,3702,4403".indexOf(comCode.substring(0, 4)) > -1){
							//直接去市的数据，这是计划单联市的面数据
							filter = "CITYADCODE ='"+cityCode+"'";
							DatasetVector datasetVector_p = (DatasetVector)datasource.getDatasets().get("china_city");
							map = getMapWzCorporeLs(riskmap_address_new,datasetVector_address,datasetVector_p,datasetVector_new,tfbh,comCode,filter);
							if(datasetVector_p!=null){
								datasetVector_p.close();
							}
						}else if("21,33,35,37,44".indexOf(comCode.substring(0, 2)) > -1){
							if("21".equals(comCode.substring(0, 2))){
								cityCode = "210200";
							}else if("33".equals(comCode.substring(0, 2))){
								cityCode = "330200";
							}else if("35".equals(comCode.substring(0, 2))){
								cityCode = "350200";
							}else if("37".equals(comCode.substring(0, 2))){
								cityCode = "370200";
							}else if("44".equals(comCode.substring(0, 2))){
								cityCode = "440300";
							}
							// 去除计划单联市的面数据的省数据，查的是市的那个面
							filter = "CITYADCODE !='"+cityCode+"' and PROADCODE='"+proCode+"'";
							//直接查询出省的面数据信息
							DatasetVector datasetVector_p = (DatasetVector)datasource.getDatasets().get("china_city");
							map = getMapWzCorporeLs(riskmap_address_new,datasetVector_address,datasetVector_p,datasetVector_new,tfbh,comCode,filter);
							if(datasetVector_p!=null){
								datasetVector_p.close();
							}
						}else {
							filter = "AdminCode ='"+proCode+"'";
							//直接查询出省的面数据信息
							DatasetVector datasetVector_p = (DatasetVector)datasource.getDatasets().get("china_province");
							map = getMapWzCorporeLs(riskmap_address_new,datasetVector_address,datasetVector_p,datasetVector_new,tfbh,comCode,filter);
							if(datasetVector_p!=null){
								datasetVector_p.close();
							}
						}
						
					}
					// 关闭资源
				    MapUtils.closeMapResource(recordset_new,null,datasetVector_new,null,datasetVector_address,riskmap_address_new,
				    		datasource,datasourceconnection,workspace);
			       
					
			}
			 

		 }
		 		
		 return map;
	}
	/**
	 * @功能：iobjectjava 通过预警的面数据分省整合成map信息
	 * @param RiskMapInsuredModify
	 * @return AjaxResult
	 * @author liqiankun
	 * @时间：20190523
	 * @修改记录：
	 */
	public Map<String ,List<WzCorporeLs>>  getMapWzCorporeLs(DatasetVector riskmap_address_new,DatasetVector datasetVector_address,
			DatasetVector datasetVector,DatasetVector datasetVector_new,String tfbh,String comCode,String filter){
		Map<String,List<WzCorporeLs>>  map = new HashMap<String, List<WzCorporeLs>>();
		
		QueryParameter parameter_p = new QueryParameter();
		parameter_p.setSpatialQueryObject(datasetVector_new);
		parameter_p.setSpatialQueryMode(SpatialQueryMode.INTERSECT);
		parameter_p.setAttributeFilter(filter);
		//查询省市面信息和新增的面数据相交的信息
		Recordset queryRecordset = datasetVector.query(parameter_p);
		System.out.println("======查出的省份的个数======:"+queryRecordset.getRecordCount());
		//通过点数据集和新增面数据集的交集的有效的点的个数
		parameter_p.setAttributeFilter("VALIDSTATUS=1");
		Recordset queryRecordset_address_intersect = datasetVector_address.query(parameter_p);
		//清除矢量数据集中的所有记录,清空成功之后，添加新的查询出来的记录
		boolean riskmapFlag =riskmap_address_new.truncate();
		boolean appendFlag  =false;
		if(riskmapFlag){
			appendFlag = riskmap_address_new.append(queryRecordset_address_intersect);
		}
		System.out.println("======查出的相交的标的的个数======:"+queryRecordset_address_intersect.getRecordCount());
		
		List<WzCorporeLs> wzCorporeLsList=new ArrayList<WzCorporeLs>();
		// 省份或者市进行循环
		while (!queryRecordset.isEOF()){
			String adminCode = "";
			if("2102,3302,3502,3702,4403".indexOf(comCode.substring(0, 4)) > -1){
				adminCode = queryRecordset.getString("CITYADCODE");
			}else if("21,33,35,37,44".indexOf(comCode.substring(0, 2)) > -1) {
				adminCode = queryRecordset.getString("CITYADCODE");
			}else {
				adminCode = queryRecordset.getString("AdminCode");
			}
			String  cityName = queryRecordset.getString("Name");
			
			GeoRegion geoRegion=(GeoRegion)queryRecordset.getGeometry();
			QueryParameter parameterP_address = new QueryParameter();
			parameterP_address.setSpatialQueryObject(geoRegion);
			parameterP_address.setAttributeFilter("VALIDSTATUS=1");
			parameterP_address.setSpatialQueryMode(SpatialQueryMode.INTERSECT);
			
			Recordset queryRecordset_address = riskmap_address_new.query(parameterP_address);
			System.out.println(adminCode+"=====省的标的个数======="+queryRecordset_address.getRecordCount());
			
			while (!queryRecordset_address.isEOF()){
				Integer  smid = Integer.parseInt(queryRecordset_address.getFieldValue("ADDRESSID")+"");
				String addressName = queryRecordset_address.getFieldValue("ADDRESSNAME")+"";
				WzCorporeLs wzCorporeLs =new WzCorporeLs();
				WzCorporeLsId  wzCorporeLsId = new WzCorporeLsId();
				wzCorporeLsId.setTfbh(tfbh);
				wzCorporeLsId.setMid(smid);
				wzCorporeLs.setId(wzCorporeLsId);
				
				wzCorporeLs.setCityCode(adminCode);
				wzCorporeLs.setAddressName(addressName);
				wzCorporeLs.setCityName(cityName);
				wzCorporeLsList.add(wzCorporeLs);
				
				queryRecordset_address.moveNext();
			}
			queryRecordset.moveNext();
		}
		map.put(comCode, wzCorporeLsList);
		System.out.println(comCode+"=====标的总数为======="+wzCorporeLsList.size());
		return map;
	}

	/**
	 * 释放数据库资源，包括数据库连接和PrepareStatement对象*/
	
	private void releaseResources(Statement stat, Connection conn, ResultSet rs) {
		try {
			if(rs!=null){
			rs.close();
			}
		} catch (SQLException e) {
			LOGGER.info("关闭异常：" + e.getMessage() ,e);
			throw new RuntimeException("关闭异常:"+e);
		}
		try {
			if (stat != null) {
				stat.close();
			}
		} catch (SQLException e) {
			LOGGER.info("关闭异常：" + e.getMessage() ,e);
			throw new RuntimeException("关闭异常:"+e);
		}
		try {
			if (conn != null) {
				conn.close();
			}
		} catch (SQLException e) {
			LOGGER.info("关闭异常：" + e.getMessage() ,e);
			throw new RuntimeException("关闭异常:"+e);
		}
	}
	/**
	 * @功能：iobjectjava 获取省级的中心点的经纬度
	 * @param 
	 * @return 
	 * @author liqiankun
	 * @时间：20190523
	 * @修改记录：
	 */
	public Map<String,String>  getMapProCenterPoint(Datasource datasource){
		Map<String,String> map = new HashMap<String, String>();
		// 获取省的面数据信息
		DatasetVector datasetVector_p = (DatasetVector)datasource.getDatasets().get("china_province");
		// 新增的数据集
		Recordset recordset_p = datasetVector_p.getRecordset(false, CursorType.DYNAMIC);
		while (!recordset_p.isEOF()){
			String lonAndLat = "";
			String adminCode = recordset_p.getFieldValue("ADMINCODE")+""; 
			String centerX = recordset_p.getFieldValue("CENTERX")+"";
			String centerY = recordset_p.getFieldValue("CENTERY")+"";
			lonAndLat = centerX+","+centerY;
			map.put(adminCode, lonAndLat);
			
			recordset_p.moveNext();
		}
		
		// 关闭资源
		MapUtils.closeMapResource(recordset_p,null,datasetVector_p,null,null,null,null,null,null);
		
		return map;
	}
	
	/**@功能：统计台风影响省/城市的保额统计
	 * @return AjaxResult
	 * @author 王坤龙
	 * @时间：20190523
	 * @修改记录：*/
	public AjaxResult sumAmount(Map<String,ArrayList<WzCorporeLs>> mapList) {
		AjaxResult ajax = new AjaxResult();
		Connection conn = null;
		PreparedStatement stat = null;
		ResultSet rs = null;
		ArrayList<WzCorporeLs> WzCorporeLsList;
		Iterator<String> iteratorAll = mapList.keySet().iterator();
		String procityFlag = "";
		String tableName = "";
	try {
		while (iteratorAll.hasNext()) {
			String keyFlag = iteratorAll.next();
			WzCorporeLsList = mapList.get(keyFlag);
			Map<String, Timestamp> resultTfdate = new HashMap<String, Timestamp>();
			switch(keyFlag){
			case "corporeLSP":
				tableName = "LS";
				procityFlag = "1";
				break;
			case "corporeLSC":
				tableName = "LS";
				procityFlag = "2";
				break;
			case "corporeLSF":
				tableName = "LS";
				procityFlag = "3";
				break;
			case "corporeYJP":
				tableName = "YJ";
				procityFlag = "1";
				break;
			case "corporeYJC":
				tableName = "YJ";
				procityFlag = "2";
				break;
			case "corporeYJF":
				tableName = "YJ";
				procityFlag = "3";
				break;
			}
			String tfbh;
			String mid;
			String citycode;
			String cityname;
			String centerx;
			String centery;
			Map<Map<String,String>,Wz_Corporels_byMid> resultMap = new HashMap<Map<String,String>,Wz_Corporels_byMid>();
			for(WzCorporeLs wzLs:WzCorporeLsList){
				tfbh = wzLs.getId().getTfbh();
				mid = wzLs.getId().getMid().toString();
				if(procityFlag.equals("3")){
					citycode = "-1";
					cityname = "";
					centerx = "0";
					centery = "0";
				} else{
					citycode = wzLs.getCityCode();
					cityname = wzLs.getCityName();
					centerx = wzLs.getCenterX().toString();
					centery = wzLs.getCenterY().toString();	
				}
				// 按照台风编号查询台风发生时间
				if(!resultTfdate.containsKey(tfbh) && tfbh != null && !("".equals(tfbh))){
					String sqlSearch="select tfdate from wz_tfbh where tfbh = " + tfbh;
					System.out.println(sqlSearch);
					List<Map<String, Object>> resulttemp = null;
					resulttemp = slaveJdbcTemplate.queryForList(sqlSearch);
					if(resulttemp!=null && resulttemp.size()==1){
						resultTfdate.put(tfbh, Timestamp.class.cast(resulttemp.get(0).get("TFDATE")));
					}
				}
				Map<String,String> datakey = new HashMap<String,String>();
				datakey.put(tfbh,citycode);
				if(!resultMap.containsKey(datakey)){
					Wz_Corporels_byMid wcbm = new Wz_Corporels_byMid(new ArrayList(Arrays.asList(mid)),cityname,centerx,centery,procityFlag);
					resultMap.put(datakey, wcbm);
				} else{
					resultMap.get(datakey).getCountList().add(mid);
				}

			}
			System.out.println("++++++++++++++++分割线+++++++++++++++++++++++++++++++++++++++"); // 目前获得台风编号，台风发生时间，台风影响Mid，影响地的citycode
			for(Map<String,String> resultKey:resultMap.keySet()){
				tfbh = "";
				mid = "";
				citycode = "";
				cityname = "";
				centerx = "";
				centery = "";
				procityFlag = "";
				for(String key:resultKey.keySet()){
					tfbh = key;
					citycode = resultKey.get(key);
				}
				Timestamp date = resultTfdate.get(tfbh);
			    SimpleDateFormat dateformatter=new SimpleDateFormat("yyyy-MM-dd");
			    SimpleDateFormat hourformatter=new SimpleDateFormat("HH");
			    String tfdate = dateformatter.format(date);
			    String tftime = hourformatter.format(date);
			    Wz_Corporels_byMid wz = resultMap.get(resultKey);
			    List<String> countList = wz.getCountList();
				cityname = wz.getCityName();
				procityFlag = wz.getProcityFlag();
				centerx = wz.getCenterX();
				centery = wz.getCenterY();
				wz.setCorporeSum(countList.size()+"");
			    BigDecimal[] bigdecimals;
			    if(countList.size()>500){
			    	System.out.println("长度过大情况：");
			    	// 截取list
			    	bigdecimals = new BigDecimal[3];
			    	bigdecimals[0] = BigDecimal.ZERO;
			    	bigdecimals[1] = BigDecimal.ZERO;
			    	bigdecimals[2] = BigDecimal.ZERO;
			    	int max = countList.size()/500;
			    	for(int i=0; i<=max; i++){
			    		System.out.println("进入第" + i + "次计算");
			    		List<String> bigdecimalsTemp;
			    		if(i<max){
			    			bigdecimalsTemp = countList.subList(i*500, (i+1)*500);
			    		} else if(i*500 != countList.size()){
			    			bigdecimalsTemp = countList.subList(i*500, countList.size());
			    		} else{
			    			break;
			    		}
			    		BigDecimal[] bigdecimalTemp = doworkSumAmount(bigdecimalsTemp,tfdate,tftime);
			    		bigdecimals[0] = bigdecimals[0].add(bigdecimalTemp[0]);
			    		bigdecimals[1] = bigdecimals[0].add(bigdecimalTemp[1]);
			    		bigdecimals[2] = bigdecimals[0].add(bigdecimalTemp[2]);
			    		System.out.println("此时 amount = " + bigdecimals[0] + " Q = "  + bigdecimals[1] + " G = "  + bigdecimals[2]);
			    	}

			    }else{
			    	// 直接调用程序
			    	bigdecimals = doworkSumAmount(countList,tfdate,tftime);
			    }
			    wz.setAmount(bigdecimals[0]);
			    wz.setAmountQ(bigdecimals[1]);
			    wz.setAmountG(bigdecimals[2]);
			    System.out.println("最终计算出 台风编号为" + tfbh+ citycode + cityname + " 省/市 = "+ procityFlag + " 影响地点共有" + wz.getCorporeSum() + " amount = " + bigdecimals[0] + " Q = "  + bigdecimals[1] + " G = "  + bigdecimals[2]);
			    // 测试查询
			    QueryRule queryRule = QueryRule.getInstance();

			    WzCorporeLsPId id = new WzCorporeLsPId();
			    id.setTfbh(tfbh);
			    id.setProcityFlag(procityFlag);
			    id.setCityCode(citycode);
			    queryRule.addEqual("id", id);
			    if(tableName.equals("LS")){
			    	List<WzCorporeLsP> resultSelect = new ArrayList<WzCorporeLsP>();
					resultSelect = databaseDao.findAll(WzCorporeLsP.class, queryRule);
				    if(resultSelect == null || resultSelect.size()==0){
				    	// 插入
					    WzCorporeLsP wzLsPIns = new WzCorporeLsP();
					    wzLsPIns.setId(id);
					    wzLsPIns.setCenterX(new BigDecimal(centerx));
					    wzLsPIns.setCenterY(new BigDecimal(centery));
					    wzLsPIns.setCityName(cityname);
					    wzLsPIns.setCorporeSum(countList.size());
					    wzLsPIns.setGAmount(bigdecimals[2]);
					    wzLsPIns.setJAmount(BigDecimal.ZERO);
					    wzLsPIns.setQAmount(bigdecimals[1]);
					    wzLsPIns.setTotalAmount(bigdecimals[0]);
					    databaseDao.save(WzCorporeLsP.class, wzLsPIns);
				    } else{
				    	// 修改，将四个数据与已经存在的数据累加
				    	WzCorporeLsP wzLsPTemp = resultSelect.get(0);
						wzLsPTemp.setTotalAmount(wzLsPTemp.getTotalAmount().add(bigdecimals[0]));
						wzLsPTemp.setQAmount(wzLsPTemp.getQAmount().add(bigdecimals[1]));
						wzLsPTemp.setGAmount(wzLsPTemp.getGAmount().add(bigdecimals[2]));
						wzLsPTemp.setCorporeSum(wzLsPTemp.getCorporeSum()+countList.size());
				    }
			    }else if(tableName.equals("YJ")){
			    	List<WzCorporeYjP> resultSelect = new ArrayList<WzCorporeYjP>();
					resultSelect = databaseDao.findAll(WzCorporeYjP.class, queryRule);
				    if(resultSelect == null || resultSelect.size()==0){
				    	// 插入
				    	WzCorporeYjP wzYjPIns = new WzCorporeYjP();
				    	wzYjPIns.setId(id);
				    	wzYjPIns.setCenterX(new BigDecimal(centerx));
				    	wzYjPIns.setCenterY(new BigDecimal(centery));
				    	wzYjPIns.setCityName(cityname);
				    	wzYjPIns.setCorporeSum(countList.size());
				    	wzYjPIns.setGAmount(bigdecimals[2]);
				    	wzYjPIns.setJAmount(BigDecimal.ZERO);
				    	wzYjPIns.setQAmount(bigdecimals[1]);
				    	wzYjPIns.setTotalAmount(bigdecimals[0]);
					    databaseDao.save(WzCorporeYjP.class, wzYjPIns);
				    } else{
				    	// 修改，将四个数据与已经存在的数据累加
				    	WzCorporeYjP wzYjPTemp = resultSelect.get(0);
				    	wzYjPTemp.setTotalAmount(wzYjPTemp.getTotalAmount().add(bigdecimals[0]));
				    	wzYjPTemp.setQAmount(wzYjPTemp.getQAmount().add(bigdecimals[1]));
				    	wzYjPTemp.setGAmount(wzYjPTemp.getGAmount().add(bigdecimals[2]));
				    	wzYjPTemp.setCorporeSum(wzYjPTemp.getCorporeSum()+countList.size());
				    }
			    }
			}
			ajax.setStatus(1);
			ajax.setStatusText("插入成功");
		}

	} catch (Exception e) {
		ajax.setStatus(0);
		ajax.setStatusText("查询异常：" + e.getMessage());
		LOGGER.info("查询异常：" + e.getMessage(), e);
		e.printStackTrace();
		throw new RuntimeException("查询异常:" + e);
	} finally {
		releaseResources(stat, conn, rs);
	}
	return ajax;
	}
	
	public BigDecimal[] doworkSumAmount(List<String> countList,String tfdate,String tftime){
		BigDecimal[] bigdecimals = new BigDecimal[3];
		bigdecimals[0] = BigDecimal.ZERO;
		bigdecimals[1] = BigDecimal.ZERO;
		bigdecimals[2] = BigDecimal.ZERO;
		if(countList.size() == 0){
			return bigdecimals;
		}
		String list_str = StringUtils.join(countList,"','");
		
		String sql = "select sum(sumamount) as amountQ from riskmap_main where classcode = '01' and addressid in ('" +list_str+ "') and (startdate<to_date('" +tfdate+ "','yyyy-MM-dd HH:mi:ss')"
				+ " or (startdate=to_date('" +tfdate+ "','yyyy-MM-dd HH:mi:ss') and starthour <='" +tftime+ "')) and (enddate>to_date('" +tfdate+ "','yyyy-MM-dd HH:mi:ss')"
				+ " or (enddate=to_date('" +tfdate+ "','yyyy-MM-dd HH:mi:ss') and endhour >='" +tftime+ "')) group by classcode";
		List<Map<String, Object>> resultQ = null;
		resultQ = slaveJdbcTemplate.queryForList(sql);
		if(resultQ!=null && resultQ.size() == 1 && resultQ.get(0)!=null && resultQ.get(0).get("AMOUNTQ")!=null){
			BigDecimal amountQ = new BigDecimal(resultQ.get(0).get("AMOUNTQ").toString()); 
//			amountQ=amountQ.setScale(2, BigDecimal.ROUND_DOWN); //小数位 直接舍去
			bigdecimals[1]=amountQ.setScale(2, BigDecimal.ROUND_HALF_UP); //四舍五入
		}
		
		String sql2 = "select sum(sumamount) as amountG from riskmap_main where classcode = '03' and addressid in ('" +list_str+ "') and (startdate<to_date('" +tfdate+ "','yyyy-MM-dd HH:mi:ss')"
				+ " or (startdate=to_date('" +tfdate+ "','yyyy-MM-dd HH:mi:ss') and starthour <='" +tftime+ "')) and (enddate>to_date('" +tfdate+ "','yyyy-MM-dd HH:mi:ss')"
				+ " or (enddate=to_date('" +tfdate+ "','yyyy-MM-dd HH:mi:ss') and endhour >='" +tftime+ "')) group by classcode";
		List<Map<String, Object>> resultG = null;
		resultG = slaveJdbcTemplate.queryForList(sql2);
		if(resultG!=null && resultG.size() == 1 && resultG.get(0)!=null && resultG.get(0).get("AMOUNTG")!=null){
			BigDecimal amountG = new BigDecimal(resultG.get(0).get("AMOUNTG").toString()); 
	//		amountG=amountG.setScale(2, BigDecimal.ROUND_DOWN); //小数位 直接舍去
			bigdecimals[2]=amountG.setScale(2, BigDecimal.ROUND_HALF_UP); //四舍五入
		}
		bigdecimals[0] = bigdecimals[1].add(bigdecimals[2]).setScale(2, BigDecimal.ROUND_HALF_UP);
		System.out.println("计算中临时 amount = " + bigdecimals[0] + " Q = "  + bigdecimals[1] + " G = "  + bigdecimals[2]);
		return bigdecimals;
		
	}
	
	
			
}
