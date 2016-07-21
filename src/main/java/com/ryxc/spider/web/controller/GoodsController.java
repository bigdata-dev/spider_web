package com.ryxc.spider.web.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.ryxc.spider.web.domain.ChartView;
import com.ryxc.spider.web.domain.LineChartView;
import com.ryxc.spider.web.page.Page;
import com.ryxc.spider.web.domain.Goods;
import com.ryxc.spider.web.utils.HbaseUtils;
import com.ryxc.spider.web.utils.SolrUtil;
import com.ryxc.spider.web.utils.FreeMarkerUtil;

@Controller
@RequestMapping("goods")
public class GoodsController {
	static final Logger logger = LoggerFactory.getLogger(GoodsController.class);
	private static final String GOODS_INDEX = "/goods/goodsList";
	private static final String GOODS_COMPARE = "/goods/goodsCompare";
	private static final String PAGE_VERSION = "/goods/version";
	private static final String GOODS_DETAIL = "/goods/goodsDetail";
	HbaseUtils hbaseUtils = new HbaseUtils();


	@RequestMapping("")
	public String index(ModelMap modelMap){
		List<Goods> goodsList = new ArrayList<Goods>();
		try {
			goodsList = SolrUtil.search("",new Long(0), new Long(5),"");
		} catch (Exception e) {
			logger.error("查询推荐商品出错,{}",e);
			e.printStackTrace();
		}
		modelMap.put("goodsList", goodsList);
		return GOODS_INDEX;
	}
	
	@ResponseBody
	@RequestMapping(value="/search", produces = "application/text; charset=utf-8")
	public String serachGoods(ModelMap modelMap,
			@RequestParam(value="skey",required = false) String skey,
			@RequestParam(value="sort",required = false) String sort,
			HttpServletRequest request,
			@RequestParam(value = "start", defaultValue = "0") Long start,
			@RequestParam(value = "range", defaultValue = "20") Long range){
		List<Goods> goodsList = new ArrayList<Goods>();
		int count = 0;
		try {
			goodsList = SolrUtil.search(skey,start, range,sort);
			count = SolrUtil.getCount(skey);
		} catch (Exception e) {
			logger.error("查询索引错误!{}",e);
			e.printStackTrace();
		}
		
		
		String nextUrl = request.getContextPath() + "/goods"+"/search";
		String pageString = Page.getJsonPage(request, "", start, range, count,nextUrl);
		Map<String, Object> root = new HashMap<String, Object>();
		root.put("pageString", pageString);
		root.put("goodsList", goodsList);
		root.put("count", count);
		root.put("range", range);
		root.put("contentPath", request.getContextPath());
		String result = FreeMarkerUtil.parseTemplate(
				"goods/goodslist.ftl", root);
		modelMap.put("count", count);
		return result;
	}
	/**
	 * 获取推荐商品
	 * 现在只是随便查询5条数据
	 * @return
	 */
	@RequestMapping(value="/getTuiJian", produces = "application/text; charset=utf-8")
	public String getTuiJian(){
		List<Goods> goodsList = new ArrayList<Goods>();
		
		return null;
	}
	
	
	
	/**
	 * 商品比较
	 * @param modelMap
	 * @return
	 */
	@RequestMapping("/compare/{ids}")
	public String compare(@PathVariable(value="ids") String ids,ModelMap modelMap){
		String[] idstr = ids.split("__");
		List<Goods> list = new ArrayList<Goods>();
		List<String> specList = new ArrayList<String>();
		for(int i=0;i<idstr.length;i++){
			if(StringUtils.isNotBlank(idstr[i])){
				Goods goods = hbaseUtils.get(HbaseUtils.TABLE_NAME, idstr[i]);
				list.add(goods);
				String specHtml = doSpecParam(goods);
				specList.add(specHtml);
			}
		}
		modelMap.put("list", list);
		modelMap.put("specList", specList);
		return GOODS_COMPARE;
	}
	/**
	 * 组装table
	 * @param list
	 * @param goods
	 * @param spec
	 */
	private String doSpecParam( Goods goods) {
		String spec = goods.getSpec();
		JSONArray jsonArray = new JSONArray(spec);
		StringBuffer sb = new StringBuffer();
		sb.append("<table class=\"gridtable1\">");
		for (int j = 0; j < jsonArray.length(); j++) {
			JSONObject object = (JSONObject)jsonArray.get(j);
			sb.append("<tr>");
			String name = object.get("name").toString();
			String value = object.get("value").toString();
			if(StringUtils.isNotBlank(value)){
				sb.append("<td>"+name+"</td>");
				sb.append("<td>"+value+"</td>");
			}else{
				sb.append("<th colspan=\"2\">"+name+"</th>");
			}
			sb.append("</tr>");
		}
		sb.append("</table>");
		return sb.toString();
	}
	
	/**
	 * 查询商品的价格走势
	 * @return
	 */
	@ResponseBody
	@RequestMapping(value="/priceTrend/{id}",produces = "application/text; charset=utf-8")
	public String getPriceTrend(@PathVariable(value="id") String id){
		List<Map<String, String>> cellList = hbaseUtils.getCellMoreVersion(HbaseUtils.TABLE_NAME, HbaseUtils.COLUMNFAMILY_1, HbaseUtils.COLUMNFAMILY_1_PRICE,id);
		LineChartView lineChartView = new LineChartView();
		List<String> listData = new ArrayList<String>();
		StringBuffer xdata = new StringBuffer();
		int size = cellList.size();
		for (int i = size-1; i >=0; i--) {
			Map<String, String> map = cellList.get(i);
			listData.add(map.get("value"));
			xdata.append(map.get("time"));
			if(i>0){
				xdata.append(",");
			}
		}
		lineChartView.addData("价格走势", listData);
		lineChartView.setXaxisdata(xdata.toString());
		lineChartView.setTitle("");
		lineChartView.setYaxislable("元");
		
		Map<String, ChartView> root = new HashMap<String, ChartView>();
		root.put("chartView", lineChartView);
		String result = FreeMarkerUtil.parseTemplate("chart/linechart.ftl", root);
		return result;
	}
	
	/**
	 * 查看商品明细
	 * @return
	 */
	@RequestMapping("/detailGoodsById/{id}")
	public String detailGoodsById(@PathVariable(value="id") String id,ModelMap modelMap){
		Goods goods = hbaseUtils.get(HbaseUtils.TABLE_NAME, id);
		String specHtml = doSpecParam(goods);
		modelMap.put("goods", goods);
		modelMap.put("specHtml", specHtml);
		return GOODS_DETAIL;
	}
	
	
	/**
	 * 版本
	 * @return
	 */
	@RequestMapping("/version")
	public String version(){
		
		return PAGE_VERSION;
	}
}
