package com.ryxc.spider.web.utils;

import com.ryxc.spider.web.domain.Goods;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


/**
 * solr工具类
 *
 */
public class SolrUtil {
	static final Logger logger = LoggerFactory.getLogger(SolrUtil.class);
	private static final String SOLR_URL = "http://ryxc166:8983/solr"; // 服务器地址
	private static HttpSolrServer server = null;
	static{
		try {
			server = new HttpSolrServer(SOLR_URL);
			server.setAllowCompression(true);
			server.setConnectionTimeout(10000);
			server.setDefaultMaxConnectionsPerHost(100);
			server.setMaxTotalConnections(100);
		} catch (Exception e) {
			logger.error("请检查tomcat服务器或端口是否开启!{}",e);
			e.printStackTrace();
		}
	}
	/**
	 * 建立索引
	 * @throws Exception
	 */
	public static void addIndex(Goods goods) throws Exception {
		server.addBean(goods);
		// 对索引进行优化
		server.optimize();
		server.commit();
		logger.info("添加索引成功！商品id:{}",goods.getId());
	}
	
	/**
	 * 查询
	 * @param skey 
	 * @param range 
	 * @param start 
	 * @param sort 
	 * @throws Exception
	 */
	public static List<Goods> search(String skey, Long start, Long range, String sort) throws Exception {
		SolrQuery params = new SolrQuery();
		if(StringUtils.isNotBlank(skey)){
			params.set("q", "name:"+skey);
		}else{
			params.set("q", "*:*");
		}
		params.set("name", skey);
		params.set("start", ""+start);
		params.set("rows", ""+range);
		params.setHighlight(true); // 开启高亮组件
		if(org.apache.commons.lang3.StringUtils.isNotBlank(sort)){
			if(sort.equals("asc")){
				params.setSort("price", SolrQuery.ORDER.asc);
			}else{
				params.setSort("price", SolrQuery.ORDER.desc);
			}
		}
		params.addHighlightField("name");// 高亮字段

		params.setHighlightSimplePre("<font color='red'>");// 标记，高亮关键字前缀

		params.setHighlightSimplePost("</font>");// 后缀
		QueryResponse response = server.query(params);
		
		List<Goods> results = response.getBeans(Goods.class);
		return results;
	}

	public static int getCount(String skey) {
		int count = 0;
		SolrQuery params = new SolrQuery();
		if(StringUtils.isNotBlank(skey)){
			params.set("q", "name:"+skey);
		}else{
			params.set("q", "*:*");
		}
		try {
			QueryResponse response = server.query(params);
			count = (int) response.getResults().getNumFound();
		} catch (SolrServerException e) {
			logger.error("获取总数失败!");
			e.printStackTrace();
		}
		
		return count;
	}
	
}
