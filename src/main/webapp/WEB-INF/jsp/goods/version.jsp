<%@page import="org.apache.commons.lang3.StringUtils"%>
<%@page import="org.json.JSONObject"%>
<%@page import="org.json.JSONArray"%>
<%@page import="cn.crxy.spider.web.domain.Goods"%>
<%@page import="java.util.List"%>
<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<%@ include file="/WEB-INF/tags/goods/goods_tag_common.tagf"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<title>商品比较</title>

<body>

	<div align="center">
		<goods:goods_head />
		<h2>版本说明：当前版本V0.1</h2>
		<br />
		<div>
			<table>
				<tr>
					<td><font color="red">2015-01-05：更新说明</font><br/>
						1：后台solr单独建立索引。<br/>
						2：前台添加价格波动趋势<br/>
					</td>
				</tr>
				<tr>
					<td><font color="red">2014-12-13：更新说明</font><br/>
						1：爬取京东，易迅的手机数据。<br/>
						2：前台solr查询。<br/>
						3：商品信息对比。<br/>
					</td>
				</tr>
			</table>
		</div>
	</div>
</body>