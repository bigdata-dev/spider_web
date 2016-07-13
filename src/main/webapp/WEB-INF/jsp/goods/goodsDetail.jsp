<%@page import="org.apache.commons.lang3.StringUtils"%>
<%@page import="org.json.JSONObject"%>
<%@page import="org.json.JSONArray"%>
<%@page import="cn.crxy.spider.web.domain.Goods"%>
<%@page import="java.util.List"%>
<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<%@ include file="/WEB-INF/tags/goods/goods_tag_common.tagf"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<title>商品详情</title>
<<script type="text/javascript">
	var root = '<%=request.getContextPath()%>';
	$(function(){
		loadChart("${goods.id}");
	});
</script>
<style type="text/css">
.gridtable {
	font-family: verdana, arial, sans-serif;
	font-size: 11px;
	color: #333333;
	border-width: 1px;
	border-color: #666666;
	border-collapse: collapse;
}

.gridtable th {
	width: 100px;
	border-width: 1px;
	padding: 8px;
	border-style: solid;
	border-color: #666666;
	background-color: #dedede;
}

.gridtable td {
	width: 800px;
	border-width: 1px;
	padding: 8px;
	border-style: solid;
	border-color: #666666;
	background-color: #ffffff;
}
</style>
<body>
	
	<div align="center">
	
	<goods:goods_head/>
	<table class="gridtable" width="998px" border="1px #000 solid"
		cellspacing="1" cellpadding="3">
		<tr>
			<th>标题</th>
			<td><a href="${goods.dataurl}" target="_black"><font color="red">${goods.name }</font></a></td>
		</tr>
		<tr>
			<th>图片</th>
			<td><img src="${goods.picpath }"
				original="${goods.picpath }" border="0" width="120"
				style="display: inline;"></td>
		</tr>
		<tr>
			<th>最低报价</th>
			<td>￥${goods.price }</td>
		</tr>
		<tr>
			<th>价格趋势</th>
			<td><div id="main" style="height:350px;width:600"></div></td>
		</tr>
		<tr>
			<th>参数</th>
			<td valign="top">${specHtml}</td>
		</tr>
	</table>
	<br />
	<br />
	</div>
</body>