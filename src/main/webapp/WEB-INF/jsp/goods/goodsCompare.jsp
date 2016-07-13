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
	width: 260px;
	border-width: 1px;
	padding: 8px;
	border-style: solid;
	border-color: #666666;
	background-color: #ffffff;
}

.gridtable1 {
	font-family: verdana, arial, sans-serif;
	font-size: 11px;
	color: #333333;
	border-width: 1px;
	border-color: #666666;
	border-collapse: collapse;
}

.gridtable1 td {
	width: 100px;
	border-width: 1px;
	padding: 8px;
	border-style: solid;
	border-color: #666666;
	background-color: #ffffff;
}

.gridtable1 th {
	width: 100px;
	border-width: 1px;
	padding: 8px;
	border-style: solid;
	border-color: #666666;
	background-color: #D1EAEA;
}
</style>
<body>
	
	<div align="center">
	
	<goods:goods_head/>
	<table class="gridtable" width="998px" border="1px #000 solid"
		cellspacing="1" cellpadding="3">
		<tr>
			<th>标题</th>
			<td><a href="${list[0].dataurl}" target="_black">${list[0].name }</a></td>
			<td><a href="${list[1].dataurl}" target="_black">${list[1].name }</a></td>
			<td><c:if test="${list[2]!=null }"><a href="${list[2].dataurl}" target="_black">${list[2].name }</a></c:if></td>
			<td><c:if test="${list[3]!=null }"><a href="${list[3].dataurl}" target="_black">${list[3].name }</a></c:if></td>
		</tr>
		<tr>
			<th>图片</th>
			<td><img src="${list[0].picpath }"
				original="${list[0].picpath }" border="0" width="120"
				style="display: inline;"></td>
			<td><img src="${list[1].picpath }"
				original="${list[1].picpath }" border="0" width="120"
				style="display: inline;"></td>
			<td><c:if test="${list[2]!=null }">
					<img src="${list[2].picpath }" original="${list[2].picpath }"
						border="0" width="120" style="display: inline;">
				</c:if></td>
			<td><c:if test="${list[3]!=null }">
					<img src="${list[3].picpath }" original="${list[3].picpath }"
						border="0" width="120" style="display: inline;">
				</c:if></td>
		</tr>
		<tr>
			<th>最低报价</th>
			<td>￥${list[0].price }</td>
			<td>￥${list[1].price }</td>
			<td><c:if test="${list[2]!=null }">￥${list[2].price }</c:if></td>
			<td><c:if test="${list[3]!=null }">￥${list[3].price }</c:if></td>
		</tr>
		<tr>
			<th>参数</th>
			<td valign="top">${specList[0]}</td>
			<td valign="top">${specList[1]}</td>
			<td valign="top"><c:if test="${specList[2]!=null }">${specList[2] }</c:if></td>
			<td valign="top"><c:if test="${specList[3]!=null }">${specList[3] }</c:if></td>
		</tr>
	</table>
	<br />
	<br />
	</div>
</body>