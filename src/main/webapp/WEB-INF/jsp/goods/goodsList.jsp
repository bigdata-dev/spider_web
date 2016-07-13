<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<%@ include file="/WEB-INF/tags/goods/goods_tag_common.tagf"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<title>商品比价系统</title>


<script language="javascript" type="text/javascript">
	var root = '<%=request.getContextPath()%>'
	$(document).ready(function() {
		//加载数据
		$.ajax({
			url : "${goods_path}/goods/search",
			dataType : 'text',
			success : function(data) {
				$("#dataList").empty().append(data);
			},
			error : function(data) {
				alert("数据记载失败!");
			}

		});

		jQuery.focusblur = function(focusid) {
			var focusblurid = $(focusid);
			var defval = focusblurid.val();
			focusblurid.focus(function() {
				var thisval = $(this).val();
				if (thisval == defval) {
					$(this).val("");
				}
			});
			focusblurid.blur(function() {
				var thisval = $(this).val();
				if (thisval == "") {
					$(this).val(defval);
				}
			});

		};
		/*下面是调用方法*/
		$.focusblur("#skey");
	});
	//对比
	function compare() {
		var checks = "";
		var count = 0;
		jQuery("input[name='compareid']").each(function(){
			checks += $(this).val() + "__";
			count++;
		});
		if (count < 2) {
			alert("请至少选择两条对比数据!");
			return;
		}
		if (count > 4) {
			alert("最多只能选择四条数据对比!");
			return;
		}
		window.open("${goods_path}/goods/compare/" + checks);
	}
	//下一页    	
	function nextPage(url, param) {
		var params = param.split("&");
		var start = params[0].split("=")[1];
		var range = params[1].split("=")[1];
		$.ajax({
			url : url,
			type : "get",
			data : {
				start : start,
				range : range
			},
			dataType : 'text',
			error : function(msg) {
				$.messager.alert("操作提示", "加载数据出错！");
			},
			success : function(text) {
				$("#dataList").empty().append(text);
			}
		});
	}
	//查询
	function searchdata() {
		var key = $("#skey").val();
		if (key == "请输入您想比较的商品名称或型号") {
			key = '';
		}
		$.ajax({
			url : "${goods_path}/goods/search",
			data : {
				skey : key
			},
			dataType : 'text',
			contentType: "application/x-www-form-urlencoded; charset=utf-8", 
			success : function(data) {
				$("#dataList").empty().append(data);
			},
			error : function(data) {
				alert("数据记载失败!");
			}
		});
	}
	//排序
	function sortdata(param) {
		$.ajax({
			url : "${goods_path}/goods/search",
			data : {
				sort : param
			},
			dataType : 'text',
			success : function(data) {
				$("#dataList").empty().append(data);
			},
			error : function(data) {
				alert("数据记载失败!");
			}
		});
	}
	
	//隐藏对比框
	function setdivhide(){
		$("#dbnr2").css("display","none");
	}
	function setdivshow(){
		$("#dbnr2").css("display","block");
	}
	//添加对比
	function addCompare(id,name,url){
		var s1 = $("#dbdiv").css("display");
		if(s1=='none'){
			$("#dbdiv").css("display","block");
		}
		var s = $("#dbnr2").css("display");
		if(s=='none'){
			$("#dbnr2").css("display","block");
		}
		var ss = $("#tishi").css("display");
		if(ss=='block'){
			$("#tishi").css("display","none");
		}
		if($("#"+id).html()!=null){
			alert(name+"已经加入对比!");
			return;
		}
		$("#comdata").append("<li id=\""+id+"\"><input type=\"hidden\" name=\"compareid\" value=\""+id+"\"><img src=\"${goods_path}/goods/closex.gif\" align=\"absmiddle\" onclick=\"delCookie('"+id+"')\"><a href=\""+url+"\" target=\"_blank\">"+name+"</a></li>");
	}
	//删除对比的商品
	function delCookie(id){
		$("#"+id).remove();
		if($("#comdata").html()==''){
			$("#tishi").css("display","block");
		}
	}
	//清空
	function clears(){
		$("#comdata").empty();
		$("#tishi").css("display","block");
	}
	
</script>
<style type="text/css">
A.f12hot:link, A.f12hot:active, A.f12hot:visited {
	text-decoration: none;
	color: #999;
}

A.f12hot:hover {
	text-decoration: none;
	color: #F16320;
}

A.f12sc:link, A.f12sc:active {
	font-size: 12px;
	text-decoration: none;
	color: #005AA0;
}

A.f12sc:visited {
	font-size: 12px;
	text-decoration: none;
	color: #005AA0;
}

A.f12sc:hover {
	font-size: 12px;
	text-decoration: underline;
	color: #ED0000;
}

#fk{
background: url('${goods_path}/goods/pk.gif');
}
#fk{
	background-position:0 0px;
	width:51px;
	height:51px;
	overflow:hidden;
	text-indent:-2000em;
	position:fixed;
	_position:absolute;
	right:10px;
	bottom:133px;
	display:none;
}
#fk:hover{
	background-position:0 -1px;
}
</style>
<body>
	<div align="center">
		<goods:goods_head/>
		<div style="width: 998px; text-align: center; margin-top: 3px;">
			<!-- 商品数据 -->
			<div style="width: 775px; float: right;">
				<div class="divRightMoldKuang">
					<div id="dataList"></div>
				</div>
			</div>
			<!-- 左侧商品推荐 -->
			<div
				style="width: 210px; float: left; margin-left: 2px; text-align: left;">
				<div class="divLeftMoldKuang">
					<div class="divLeftTitleDi">
						<a href="#"
							class="f14yh">推荐商品</a>
					</div>
				<c:forEach items="${goodsList }" var="goods">
					<div id="Div1"
						style="text-align: left; padding: 6px 0px; overflow: hidden;">
						<div class="plistCommentTitle">
							<font class="plistCommentDisPing">[荐]</font><a
								href="${goods.dataurl }" class="f12sc">${goods.name }</a>
						</div>
						<div class="plistCommentContent">
							<div class="plistCommentLeft">
								<a href="${goods.dataurl }"><img src="${goods.picpath }"
									width="50"  border="0"></a>
							</div>
							<div class="plistCommentRight">
								最低价：￥${goods.price }<a href="#" onclick="showPrice('${goods.id }')">价格走势</a>
							</div>
						</div>
						<div class="clear"></div>
						<div>
							<hr size="1" class="hrxu">
						</div>
					</div>
				</c:forEach>
					
				</div>
			</div>

		</div>

		<div id="dbdiv" style="display: none">
			<div id="dbimg" style="" onclick="setdivshow();"></div>
			<div id="dbnr2" style="display: none;">
				<b><span>产品对比</span> <span class="span2" onclick="setdivhide()"><img
						src="${goods_path}/goods/closex.gif"
						align="absmiddle" alt="隐藏对比框">&nbsp;隐藏对比框</span></b>
				<div id="tishi" style="display: block;">对不起，您还没有选择产品</div>
				<ul style="text-align: left;" id="comdata"></ul>
				<div class="pkbut">
					<span><a href="#" onclick="compare();">开始对比</a></span><span
						onclick="clears()">清&nbsp;空</span>
				</div>
			</div>
		</div>
	</div>
</body>