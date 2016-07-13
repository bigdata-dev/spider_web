<%@ tag language="java" body-content="scriptless" pageEncoding="UTF-8"%>
<%@ include file="goods_tag_common.tagf"%>
		<div
			style="width: 100%; height: 27px; line-height: 27px; border-bottom: solid 1px #DCDCDC; background-color: #f7f7f7;"
			align="center"></div>
		<div class="clear"></div>
		<div style="width: 100%; background-color: #fff; height: 100px;">
			<div style="width: 1000px; margin: 0px auto; text-align: center;">
				<div style="width: 994px; margin: 0px; height: 100px;">
					<div
						style="float: left; width: 410px; text-align: left; margin-top: 15px;">
						<!-- logo -->
						<img src="" border="0">
						<img src="${goods_path}/goods/webdesc.png"  border="0">
					</div>
					<div
						style="float: right; padding: 15px 0px 8px 0px; *padding-top: 20px; width: 580px; text-align: left;">

						<div style="margin-top: 6px;">
							<input id="skey"
								style="margin-top: 1px; border: 2px solid #ff9000; PADDING: 0px 4px 0px 4px; FONT-SIZE: 14px; WIDTH: 480px; COLOR: #a9a9a9; LINE-HEIGHT: 34px; HEIGHT: 34px; display: inline-block; vertical-align: middle;"
								value="请输入您想比较的商品名称或型号" name="key" autocomplete="off"> <input
								style="width: 85px; border: none; height: 38px; margin-top: 1px; background: url(${goods_path}/goods/Search2014.gif) no-repeat center #ff9000; cursor: pointer; display: inline-block; vertical-align: middle;"
								type="button" value="" name="btnSearch" onclick="searchdata();">
						</div>
						<div class="clear"></div>

						<div
							style="color: #999; margin-top: 2px; height: 22px; line-height: 22px;">
							热门比价：<a href="#" class="f12hui">iPhone</a>
						</div>
					</div>
				</div>
				<div class="clear"></div>
			</div>
		</div>


		<div
			style="width: 100%; background-color: #FF9000; height: 40px; border-bottom: 1px solid #EE7101;">
			<div style="width: 994px; margin: 0px auto; text-align: center;">
				<div style="margin: 0px;">
					<div class="menudiv3 menudiv1d" id="menuindex">
						<a href="${goods_path}/goods" class="menu">首页</a>
					</div>
					<div class="menudiv3" id="menubijia">
						<a href="#" class="menu">开发中</a>
					</div>
					<div class="menudiv3" id="menucuxiao">
						<a href="#" class="menu">开发中</a>
					</div>
					<div class="menudiv3" id="menubaicai">
						<a href="#" class="menu">开发中</a>
					</div>
					<div class="menudiv3" id="menufanli">
						<a href="#" class="menu">开发中</a>
					</div>
					<div class="menudiv3" id="menucomment">
						<a href="${goods_path}/goods/version" class="menu">版本说明</a>
					</div>
				</div>
				<div class="clear"></div>
			</div>
		</div>
		<div></div>
		<!-- 导航与下面的空格 -->
		<div
			style="width: 990px; height: 20px; text-align: left; padding: 10px 4px 0px 4px;">
			<div
				style="float: right; text-align: right; width: 100px; margin-right: 10px;">
			</div>
		</div>
