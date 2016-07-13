 <div  style="height: 32px; padding-left: 10px; background-color:#f7f7f7;  text-align: left; padding-top: 6px;">
                    <div  style="float: right; width: 200px; text-align: right; padding-top: 5px; margin-right: 10px;">
                       共找到 <font  style="color: #DF4100;">
                            ${count }</font> 个商品</div>
                    <div  style="float: left; width: 340px;">
                        <table  cellpadding="0"  cellspacing="0"><tbody><tr><td><div  class="sortDesced"><a  href="#"  style="font-size: 12px; color:#E85708" onclick="sortdata('');" title="按默认排序（匹配度）">默认</a></div></td><td><div  class="sortAsc"><a  href="#"  style="font-size: 12px; " onclick="sortdata('asc');"  title="按价格从低到高排序">价格</a></div></td><td><div  class="sortDesc"><a  href="#"  style="font-size: 12px; " onclick="sortdata('desc');" title="按价格从高到低排序">价格</a></div></td></tr></tbody></table>
                    </div>
                </div>
                <div  class="clear">
                </div>
 <div  id="ctl00_ContentPlaceHolder1_DivListpro"  style="text-align: left; margin-top: 10px; padding-left: 12px;"  class="team_main_side">
 <#list goodsList as goods>
 <div  class="divproduct">
	<div  class="divpropic">
		<a  href="${contentPath}/goods/detailGoodsById/${goods.id }"  target="_blank">
		<img  src="${goods.picpath }" original="${goods.picpath }" border="0"  width="120"  style="display: inline;"></a>
	</div>
	<div  class="divproname">
	<a  href="${contentPath}/goods/detailGoodsById/${goods.id }"  class="f12"  target="_blank" >${goods.name }</a>
	</div>
	<div>最低价：<font  class="fontmoney">￥${goods.price }</font></div>
	<div><img  src="${contentPath}/goods/pf5.png"    alt="强烈推荐"  align="absmiddle"  style="display: inline;">4.7分</div>
	<div  style="color:#999999">
		<font color="block">来源：${goods.from } </font>|
		<a  href="#" onclick="addCompare('${goods.id}','${goods.name}','${goods.dataurl}');"  class="f12sc">对比</a>
	</div> 
</div>
</#list>
	<div  class="clear">
	</div>
</div>
<div  id="ctl00_ContentPlaceHolder1_dispage"  style="margin: 10px 8px; padding: 3px;">共有${count}条记录  ，每页${range}条记录 &nbsp; ${pageString}</div>
