import{_ as t}from"./_plugin-vue_export-helper.cdc0426e.js";import{o as e,c as o,a as n,b as s,d as p,w as c,e as i,r as u}from"./app.fa6ea4a7.js";const r={},l=n("h1",{id:"rocketmq连接器配置示例",tabindex:"-1"},[n("a",{class:"header-anchor",href:"#rocketmq连接器配置示例","aria-hidden":"true"},"#"),s(" RocketMQ连接器配置示例")],-1),d=i(`<h2 id="rocketmq-writer-example" tabindex="-1"><a class="header-anchor" href="#rocketmq-writer-example" aria-hidden="true">#</a> RocketMQ Writer example</h2><p>假设我们在本地启动了一个RocketMQ服务，其name server地址为 &quot;127.0.0.1:9876&quot;, 并且我们在其中创建了一个名为 &quot;test_topic&quot; 的topic。</p><p>那么我们可以使用如下的配置文档写入上述topic：</p><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">{</span>
   <span class="token property">&quot;job&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
     <span class="token property">&quot;writer&quot;</span><span class="token operator">:</span> <span class="token punctuation">{</span>
       <span class="token property">&quot;class&quot;</span><span class="token operator">:</span> <span class="token string">&quot;com.bytedance.bitsail.connector.legacy.rocketmq.sink.RocketMQOutputFormat&quot;</span><span class="token punctuation">,</span>
       <span class="token property">&quot;name_server_address&quot;</span><span class="token operator">:</span> <span class="token string">&quot;127.0.0.1:9876&quot;</span><span class="token punctuation">,</span>
       <span class="token property">&quot;topic&quot;</span><span class="token operator">:</span> <span class="token string">&quot;test_topic&quot;</span><span class="token punctuation">,</span>
       
       <span class="token property">&quot;producer_group&quot;</span><span class="token operator">:</span> <span class="token string">&quot;test_producer_group&quot;</span><span class="token punctuation">,</span>
       <span class="token property">&quot;tag&quot;</span><span class="token operator">:</span> <span class="token string">&quot;itcase_test&quot;</span><span class="token punctuation">,</span>
       <span class="token property">&quot;key&quot;</span><span class="token operator">:</span> <span class="token string">&quot;id&quot;</span><span class="token punctuation">,</span>
       <span class="token property">&quot;partition_fields&quot;</span><span class="token operator">:</span> <span class="token string">&quot;id,date_field&quot;</span><span class="token punctuation">,</span>
       
       <span class="token property">&quot;columns&quot;</span><span class="token operator">:</span> <span class="token punctuation">[</span>
         <span class="token punctuation">{</span>
           <span class="token property">&quot;index&quot;</span><span class="token operator">:</span> <span class="token number">0</span><span class="token punctuation">,</span>
           <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;id&quot;</span><span class="token punctuation">,</span>
           <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;string&quot;</span>
         <span class="token punctuation">}</span><span class="token punctuation">,</span>
         <span class="token punctuation">{</span>
           <span class="token property">&quot;index&quot;</span><span class="token operator">:</span> <span class="token number">1</span><span class="token punctuation">,</span>
           <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;string_field&quot;</span><span class="token punctuation">,</span>
           <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;string&quot;</span>
         <span class="token punctuation">}</span><span class="token punctuation">,</span>
         <span class="token punctuation">{</span>
           <span class="token property">&quot;index&quot;</span><span class="token operator">:</span> <span class="token number">2</span><span class="token punctuation">,</span>
           <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;int_field&quot;</span><span class="token punctuation">,</span>
           <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;bigint&quot;</span>
         <span class="token punctuation">}</span><span class="token punctuation">,</span>
         <span class="token punctuation">{</span>
           <span class="token property">&quot;index&quot;</span><span class="token operator">:</span> <span class="token number">3</span><span class="token punctuation">,</span>
           <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;double_field&quot;</span><span class="token punctuation">,</span>
           <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;double&quot;</span>
         <span class="token punctuation">}</span><span class="token punctuation">,</span>
         <span class="token punctuation">{</span>
           <span class="token property">&quot;index&quot;</span><span class="token operator">:</span> <span class="token number">4</span><span class="token punctuation">,</span>
           <span class="token property">&quot;name&quot;</span><span class="token operator">:</span> <span class="token string">&quot;date_field&quot;</span><span class="token punctuation">,</span>
           <span class="token property">&quot;type&quot;</span><span class="token operator">:</span> <span class="token string">&quot;date&quot;</span>
         <span class="token punctuation">}</span>
       <span class="token punctuation">]</span>
     <span class="token punctuation">}</span>
   <span class="token punctuation">}</span>
<span class="token punctuation">}</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div>`,4);function k(q,v){const a=u("RouterLink");return e(),o("div",null,[l,n("p",null,[s("Parent document: "),p(a,{to:"/zh/documents/docs/connectors/rocketmq/rocketmq.html"},{default:c(()=>[s("rocketmq-connector")]),_:1})]),d])}const y=t(r,[["render",k],["__file","rocketmq-example.html.vue"]]);export{y as default};
