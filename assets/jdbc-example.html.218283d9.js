import{_ as t}from"./_plugin-vue_export-helper.cdc0426e.js";import{o as i,c as a,a as n,b as s,d as u,w as o,e as l,r as d}from"./app.7ba59d0f.js";const r={},c=n("h1",{id:"jdbc连接器示例",tabindex:"-1"},[n("a",{class:"header-anchor",href:"#jdbc连接器示例","aria-hidden":"true"},"#"),s(" Jdbc连接器示例")],-1),v=l(`<h2 id="mysql-example" tabindex="-1"><a class="header-anchor" href="#mysql-example" aria-hidden="true">#</a> MySQL Example</h2><h3 id="mysql-source" tabindex="-1"><a class="header-anchor" href="#mysql-source" aria-hidden="true">#</a> MySQL source</h3><h4 id="mysql-table-sync" tabindex="-1"><a class="header-anchor" href="#mysql-table-sync" aria-hidden="true">#</a> MySQL table sync</h4><div class="language-Json line-numbers-mode" data-ext="Json"><pre class="language-Json"><code>{
    &quot;job&quot;:{
        &quot;reader&quot;:{
            &quot;class&quot;:&quot;com.bytedance.bitsail.connector.legacy.jdbc.source.JDBCInputFormat&quot;,
            &quot;columns&quot;:[
                {
                    &quot;name&quot;:&quot;id&quot;,
                    &quot;type&quot;:&quot;bigint&quot;
                },
                {
                    &quot;name&quot;:&quot;name&quot;,
                    &quot;type&quot;:&quot;varchar&quot;
                },
                {
                    &quot;name&quot;:&quot;int_info&quot;,
                    &quot;type&quot;:&quot;int&quot;
                },
                {
                    &quot;name&quot;:&quot;double_info&quot;,
                    &quot;type&quot;:&quot;double&quot;
                },
                {
                    &quot;name&quot;:&quot;bytes_info&quot;,
                    &quot;type&quot;:&quot;binary&quot;
                }
            ],
            &quot;table_name&quot;:&quot;your table name&quot;,
            &quot;password&quot;:&quot;your password&quot;,
            &quot;db_name&quot;:&quot;your db name&quot;,
            &quot;user_name&quot;:&quot;your user name&quot;,
            &quot;split_pk&quot;:&quot;id&quot;,
            &quot;connections&quot;:[
                {
                    &quot;slaves&quot;:[
                        {
                            &quot;db_url&quot;:&quot;jdbc:mysql://address=(protocol=tcp)(host=192.168.1.202)(port=3306)/test?permitMysqlScheme&amp;rewriteBatchedStatements=true&amp;autoReconnect=true&amp;useUnicode=true&amp;characterEncoding=utf-8&amp;zeroDateTimeBehavior=convertToNull&amp;jdbcCompliantTruncation=false&quot;
                        }
                    ]
                }
            ]
        }
    }
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="mysql-sql-sync" tabindex="-1"><a class="header-anchor" href="#mysql-sql-sync" aria-hidden="true">#</a> MySQL SQL sync</h4><div class="language-Json line-numbers-mode" data-ext="Json"><pre class="language-Json"><code>{
    &quot;job&quot;:{
        &quot;reader&quot;:{
            &quot;class&quot;:&quot;com.bytedance.bitsail.connector.legacy.jdbc.source.JDBCInputFormat&quot;,
            &quot;columns&quot;:[
                {
                    &quot;index&quot;:0,
                    &quot;name&quot;:&quot;id&quot;,
                    &quot;type&quot;:&quot;bigint&quot;
                },
                {
                    &quot;name&quot;:&quot;name&quot;,
                    &quot;type&quot;:&quot;varchar&quot;
                },
                {
                    &quot;name&quot;:&quot;int_info&quot;,
                    &quot;type&quot;:&quot;int&quot;
                },
                {
                    &quot;name&quot;:&quot;double_info&quot;,
                    &quot;type&quot;:&quot;double&quot;
                },
                {
                    &quot;name&quot;:&quot;bytes_info&quot;,
                    &quot;type&quot;:&quot;binary&quot;
                }
            ],
            &quot;password&quot;:&quot;your password&quot;,
            &quot;db_name&quot;:&quot;your db name&quot;,
            &quot;user_name&quot;:&quot;your user name&quot;,
            &quot;customized_sql&quot;:&quot;select id, name, int_info, double_info, bytes_info from table_name where id &lt; 100&quot;,
            &quot;connections&quot;:[
                {
                    &quot;slaves&quot;:[
                        {
                            &quot;db_url&quot;:&quot;jdbc:mysql://address=(protocol=tcp)(host=192.168.1.202)(port=3306)/test?permitMysqlScheme&amp;rewriteBatchedStatements=true&amp;autoReconnect=true&amp;useUnicode=true&amp;characterEncoding=utf-8&amp;zeroDateTimeBehavior=convertToNull&amp;jdbcCompliantTruncation=false&quot;
                        }
                    ]
                }
            ]
        }
    }
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="mysql-sink" tabindex="-1"><a class="header-anchor" href="#mysql-sink" aria-hidden="true">#</a> MySQL sink</h3><h4 id="mysql-insert-sync" tabindex="-1"><a class="header-anchor" href="#mysql-insert-sync" aria-hidden="true">#</a> MySQL insert sync</h4><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">{</span>
    <span class="token property">&quot;job&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
        <span class="token property">&quot;writer&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
            <span class="token property">&quot;class&quot;</span><span class="token operator">:</span><span class="token string">&quot;com.bytedance.bitsail.connector.legacy.jdbc.sink.JDBCOutputFormat&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;partition_value&quot;</span><span class="token operator">:</span><span class="token string">&quot;20221001&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;user_name&quot;</span><span class="token operator">:</span><span class="token string">&quot;test&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;columns&quot;</span><span class="token operator">:</span><span class="token punctuation">[</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;name&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;varchar&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;int_info&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;int&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;double_info&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;double&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;bytes_info&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;binary&quot;</span>
                <span class="token punctuation">}</span>
            <span class="token punctuation">]</span><span class="token punctuation">,</span>
            <span class="token property">&quot;partition_pattern_format&quot;</span><span class="token operator">:</span><span class="token string">&quot;yyyyMMdd&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;table_name&quot;</span><span class="token operator">:</span><span class="token string">&quot;table name&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;password&quot;</span><span class="token operator">:</span><span class="token string">&quot;password&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;db_name&quot;</span><span class="token operator">:</span><span class="token string">&quot;test&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;connections&quot;</span><span class="token operator">:</span><span class="token punctuation">[</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;db_url&quot;</span><span class="token operator">:</span><span class="token string">&quot;jdbc:mysql://address=(protocol=tcp)(host=192.168.1.202)(port=3306)/test?permitMysqlScheme&amp;rewriteBatchedStatements=true&amp;autoReconnect=true&amp;useUnicode=true&amp;characterEncoding=utf-8&amp;zeroDateTimeBehavior=convertToNull&amp;jdbcCompliantTruncation=false&quot;</span>
                <span class="token punctuation">}</span>
            <span class="token punctuation">]</span><span class="token punctuation">,</span>
            <span class="token property">&quot;partition_name&quot;</span><span class="token operator">:</span><span class="token string">&quot;datetime&quot;</span>
        <span class="token punctuation">}</span>
    <span class="token punctuation">}</span>
<span class="token punctuation">}</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="mysql-overwrite-sync" tabindex="-1"><a class="header-anchor" href="#mysql-overwrite-sync" aria-hidden="true">#</a> MySQL overwrite sync</h4><div class="language-json line-numbers-mode" data-ext="json"><pre class="language-json"><code><span class="token punctuation">{</span>
    <span class="token property">&quot;job&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
        <span class="token property">&quot;writer&quot;</span><span class="token operator">:</span><span class="token punctuation">{</span>
            <span class="token property">&quot;class&quot;</span><span class="token operator">:</span><span class="token string">&quot;com.bytedance.bitsail.connector.legacy.jdbc.sink.JDBCOutputFormat&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;write_mode&quot;</span><span class="token operator">:</span><span class="token string">&quot;overwrite&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;user_name&quot;</span><span class="token operator">:</span><span class="token string">&quot;test&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;columns&quot;</span><span class="token operator">:</span><span class="token punctuation">[</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;name&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;varchar&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;int_info&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;int&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;double_info&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;double&quot;</span>
                <span class="token punctuation">}</span><span class="token punctuation">,</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;name&quot;</span><span class="token operator">:</span><span class="token string">&quot;bytes_info&quot;</span><span class="token punctuation">,</span>
                    <span class="token property">&quot;type&quot;</span><span class="token operator">:</span><span class="token string">&quot;binary&quot;</span>
                <span class="token punctuation">}</span>
            <span class="token punctuation">]</span><span class="token punctuation">,</span>
            <span class="token property">&quot;table_name&quot;</span><span class="token operator">:</span><span class="token string">&quot;table name&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;password&quot;</span><span class="token operator">:</span><span class="token string">&quot;password&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;db_name&quot;</span><span class="token operator">:</span><span class="token string">&quot;test&quot;</span><span class="token punctuation">,</span>
            <span class="token property">&quot;connections&quot;</span><span class="token operator">:</span><span class="token punctuation">[</span>
                <span class="token punctuation">{</span>
                    <span class="token property">&quot;db_url&quot;</span><span class="token operator">:</span><span class="token string">&quot;jdbc:mysql://address=(protocol=tcp)(host=192.168.1.202)(port=3306)/test?permitMysqlScheme&amp;rewriteBatchedStatements=true&amp;autoReconnect=true&amp;useUnicode=true&amp;characterEncoding=utf-8&amp;zeroDateTimeBehavior=convertToNull&amp;jdbcCompliantTruncation=false&quot;</span>
                <span class="token punctuation">}</span>
            <span class="token punctuation">]</span>
        <span class="token punctuation">}</span>
    <span class="token punctuation">}</span>
<span class="token punctuation">}</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><hr><h2 id="oracle-example" tabindex="-1"><a class="header-anchor" href="#oracle-example" aria-hidden="true">#</a> Oracle Example</h2><h3 id="oracle-source" tabindex="-1"><a class="header-anchor" href="#oracle-source" aria-hidden="true">#</a> Oracle source</h3><div class="language-Json line-numbers-mode" data-ext="Json"><pre class="language-Json"><code>{
    &quot;job&quot;:{
        &quot;reader&quot;:{
            &quot;class&quot;:&quot;com.bytedance.bitsail.connector.legacy.jdbc.source.OracleInputFormat&quot;,
            &quot;columns&quot;: [
                {
                  &quot;index&quot;: 0,
                  &quot;name&quot;: &quot;ID&quot;,
                  &quot;type&quot;: &quot;varchar&quot;
                },
                {
                  &quot;index&quot;: 1,
                  &quot;name&quot;: &quot;INT_TYPE&quot;,
                  &quot;type&quot;: &quot;integer&quot;
                },
                {
                  &quot;index&quot;: 2,
                  &quot;name&quot;: &quot;BIGINT_TYPE&quot;,
                  &quot;type&quot;: &quot;number&quot;
                },
                {
                  &quot;index&quot;: 3,
                  &quot;name&quot;: &quot;FLOAT_TYPE&quot;,
                  &quot;type&quot;: &quot;float&quot;
                },
                {
                  &quot;index&quot;: 4,
                  &quot;name&quot;: &quot;DOUBLE_TYPE&quot;,
                  &quot;type&quot;: &quot;double&quot;
                },
                {
                  &quot;index&quot;: 5,
                  &quot;name&quot;: &quot;RAW_TYPE&quot;,
                  &quot;type&quot;: &quot;raw&quot;
                },
                {
                  &quot;index&quot;: 6,
                  &quot;name&quot;: &quot;DATE_TYPE&quot;,
                  &quot;type&quot;: &quot;date&quot;
                }
            ],
            &quot;user_name&quot;:&quot;your user name&quot;,
            &quot;password&quot;:&quot;your password&quot;,
            &quot;db_name&quot;:&quot;your db name&quot;,
            &quot;table_schema&quot;:&quot;your schema name&quot;,
            &quot;table_name&quot;:&quot;your table name&quot;,
            &quot;split_pk&quot;:&quot;ID&quot;,
            &quot;connections&quot;:[
                {
                    &quot;slaves&quot;:[
                        {
							&quot;db_url&quot;:&quot;jdbc:oracle:thin:@localhost:51912/TEST&quot;,
							&quot;host&quot;:&quot;localhost&quot;,
							&quot;port&quot;:51912
						}
                    ]
                }
            ]
        }
    }
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="oracle-sink" tabindex="-1"><a class="header-anchor" href="#oracle-sink" aria-hidden="true">#</a> Oracle sink</h3><div class="language-Json line-numbers-mode" data-ext="Json"><pre class="language-Json"><code>{
    &quot;job&quot;:{
        &quot;writer&quot;: {
          &quot;class&quot;: &quot;com.bytedance.bitsail.connector.legacy.jdbc.sink.OracleOutputFormat&quot;,
          &quot;db_name&quot;: &quot;your db name&quot;,
          &quot;table_name&quot;: &quot;your table name&quot;,
          &quot;primary_key&quot;: &quot;ID&quot;,
          &quot;connections&quot;: [
          ],
          &quot;user_name&quot;: &quot;your user name&quot;,
          &quot;password&quot;: &quot;your password&quot;,
          &quot;write_mode&quot;: &quot;insert&quot;,
          &quot;writer_parallelism_num&quot;: 1,
          &quot;partition_name&quot;: &quot;DATETIME&quot;,
          &quot;partition_value&quot;: &quot;20220705&quot;,
          &quot;partition_pattern_format&quot;: &quot;yyyyMMdd&quot;,
          &quot;columns&quot;: [
            {
              &quot;index&quot;: 0,
              &quot;name&quot;: &quot;ID&quot;,
              &quot;type&quot;: &quot;varchar&quot;
            },
            {
              &quot;index&quot;: 1,
              &quot;name&quot;: &quot;INT_TYPE&quot;,
              &quot;type&quot;: &quot;integer&quot;
            },
            {
              &quot;index&quot;: 2,
              &quot;name&quot;: &quot;BIGINT_TYPE&quot;,
              &quot;type&quot;: &quot;number&quot;
            },
            {
              &quot;index&quot;: 3,
              &quot;name&quot;: &quot;FLOAT_TYPE&quot;,
              &quot;type&quot;: &quot;float&quot;
            },
            {
              &quot;index&quot;: 4,
              &quot;name&quot;: &quot;DOUBLE_TYPE&quot;,
              &quot;type&quot;: &quot;double&quot;
            },
            {
              &quot;index&quot;: 5,
              &quot;name&quot;: &quot;RAW_TYPE&quot;,
              &quot;type&quot;: &quot;raw&quot;
            },
            {
              &quot;index&quot;: 6,
              &quot;name&quot;: &quot;DATE_TYPE&quot;,
              &quot;type&quot;: &quot;date&quot;
            }
          ],
          &quot;connections&quot;:[
            {
              &quot;db_url&quot;:&quot;jdbc:oracle:thin:@localhost:1521/test?currentSchema=opensource_test&amp;rewriteBatchedStatements=true&amp;autoReconnect=true&amp;useUnicode=true&amp;characterEncoding=utf-8&amp;zeroDateTimeBehavior=convertToNull&quot;
            }
          ]
        }
    }
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><hr><h2 id="postgresql-example" tabindex="-1"><a class="header-anchor" href="#postgresql-example" aria-hidden="true">#</a> PostgreSQL Example</h2><h3 id="postgresql-source" tabindex="-1"><a class="header-anchor" href="#postgresql-source" aria-hidden="true">#</a> PostgreSQL source</h3><div class="language-Json line-numbers-mode" data-ext="Json"><pre class="language-Json"><code>{
    &quot;job&quot;:{
        &quot;reader&quot;:{
            &quot;class&quot;:&quot;com.bytedance.bitsail.connector.legacy.jdbc.source.PostgresqlInputFormat&quot;,
            &quot;columns&quot;:[
                {
                    &quot;index&quot;:0,
                    &quot;name&quot;:&quot;id&quot;,
                    &quot;type&quot;:&quot;bigint&quot;
                },
                {
                    &quot;name&quot;:&quot;name&quot;,
                    &quot;type&quot;:&quot;varchar&quot;
                },
                {
                    &quot;name&quot;:&quot;int_info&quot;,
                    &quot;type&quot;:&quot;int&quot;
                },
                {
                    &quot;name&quot;:&quot;double_info&quot;,
                    &quot;type&quot;:&quot;double&quot;
                },
                {
                    &quot;name&quot;:&quot;bytes_info&quot;,
                    &quot;type&quot;:&quot;bytea&quot;
                }
            ],
            &quot;user_name&quot;:&quot;your user name&quot;,
            &quot;password&quot;:&quot;your password&quot;,
            &quot;db_name&quot;:&quot;your db name&quot;,
            &quot;table_schema&quot;:&quot;your schema name&quot;,
            &quot;table_name&quot;:&quot;your table name&quot;,
            &quot;split_pk&quot;:&quot;id&quot;,
            &quot;connections&quot;:[
                {
                    &quot;slaves&quot;:[
                        {
                            &quot;db_url&quot;:&quot;jdbc:postgresql://192.168.1.202:5432/test?currentSchema=opensource_test&amp;rewriteBatchedStatements=true&amp;autoReconnect=true&amp;useUnicode=true&amp;characterEncoding=utf-8&amp;zeroDateTimeBehavior=convertToNull&quot;
                        }
                    ]
                }
            ]
        }
    }
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="postgresql-sink" tabindex="-1"><a class="header-anchor" href="#postgresql-sink" aria-hidden="true">#</a> PostgreSQL sink</h3><div class="language-Json line-numbers-mode" data-ext="Json"><pre class="language-Json"><code>{
    &quot;job&quot;:{
        &quot;writer&quot;:{
            &quot;class&quot;:&quot;com.bytedance.bitsail.connector.legacy.jdbc.sink.PostgresqlOutputFormat&quot;,
            &quot;partition_value&quot;:&quot;20221001&quot;,
            &quot;user_name&quot;:&quot;test&quot;,
            &quot;columns&quot;:[
                {
                    &quot;name&quot;:&quot;name&quot;,
                    &quot;type&quot;:&quot;varchar&quot;
                },
                {
                    &quot;name&quot;:&quot;int_info&quot;,
                    &quot;type&quot;:&quot;int&quot;
                },
                {
                    &quot;name&quot;:&quot;double_info&quot;,
                    &quot;type&quot;:&quot;double&quot;
                },
                {
                    &quot;name&quot;:&quot;bytes_info&quot;,
                    &quot;type&quot;:&quot;binary&quot;
                }
            ],
            &quot;partition_pattern_format&quot;:&quot;yyyyMMdd&quot;,
            &quot;table_name&quot;:&quot;table name&quot;,
            &quot;password&quot;:&quot;password&quot;,
            &quot;db_name&quot;:&quot;test&quot;,
            &quot;table_schema&quot;:&quot;your table schema&quot;,
            &quot;primary_key&quot;: &quot;id&quot;,
            &quot;connections&quot;:[
                {
                    &quot;db_url&quot;:&quot;jdbc:postgresql://192.168.1.202:5432/test?currentSchema=opensource_test&amp;rewriteBatchedStatements=true&amp;autoReconnect=true&amp;useUnicode=true&amp;characterEncoding=utf-8&amp;zeroDateTimeBehavior=convertToNull&quot;
                }
            ],
            &quot;partition_name&quot;:&quot;datetime&quot;
        }
    }
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><hr><h2 id="sqlserver-example" tabindex="-1"><a class="header-anchor" href="#sqlserver-example" aria-hidden="true">#</a> SqlServer Example</h2><h3 id="sqlserver-source" tabindex="-1"><a class="header-anchor" href="#sqlserver-source" aria-hidden="true">#</a> SqlServer source</h3><div class="language-Json line-numbers-mode" data-ext="Json"><pre class="language-Json"><code>{
    &quot;job&quot;:{
        &quot;reader&quot;:{
            &quot;class&quot;:&quot;com.bytedance.bitsail.connector.legacy.jdbc.source.SqlServerInputFormat&quot;,
            &quot;columns&quot;:[
                {
                    &quot;index&quot;:0,
                    &quot;name&quot;:&quot;id&quot;,
                    &quot;type&quot;:&quot;bigint&quot;
                },
                {
                    &quot;name&quot;:&quot;name&quot;,
                    &quot;type&quot;:&quot;varchar&quot;
                },
                {
                    &quot;name&quot;:&quot;int_info&quot;,
                    &quot;type&quot;:&quot;int&quot;
                },
                {
                    &quot;name&quot;:&quot;double_info&quot;,
                    &quot;type&quot;:&quot;double&quot;
                }
            ],
            &quot;table_name&quot;:&quot;your table name&quot;,
            &quot;password&quot;:&quot;your password&quot;,
            &quot;db_name&quot;:&quot;your db name&quot;,
            &quot;table_schema&quot;:&quot;your table schema&quot;,
            &quot;user_name&quot;:&quot;your user name&quot;,
            &quot;split_pk&quot;:&quot;id&quot;,
            &quot;connections&quot;:[
                {
                    &quot;slaves&quot;:[
                        {
                            &quot;db_url&quot;:&quot;jdbc:sqlserver://192.168.1.202:1433;databaseName=dts_test&quot;
                        }
                    ]
                }
            ]
        }
    }
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="sqlserver-sink" tabindex="-1"><a class="header-anchor" href="#sqlserver-sink" aria-hidden="true">#</a> SqlServer sink</h3><div class="language-Json line-numbers-mode" data-ext="Json"><pre class="language-Json"><code>{
    &quot;job&quot;:{
        &quot;writer&quot;:{
            &quot;class&quot;:&quot;com.bytedance.bitsail.connector.legacy.jdbc.sink.SqlServerOutputFormat&quot;,
            &quot;partition_value&quot;:&quot;20221001&quot;,
            &quot;user_name&quot;:&quot;test&quot;,
            &quot;columns&quot;:[
                {
                    &quot;name&quot;:&quot;name&quot;,
                    &quot;type&quot;:&quot;varchar&quot;
                },
                {
                    &quot;name&quot;:&quot;int_info&quot;,
                    &quot;type&quot;:&quot;int&quot;
                },
                {
                    &quot;name&quot;:&quot;double_info&quot;,
                    &quot;type&quot;:&quot;double&quot;
                }
            ],
            &quot;partition_pattern_format&quot;:&quot;yyyyMMdd&quot;,
            &quot;table_name&quot;:&quot;table name&quot;,
            &quot;password&quot;:&quot;password&quot;,
            &quot;db_name&quot;:&quot;test&quot;,
            &quot;table_schema&quot;:&quot;your table schema&quot;,
            &quot;connections&quot;:[
                {
                    &quot;db_url&quot;:&quot;jdbc:sqlserver://192.168.1.202:1433;databaseName=dts_test&quot;
                }
            ],
            &quot;partition_name&quot;:&quot;datetime&quot;
        }
    }
}
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div>`,29);function q(p,m){const e=d("RouterLink");return i(),a("div",null,[c,n("p",null,[s("上级文档: "),u(e,{to:"/zh/documents/connectors/Jdbc/jdbc.html"},{default:o(()=>[s("Jdbc-connector")]),_:1})]),v])}const k=t(r,[["render",q],["__file","jdbc-example.html.vue"]]);export{k as default};
