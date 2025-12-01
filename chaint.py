import numpy as np
import pandas as pd
import json
# import clickhouse_connect
from tqdm import tqdm
from bisect import bisect_left, bisect_right
#from chain_transaction import chain_df
from datetime import datetime, timedelta
import time
import dash
from dash import html, dcc, Output, Input, State
from dash import dash_table
import feffery_antd_components as fac
from chain_transaction import chain_df
#from chain_transaction_with_global_id import chain_df
import warnings
warnings.filterwarnings("ignore")

import logging 
logging.basicConfig(
    level=logging.INFO,
    format = '%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log',encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ck_host = '22.3.21.12'
# ck_port = 8123
# ck_user = 'default'
# ck_password = 'Ck_ai0ps'
# ck_database = 'ccdc'
# ck_table = "ods_apm_trace_detail_process"
# ck_client = clickhouse_connect.get_client(host=ck_host,port=ck_port,user=ck_user,password=ck_password)

def replace_f5_with_nearest_optimized(df):
    result_df = df.copy()
    f5_b_mask = result_df['dstSysname'].str.contains('F5',na=False)
    f5_a_mask = result_df['srcSysname'] == 'F5'
    if not f5_b_mask.any() or not f5_a_mask.any():
        return result_df
    df_a = df.loc[f5_a_mask, []].reset_index()
    df_b = df.loc[f5_b_mask, ['dstSysname', 'dst_ip']].reset_index()
    merged = pd.merge_asof(df_a, df_b, on='index', direction='nearest')
    update_data = merged.set_index('index')[['dstSysname', 'dst_ip']]
    update_data.rename(columns={'dstSysname':'srcSysname', 'dst_ip':'src_ip'},inplace=True)
    result_df.update(update_data)
    return result_df

def correct_f5(df, latency_threshold):
    df.reset_index(drop=True, inplace=True)
    result_df = df.copy()
    f5_mask = df[df['dstSysname'].str.contains('F5',na=False)]
    f5 = df[(df['srcSysname'].str.contains('F5',na=False))]
    f5_ = f5[f5.apply(lambda row: str(row['dstSysname']) not in str(row['srcSysname']), axis=1)]
    for idx,data in f5_.iterrows():
        current_timestamp = data['start_at_ms']
        current_msgType = data['msgType']
        current_dstSysname = data['dstSysname']
        select = f5_mask[(f5_mask['start_at_ms']>=current_timestamp-latency_threshold)&(f5_mask['start_at_ms']<=current_timestamp)&(f5_mask['msgType']==current_msgType)&(f5_mask['dstSysname'].str.contains(current_dstSysname))]

        if not select.empty:
            select = select.sort_values(by=['start_at_ms'])
            new_src_ip = select['dst_ip'].iloc[0]
            new_src_name = select['dstSysname'].iloc[0]
            result_df.at[idx, 'srcSysname']=new_src_name
            result_df.at[idx, 'src_ip']=new_src_ip
    return result_df

def update_f5_source_system_merge_asof(df, latency_threshold):
    result_df = df.copy()
    f5_dst_mask = result_df['dstSysname'].str.contains('F5',na=False)
    f5_src_mask = result_df['srcSysname'] == 'F5'
    f5_src_df = result_df.loc[f5_src_mask, ['start_at_ms', 'msgType']].reset_index()
    f5_dst_df = result_df.loc[f5_dst_mask, ['start_at_ms', 'msgType', 'dstSysname', 'dst_ip']]
    if f5_src_df.empty or f5_dst_df.empty:
        return result_df
    f5_src_df = f5_src_df.sort_values('start_at_ms')
    f5_dst_df = f5_dst_df.sort_values('start_at_ms')
    best_matches = pd.merge_asof(left=f5_src_df, right=f5_dst_df, on='start_at_ms', by='msgType', tolerance=latency_threshold, direction='backward')
    best_matches = best_matches.dropna(subset=['dstSysname'])
    if best_matches.empty:
        return result_df
    update_df = best_matches.set_index('index')[['dstSysname', 'dst_ip']]
    update_df.rename(columns={'dstSysname':'srcSysname', 'dst_ip':'src_ip'},inplace=True)
    result_df.update(update_df)
    return result_df

def update_f5_source_system_merge_asof3(df, latency_threshold):
    result_df = df.copy()
    f5_src_mask = result_df['srcSysname'] == 'F5'
    f5_dst_mask = result_df['dstSysname'].str.contains('F5',na=False)
    f5_src_df = result_df.loc[f5_src_mask, ['start_at_ms', 'msgType', 'dstSysname', 'dst_ip']].reset_index()
    f5_dst_df = result_df.loc[f5_dst_mask, ['start_at_ms', 'msgType', 'dstSysname', 'dst_ip']]
    if f5_src_df.empty or f5_dst_df.empty:
        return result_df
    
    f5_src_df['match_key'] = f5_src_df['dstSysname'] + '-F5'
    f5_dst_df['match_key'] = f5_dst_df['dstSysname']    
    del f5_src_df['dstSysname']
    
    f5_src_df['match_ip'] = f5_src_df['dst_ip'].str.split(".").str[0]
    f5_dst_df['match_ip'] = f5_dst_df['dst_ip'].str.split(".").str[0]    
    del f5_src_df['dst_ip']
    
    f5_src_df = f5_src_df.sort_values('start_at_ms')
    f5_dst_df = f5_dst_df.sort_values('start_at_ms')
    best_matches = pd.merge_asof(left=f5_src_df, right=f5_dst_df, on='start_at_ms', by=['msgType', 'match_key', 'match_ip'], tolerance=latency_threshold, direction='backward')
    best_matches = best_matches.dropna(subset=['dstSysname'])
    if best_matches.empty:
        return result_df
    update_df = best_matches.set_index('index')[['dstSysname', 'dst_ip']]
    update_df.rename(columns={'dstSysname':'srcSysname', 'dst_ip':'src_ip'},inplace=True)
    result_df.update(update_df)
    
    def check_containment(row):
        src_dst = str(row['srcSysname'])
        dst_dst = str(row['dstSysname'])
        return 'F5' in src_dst and src_dst != dst_dst + '-F5'
    containment_mask = result_df.apply(check_containment, axis=1)
    best_matches = result_df[containment_mask]
    best_matches.to_csv("not_fit.csv", index=False)

    result_df_none_type = result_df[result_df['msgType'].isna()]
    result_df_none_type.to_csv("result_df_none_type.csv", index=False)
    
    # result_df[(result_df['msgType'].isna()) & () & ()]
    result_df['src_ip_1'] = result_df['src_ip'].str.split(".").str[0]
    result_df['src_ip_2'] = result_df['dst_ip'].str.split(".").str[0]
    result_df_filter = result_df[(result_df['src_ip_1']!=result_df['src_ip_2']) & (df['srcSysname'].str.contains('F5', na=False))]
    result_df_filter.to_csv("result_df_filter.csv", index=False)
    del result_df['src_ip_1']
    del result_df['src_ip_2']
    

    return result_df

def search_apm_data(start, end, limit=5000000):
    if start=="" or end=="":
        logger.warning(f"【数据查询】未配置开始和结束时间！")
        return pd.DataFrame()
    else:
        start_timestamp = int(datetime.strptime(start,'%Y-%m-%d %H:%M:%S').timestamp()*1000)
        end_timestamp = int(datetime.strptime(end,'%Y-%m-%d %H:%M:%S').timestamp()*1000)
        # # sql = f"select msgType, src_ip, dst_ip, start_at_ms, latency_msec, global_id from ccdc.ods_apm_trace_detail_process_v2 where  `sync-flag` = '同步' and start_at_ms>={start_timestamp} and start_at_ms<={end_timestamp} limit {limit}"
        # sql = f"select msgType, src_ip, dst_ip, start_at_ms, latency_msec, global_id, sport, dport from ccdc.ods_apm_trace_detail_process_v2 where  latency_msec>0 and start_at_ms>={start_timestamp} and start_at_ms<={end_timestamp} limit {limit}"
        # logger.info(sql)
        # t1 = time.time()
        # df = ck_client.query_df(sql)
        # t2 = time.time()
        # logger.info(f"【数据查询】{start} ~ {end}共{len(df)}条APM数据（仅同步）, 耗时{round(t2 - t1,2)}s")
        # t3 = time.time()
        # ip_sql="select objectName,splitByString('-', objectName)[-2] sysname, properties['ip'] as ip , properties['businessIp'] as b_ip from adom.dwd_cmdb_object_all where objectType = 'System'"
        # ip_df = ck_client.query_df(ip_sql)
        # f5_ip_sql = "select objectName,properties['ip'] as ip from adom.dwd_cmdb_object_all where objectType = 'LoadBalancer'"
        # f5_ip_df = ck_client.query_df(f5_ip_sql)
        # f5_ip_df["sysname"] = "F5"
        # f5_ip_df["ip_type"] = "ip"
        # if len(ip_df) > 0:
            # ip_df = ip_df.melt(id_vars = ["objectName","sysname"],value_vars=["ip","b_ip"],var_name="ip_type",value_name='ip8').sort_values(by=["objectName","ip_type"])
            # ip_df = ip_df.rename(columns={"ip8":"ip"})
            # extra_ip = pd.read_csv("/app/eoi/jupyter_env/zsc/apm/trace_extract/EXTRA_IP.csv")
            # ip_df = pd.concat([ip_df, f5_ip_df, extra_ip],ignore_index=True)
            # with open('/app/eoi/jupyter_env/zsc/apm/trace_extract/F5.json', 'r', encoding='utf-8') as file:
            #     f5 = json.load(file)
            # df["srcSysname"] = df['src_ip'].map(dict(zip(ip_df['ip'], ip_df['sysname'])))
            # df["dstSysname"] = df['dst_ip'].map(dict(zip(ip_df['ip'], ip_df['sysname'])))
            # df['srcSysname']=df['src_ip'].map(f5).combine_first(df['srcSysname'])
            # df['dstSysname']=df['dst_ip'].map(f5).combine_first(df['dstSysname'])
            # df["sort_priority"] = df["srcSysname"].str.contains('F5',case=False,na=False)
            # df = df.sort_values(by=['start_at_ms','sort_priority'], ascending=[True,True])
            # del df["sort_priority"]
            # df.reset_index(drop=True, inplace=True)
            # f5_src_ip = list(df[df['srcSysname']=='F5']['src_ip'].unique())
            # f5_dst_ip = list(df[df['dstSysname']=='F5']['dst_ip'].unique())
            # t4 = time.time()
            # logger.info(f"【数据丰富】srcSysname为F5的src_ip有：{f5_src_ip}，dstSysname为F5的dst_ip有：{f5_dst_ip}，耗时{round(t4 - t3, 2)}s")
        t5 = time.time()
            # df['ori_src_ip'] = df['src_ip']
            # df['ori_dst_ip'] = df['dst_ip']
            #
            # df.to_csv("data_1117.csv", index=False)
        df = pd.read_csv("data_20251117.csv")

        df = update_f5_source_system_merge_asof3(df, 36000000)

        print(len(df))
        df = df[~((df['srcSysname'].str.contains('MQ'))|(df['dstSysname'].str.contains('MQ')))]
        print(len(df))

        #df = correct_f5(df, 10)
        #df = replace_f5_with_nearest_optimized(df)
        df['r_src_ip'] = df['src_ip']
        df['r_dst_ip'] = df['dst_ip']
        df['src_ip'] = df['ori_src_ip']
        df['dst_ip'] = df['ori_dst_ip']
        del df['ori_src_ip']
        del df['ori_dst_ip']
        f5_src_ip = list(df[df['srcSysname']=='F5']['src_ip'].unique())
        f5_dst_ip = list(df[df['dstSysname']=='F5']['dst_ip'].unique())
        t6 = time.time()
        logger.info(f"【F5处理】srcSysname为F5的src_ip有：{f5_src_ip}，dstSysname为F5的dst_ip有：{f5_dst_ip}，耗时{round(t6 - t5, 2)}s")
        df["index"] = df.index
        return df
        # else:
        #     logger.warning(f"【数据丰富】未获取到cmdb数据，丰富失败！")
        #     return pd.DataFrame()
        
def circled(n: int) -> str:
    #return chr(9311 + n) if 1 <= n <= 20 else f"{n}"
    return f"{n}."

def build_mermaid_from_rows(rows):
    pairs = {}
    for i, r in enumerate(rows, 1):
        s = (r.get("srcSysname", "").strip() or "UNKNOWN_SRC")
        d = (r.get("dstSysname", "").strip() or "UNKNOWN_DST")
        s_ip = (r.get("src_ip", "").strip() or "UNKNOWN_SRC_IP")
        d_ip = (r.get("dst_ip", "").strip() or "UNKNOWN_DST_IP")
        label = f"{circled(i)} {r.get('msgType','') or 'None'}"
        pairs.setdefault((s, d), []).append(label)
    lines = ["flowchart LR"]
    for (s, d), labels in pairs.items():
        lines.append(f"    {s} -->|{'<br/>'.join(labels)}| {d}")
        #lines.append(f"    {s}({s_ip}) -->|{'<br/>'.join(labels)}| {d}({d_ip})")
    return "\n".join(lines) + "\n"

def build_html_doc(mermaid_text: str, trace_id: int) -> str:
    return f"""
<!DOCTYPE html>
<html lang=\"zh-CN\"><head><meta charset=\"UTF-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
<title>链路识别</title>
<style>
  body{{margin:0;background:#f5f7fa;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,'Noto Sans','PingFang SC','Hiragino Sans GB','Microsoft YaHei',sans-serif}}
  .container{{padding:16px}}
  .card{{background:#ffffff;border:1px solid #e5e7eb;border-radius:12px;box-shadow:0 6px 24px rgba(15,23,42,0.08);padding:16px;position:relative;overflow:hidden}}
  .title{{text-align:center;font-weight:700;font-size:18px;margin:4px 0 12px;color:#0f172a;letter-spacing:.2px}}
  .mermaid{{background:transparent}}
  .mermaid svg {{ color:#0f172a; }}
  .mermaid svg .node rect {{ fill:#ffffff; stroke:#334155; stroke-width:1.2; }}
  .mermaid svg .node text {{ fill:#0f172a; font-weight:600; }}
  .mermaid svg .edgePath path {{ stroke:#334155; stroke-width:1.8; }}
  .mermaid svg .edgeLabel .label rect {{ fill:#ffffff; stroke:#cbd5e1; }}
  .mermaid svg .edgeLabel .label text {{ fill:#0f172a; font-weight:600; }}
</style>
</head><body>
<div class=\"container\"><div class=\"card\"><div class=\"title\">trace_id = {trace_id:} </div>
<div class=\"mermaid\">{mermaid_text}</div></div></div>
<script src="/assets/mermaid.min.js"></script>
<script>mermaid.initialize({{startOnLoad:true,theme:'default',securityLevel:'loose',flowchart:{{curve:'basis',rankSpacing:120,nodeSpacing:70}}}});</script>
</body></html>
"""

def build_html_doc_system(mermaid_text: str, sysname: str, start, end, trace_length) -> str:
    return f"""
<!DOCTYPE html>
<html lang=\"zh-CN\"><head><meta charset=\"UTF-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
<title>链路识别</title>
<style>
  body{{margin:0;background:#f5f7fa;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,'Noto Sans','PingFang SC','Hiragino Sans GB','Microsoft YaHei',sans-serif}}
  .container{{padding:16px}}
  .card{{background:#ffffff;border:1px solid #e5e7eb;border-radius:12px;box-shadow:0 6px 24px rgba(15,23,42,0.08);padding:16px;position:relative;overflow:hidden}}
  .title{{text-align:center;font-weight:700;font-size:18px;margin:4px 0 12px;color:#0f172a;letter-spacing:.2px}}
  .mermaid{{background:transparent}}
  .mermaid svg {{ color:#0f172a; }}
  .mermaid svg .node rect {{ fill:#ffffff; stroke:#334155; stroke-width:1.2; }}
  .mermaid svg .node text {{ fill:#0f172a; font-weight:600; }}
  .mermaid svg .edgePath path {{ stroke:#334155; stroke-width:1.8; }}
  .mermaid svg .edgeLabel .label rect {{ fill:#ffffff; stroke:#cbd5e1; }}
  .mermaid svg .edgeLabel .label text {{ fill:#0f172a; font-weight:600; }}
</style>
</head><body>
<div class=\"container\"><div class=\"card\"><div class=\"title\">  {sysname}({start} ~ {end[-8:]}, {trace_length}个Trace) </div>
<div class=\"mermaid\">{mermaid_text}</div></div></div>
<script src="/assets/mermaid.min.js"></script>
<script>mermaid.initialize({{startOnLoad:true,theme:'default',securityLevel:'loose',flowchart:{{curve:'basis',rankSpacing:120,nodeSpacing:70}}}});</script>
</body></html>
"""


# now_dt = datetime.now()
# start_dt = now_dt - timedelta(minutes=1)
now_dt = '2025-11-17 11:00:00'
start_dt ='2025-11-17 10:50:00'
        
def build_layout():
    # now_dt = datetime.now()
    # start_dt = now_dt - timedelta(minutes=1)
    query_bar = fac.AntdSpace([
        fac.AntdIcon(icon='antd-field-time', style={"color":'#fff', "fontSize":18}),
        fac.AntdText("时间范围",strong=True,style={"color":"white"}),
        fac.AntdDateRangePicker(
            id = "picker-range",
            showTime=True,
            format='YYYY-MM-DD HH:mm:ss',
            defaultValue=[
                # start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                # now_dt.strftime("%Y-%m-%d %H:%M:%S"),
                start_dt,
                now_dt,
            ],
            style={"width":420}
        ),
        fac.AntdText("left offset",strong=True,style={"color":"white"}),
        fac.AntdInputNumber(id='left-offset',value=10),
        fac.AntdText("right offset",strong=True,style={"color":"white"}),
        fac.AntdInputNumber(id='right-offset',value=10),
        fac.AntdButton("串联",id="btn-query",type='primary',icon=fac.AntdIcon(icon='antd-link'),autoSpin=True),
        #fac.AntdButton("串联",id="btn-link",type='primary',icon=fac.AntdIcon(icon='antd-link'),autoSpin=True),
    ], size=12, style={"padding":"8px 16px"})
    
    apm_static = fac.AntdRow([
        fac.AntdCol([
            fac.AntdTag(id='data-nums',content='查询数据--条', color='green', icon=fac.AntdIcon(icon='antd-check-circle')),
        ]),
        fac.AntdCol([
            fac.AntdTag(id='data-time',content='查询耗时--秒', color='green', icon=fac.AntdIcon(icon='antd-check-circle')),
        ]),
        fac.AntdCol([
            fac.AntdTag(id='link-nums',content='串联链路--条', color='green', icon=fac.AntdIcon(icon='antd-check-circle')),
        ]),
        fac.AntdCol([
            fac.AntdTag(id='link-time',content='串联耗时--秒', color='green', icon=fac.AntdIcon(icon='antd-check-circle')),
        ]),
        fac.AntdCol([
            fac.AntdTag(id='global-id-rate',content='global_id覆盖率--%', color='green', icon=fac.AntdIcon(icon='antd-check-circle')),
        ]),
        fac.AntdCol([
            fac.AntdTag(id='global-id-nums',content='global_id数量--个', color='green', icon=fac.AntdIcon(icon='antd-check-circle')),
        ]),
    ])
    
    
    apm_table = fac.AntdTable(
        id="apm-table",
        columns=[
            {"title": "index", "dataIndex": "index"},
            {"title": "start_at_ms", "dataIndex": "start_at_ms"},
            {"title": "msgType", "dataIndex": "msgType"},
            {"title": "src_ip", "dataIndex": "src_ip"},
            {"title": "dst_ip", "dataIndex": "dst_ip"},
            {"title": "r_src_ip", "dataIndex": "r_src_ip"},
            {"title": "r_dst_ip", "dataIndex": "r_dst_ip"},
            {"title": "srcSysname", "dataIndex": "srcSysname"},
            {"title": "dstSysname", "dataIndex": "dstSysname"},
            {"title": "latency_msec", "dataIndex": "latency_msec"},
            {"title": "global_id", "dataIndex": "global_id"},
            {"title": "sport", "dataIndex": "sport"},
            {"title": "dport", "dataIndex": "dport"},
            {"title": "trans_id", "dataIndex": "trans_id"},
            {"title": "probe_name", "dataIndex": "probe_name"},
            {"title": "parent_row_id", "dataIndex": "parent_row_id"},
            {"title": "trace_id", "dataIndex": "trace_id"},
            {"title": "depth", "dataIndex": "depth"},
        ],
        data=[],
        bordered=True,
        pagination={"pageSize": 10, 'showSizeChanger':True, "pageSizeOptions":[5,10,20,50,100]},
        maxWidth=1000,
        style={"width": "100%", "minHeight":"800px"},
    )
    
    trace_select = fac.AntdRow([
        fac.AntdCol([
            fac.AntdText("trace_id: ",strong=True),
            fac.AntdInput(id='trace-id', placeholder='please input trace_id', style={'width':'350px'})
        ]),
        # fac.AntdCol([
        #     fac.AntdButton("查看链路",id="view-link",type='primary',icon=fac.AntdIcon(icon='antd-eye'),autoSpin=True)
        # ]),
        
    ])
    
    trace_table = fac.AntdTable(
        id="trace-table",
        columns=[
            {"title": "index", "dataIndex": "index"},
            {"title": "start_at_ms", "dataIndex": "start_at_ms"},
            {"title": "msgType", "dataIndex": "msgType"},
            {"title": "src_ip", "dataIndex": "src_ip"},
            {"title": "dst_ip", "dataIndex": "dst_ip"},
            {"title": "r_src_ip", "dataIndex": "r_src_ip"},
            {"title": "r_dst_ip", "dataIndex": "r_dst_ip"},
            {"title": "srcSysname", "dataIndex": "srcSysname"},
            {"title": "dstSysname", "dataIndex": "dstSysname"},
            {"title": "latency_msec", "dataIndex": "latency_msec"},
            {"title": "global_id", "dataIndex": "global_id"},
            {"title": "sport", "dataIndex": "sport"},
            {"title": "dport", "dataIndex": "dport"},
            {"title": "trans_id", "dataIndex": "trans_id"},
            {"title": "probe_name", "dataIndex": "probe_name"},
            {"title": "parent_row_id", "dataIndex": "parent_row_id"},
            {"title": "trace_id", "dataIndex": "trace_id"},
            {"title": "depth", "dataIndex": "depth"},
        ],
        data=[],
        bordered=True,
        pagination={"pageSize": 5, 'showSizeChanger':True, "pageSizeOptions":[5,10,20,50,100]},
        maxWidth=1000,
        style={"width": "100%"},
    )
   
    trace_result = html.Iframe(id="diagram-frame", style={"width": "100%","minHeight":'1000px',"height":'2000px',"border": 0})
    
    system_select = fac.AntdRow([
        fac.AntdCol([
            fac.AntdText("Sysname: ",strong=True),
            fac.AntdInput(id='src-sysname', placeholder='please input sysname', style={'width':'350px'})
        ]),
        # fac.AntdCol([
        #     fac.AntdButton("查看链路",id="view-system-link",type='primary',icon=fac.AntdIcon(icon='antd-eye'),autoSpin=True)
        # ]),
        
    ])
    
    system_trace_result = html.Iframe(id="diagram-frame-system", style={"width": "100%","minHeight":'1000px',"height":'2000px',"border": 0})
    
    msgType_select = fac.AntdRow([
        fac.AntdCol([
            fac.AntdSpace([
                fac.AntdText("Sysname: ",strong=True),
                fac.AntdInput(id='src-sysname', placeholder='please input sysname', style={'width':'350px'}),
                fac.AntdText("msgType: ",strong=True),
                fac.AntdInput(id='src-msgType', placeholder='please input msgType', style={'width':'350px'}),
                fac.AntdButton('查询', id="query-button",type='primary',size='large',style={'marginLeft': '10px'})
            ], style={'gap': '16px'})
        ])
    ])
    
    msgType_trace_result = html.Iframe(id="diagram-frame-msgType", style={"width": "100%","minHeight":'1000px',"height":'2000px',"border": 0})
   
    
    tables_tabs = fac.AntdTabs(
        id="tabs-performance",
        items=[
            {"key":"apm","label":"APM","children":[apm_static,fac.AntdDivider(), apm_table]},
            {"key":"trace","label":"Trace","children":[trace_select, fac.AntdDivider(), trace_table, trace_result]},
            {"key":"sysname","label":"Sysname","children":[system_select, fac.AntdDivider(), system_trace_result]},
            {"key":"msgType","label":"msgType","children":[msgType_select, fac.AntdDivider(), msgType_trace_result]},
        ],
        defaultActiveKey="apm",
        style={"background":"#fff", "padding":12, "borderRadius":6, "boder":"1px solid #e5e7eb", "minHeight":"900px"},
        type='card'
    )
    
    return fac.AntdLayout([
        fac.AntdHeader(
            fac.AntdRow([
                #fac.AntdCol(fac.AntdText("性能指标查看", strong=True, style={"color":"#fff", "fontSize":18}))
                query_bar
            ]), style={"background":"#001529"}
        ),
        fac.AntdContent([
            #statics_card,
            tables_tabs
        ], style={"padding":16})
    ])

app = dash.Dash(__name__)
app.title = "交易链路串联"
app.layout = build_layout()

cached = {
    "df":pd.DataFrame()
}

@app.callback(
    Output("apm-table", "data",allow_duplicate=True),
    Output("btn-query", "loading"),
    Output("data-nums", "content"),
    Output("data-time", "content"),
    Output("link-nums", "content"),
    Output("link-time", "content"),
    Output("global-id-rate", "content"),
    Output("global-id-nums", "content"),
    # Output("msgType","options"),
    # Output("msgType","value"),
    Input("btn-query", "nClicks"),
    State("picker-range", "value"),
    prevent_initial_call=True,
)
def on_query(n_clicks, date_range):
    if date_range and len(date_range)==2:
        start = date_range[0]
        end = date_range[1]
    else:
        start = ""
        end = ""
    t1 = time.time()
    df = search_apm_data(start, end)
    t2 = time.time()
    data_nums = len(df)
    data_time = round(t2-t1,1)
    t1 = time.time()
    df = chain_df(df, left_ms = 0, right_ms = 0)
    df['msgType'] = df['msgType'].fillna('None')
    #logger.info(list(df['msgType'].unique()))
    msgTypes = list(df['msgType'].unique())
    #msgTypes.remove("")
    df.to_csv(f"PSISADP_test_1117.csv",index=False)
    #logger.info(df['msgType'].unique())
    cached["df"] = df
    cached["start"] = start
    cached["end"] = end
    t2 = time.time()
    link_nums = len(df['trace_id'].unique())
    link_time = round(t2-t1,1)
    global_id_rate = round(len(df[df['global_id']!=''])/len(df) * 100, 1)
    global_id_nums = len(df['global_id'].unique())
    return df[:1000].to_dict('records'), False, f"查询数据：{data_nums}条", f"查询耗时：{data_time}秒", f"串联链路：{link_nums}条", f"串联耗时：{link_time}秒",f"global_id覆盖率: {global_id_rate}%",f"global_id数量: {global_id_nums}"
# ,msgTypes,msgTypes[0]


@app.callback(
    Output("diagram-frame", "srcDoc"),
    Output("trace-table", "data"),
    #Output("view-link", "loading"),
    #Input("view-link", "nClicks"),
    Input("trace-id", "value"),
    prevent_initial_call=True,
)
def on_query(trace_id):
    try:
        
        trace_id = int(trace_id)
        df = cached["df"]
        trace_df = df[df['trace_id']==int(trace_id)]
        logger.info(f"{trace_id}: {len(trace_df)}")
        trace_rows = trace_df.to_dict('records')
        html_text = build_mermaid_from_rows(trace_rows)
        html_doc=build_html_doc(html_text, trace_id)
        return html_doc, trace_rows
    except Exception as e:
        logger.error(e)
        return dash.no_update, dash.no_update
    
@app.callback(
    Output("diagram-frame-system", "srcDoc"),
    #Output("trace-table", "data"),
    #Output("view-system-link", "loading"),
    #Input("view-system-link", "nClicks"),
    Input("src-sysname", "value"),
    prevent_initial_call=True,
)
def on_query(sysname):
    try:
        df = cached["df"]
        start = cached["start"]
        end = cached["end"]
        trace_ids = list(df[(df['srcSysname']==sysname)&(df['parent_row_id']==-1)]['trace_id'].unique())
        trace_length = len(trace_ids)
        logger.info(f"Find {sysname}下{trace_length}个trace")
        
        #target_trace = list(df[(df['srcSysname']=='PSISADP')&(df['parent_row_id']==-1)]['trace_id'].unique())
        relevant_df = df[df['trace_id'].isin(trace_ids)].copy()
        relevant_df['msgType'] = relevant_df['msgType'].fillna("None")
        merge_trace_df = (relevant_df.groupby(['trace_id','srcSysname','dstSysname','msgType'])['start_at_ms'].min().reset_index().drop('trace_id', axis=1))
        dfs = merge_trace_df.groupby(['srcSysname','dstSysname','msgType'])
        agg_trace_list = []
        for k,v in dfs:
            agg_trace_list.append({"srcSysname":k[0], "dstSysname":k[1], "msgType":k[2], "first_start_at_ms":v['start_at_ms'].min(), "start_at_ms_list":list[v['start_at_ms']]})
        agg_trace_df = pd.DataFrame(agg_trace_list)
        agg_trace_df['sort_priority'] =  agg_trace_df["srcSysname"].str.contains('F5').astype(int)
        agg_trace_df = agg_trace_df.sort_values(by=['first_start_at_ms','sort_priority']).drop('sort_priority', axis=1)
        #agg_trace_df["start_at_ms_list"] = agg_trace_df["start_at_ms_list"].apply(lambda t:', '.join(t))
        agg_trace_df.reset_index(drop=True, inplace=True)
       
        # agg_data_list = []
        # for trace_id in trace_ids:
        #     trace_df = df[df['trace_id']==int(trace_id)]
        #     trace_df['msgType'] = trace_df['msgType'].fillna("None")
        #     dfs = trace_df.groupby(['srcSysname','dstSysname',"msgType"])
        #     for k,v in dfs:
        #         first_start_at_ms = min(v["start_at_ms"])
        #         agg_data_list.append({'srcSysname':k[0], 'dstSysname':k[1], "msgType":k[2], "first_start_at_ms":first_start_at_ms})
        # agg_trace_df = pd.DataFrame(agg_data_list)
        agg_rows = agg_trace_df.to_dict('records')
        mermaid_text = build_mermaid_from_rows(agg_rows)
        html_doc = build_html_doc_system(mermaid_text, sysname, start, end, trace_length)
        return html_doc
    except Exception as e:
        logger.error(e)
        return dash.no_update
    
# @app.callback(
#     Output("diagram-frame-msgType", "srcDoc"),
#     Input("msgType", "value"),
#     prevent_initial_call=True,
# )
# def on_query(msgType):
#     try:
#         df = cached["df"]
#         start = cached["start"]
#         end = cached["end"]
#         trace_ids = list(df[(df['msgType']==msgType)]['trace_id'].unique())
#         trace_length = len(trace_ids)
#         logger.info(f"Find {msgType}下{trace_length}个trace")
        
#         relevant_df = df[df['trace_id'].isin(trace_ids)].copy()
#         relevant_df['msgType'] = relevant_df['msgType'].fillna("None")
#         merge_trace_df = (relevant_df.groupby(['trace_id','srcSysname','dstSysname','msgType'])['start_at_ms'].min().reset_index().drop('trace_id', axis=1))
#         dfs = merge_trace_df.groupby(['srcSysname','dstSysname','msgType'])
#         agg_trace_list = []
#         for k,v in dfs:
#             if k[2] == msgType:
#                 agg_trace_list.append({"srcSysname":k[0], "dstSysname":k[1], "msgType":k[2], "first_start_at_ms":v['start_at_ms'].min(), "start_at_ms_list":list[v['start_at_ms']]})
#         agg_trace_df = pd.DataFrame(agg_trace_list)
#         agg_trace_df['sort_priority'] =  agg_trace_df["srcSysname"].str.contains('F5').astype(int)
#         agg_trace_df = agg_trace_df.sort_values(by=['first_start_at_ms','sort_priority']).drop('sort_priority', axis=1)
#         agg_trace_df.reset_index(drop=True, inplace=True)
#         agg_rows = agg_trace_df.to_dict('records')
#         mermaid_text = build_mermaid_from_rows(agg_rows)
#         html_doc = build_html_doc_system(mermaid_text, msgType, start, end, trace_length)
#         return html_doc
#     except Exception as e:
#         logger.error(e)
        return dash.no_update
    
    
from collections import defaultdict, deque
def filter_trans_from_entry_point(df, entry_point_indices):
    if df.empty:
        return df
    
    entry_points_set = set(entry_point_indices)
    
    children_map = defaultdict(list)
    index_to_dfidx = {}
    
    for df_idx, row in df.iterrows():
        row_index = row['index']
        index_to_dfidx[row_index] = df_idx
        parent_id = row['parent_row_id']
        if parent_id >= 0:
            children_map[int(parent_id)].append(int(row_index))
    
    trace_groups = df.groupby('trace_id')
    
    rows_to_keep = []
    
    for trace_id, trace_df in trace_groups:
        if trace_id < 0:
            continue
            
        trace_indices = trace_df['index'].tolist()
        entry_candidates_in_trace = [idx for idx in trace_indices if idx in entry_points_set]
        
        if not entry_candidates_in_trace:
            continue
            
        if 'depth' in df.columns:
            candidate_depths = [(idx, df.loc[index_to_dfidx[idx], 'depth']) for idx in entry_candidates_in_trace]
            entry_index = min(candidate_depths, key=lambda x: x[1])[0]
        else:
            entry_index = entry_candidates_in_trace[0]
            
        indices_to_keep = set()
        queue = deque([entry_index])
        
        while queue:
            current_index = queue.popleft()
            indices_to_keep.add(current_index)
            
            for child_index in children_map.get(current_index, []):
                if child_index not in indices_to_keep:
                    queue.append(child_index)
        
        rows_to_keep.extend(indices_to_keep)
    
    if not rows_to_keep:
        return pd.DataFrame(columns=df.columns)
    
    result_df = df[df['index'].isin(rows_to_keep)].copy()
    result_df = result_df.reset_index(drop=True)
    
    return result_df
    
    
    
@app.callback(
    Output('diagram-frame-msgType', 'srcDoc'),
    # 监听查询按钮的点击次数 (Input)
    Input('query-button', 'nClicks'),
    # 获取系统下拉框的值 (State)
    State('src-sysname', 'value'),
    # 获取交易类型下拉框的值 (State)
    State('src-msgType', 'value'),
    prevent_initial_call=True
)
# 注意：您需要在前端定义这些新的 Dash 组件
def on_query(n_clicks, selected_system, selected_msgType):
    # 确保只有在点击按钮后才执行查询
    if n_clicks is None:
        return dash.no_update

    try:
        # 1. 输入校验
        if not selected_system or not selected_msgType:
            logger.warning("System or Message Type not selected.")
            return dash.no_update

        df = cached['df']
        start = cached['start']
        end = cached['end']

        # ---------------------------------------------------------------------
        # 2. 新的核心过滤逻辑: 找到符合条件的节点
        # ---------------------------------------------------------------------
        root_nodes_criteria = (
                # (df['parent_row_id'] == -1) &  # 必须是根节点
                (df['srcSysname'] == selected_system + '-F5') &  # 源系统必须匹配
                (df['msgType'] == selected_msgType)  # 交易类型必须匹配
        )

        # 提取这些根节点对应的所有 trace_id
        trace_ids = list(df[root_nodes_criteria]['trace_id'].unique())

        if not trace_ids:
            logger.info("No traces found matching the root node criteria.")
            # 如果没有找到链路，返回一个空图表的 HTML
            html_doc = build_html_doc_system("graph TD; A[未找到符合条件的链路]", selected_system, start, end, 0)
            return html_doc

        trace_length = len(trace_ids)
        logger.info(f"Find relevant trace(trace_length): {trace_length} (trace)")

        # ---------------------------------------------------------------------
        # 3. 完整链路数据提取: 获取这些 trace_id 对应的所有数据行
        # (不限制后续数据的 msgType)
        # ---------------------------------------------------------------------
        relevant_df = df[df['trace_id'].isin(trace_ids)].copy()
        
        relevant_df = filter_trans_from_entry_point(relevant_df, list(df[root_nodes_criteria]['index']))

        # 数据清洗和聚合 (保留原逻辑，操作新的 relevant_df)
        relevant_df = relevant_df.fillna({'msgType': 'None'})

        # 聚合步骤
        new_trace_df = relevant_df.groupby(['trace_id', 'srcSysname', 'dstSysname', 'msgType'])[
            'start_at_ms'].min().reset_index().drop('trace_id', axis=1)
        dfs = new_trace_df.groupby(['srcSysname', 'dstSysname', 'msgType'])
        agg_trace_list = []

        # 只选择 符合条件的节点 后面的路径
        for k, v in dfs:
            # if k[2] == selected_msgType:
                # 假设 k[0] 是 srcSysname, k[1] 是 dstSysname, k[2] 是 msgType
                agg_trace_list.append([k[0], k[1], k[2], v['start_at_ms'].min(), list(v['start_at_ms'])])

        agg_trace_df = pd.DataFrame(agg_trace_list, columns=['srcSysname', 'dstSysname', 'msgType', 'first_start_at_ms',
                                                             'start_at_ms_list'])

        # 排序和图表生成
        agg_trace_df['agg_trace_sort_priority'] = agg_trace_df['srcSysname'].str.contains('F5').astype(int)
        # 注意: 修复了原代码中 sort_values 列名 'sort_priority' 的错误
        agg_trace_df = agg_trace_df.sort_values(by=['first_start_at_ms', 'agg_trace_sort_priority'], ascending=True)
        agg_trace_df = agg_trace_df.drop('agg_trace_sort_priority', axis=1)

        agg_trace_df = agg_trace_df.reset_index(drop=True)
        agg_trace_rows = agg_trace_df.to_dict('records')

        # 构建 Mermaid 图和 HTML 文档
        mermaid_text = build_mermaid_from_rows(agg_trace_rows)
        # 传入 selected_system 作为额外的文档参数
        html_doc = build_html_doc_system(mermaid_text, selected_system, start, end, trace_length)
        return html_doc

    except Exception as e:
        logger.error(e)
        return dash.no_update
    

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5556, debug=True)