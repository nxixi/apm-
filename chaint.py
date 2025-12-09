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
from chain_transaction_new import chain_df
#from chain_transaction_with_global_id import chain_df
import warnings
warnings.filterwarnings("ignore")

import logging
from collections import defaultdict
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

    # def check_containment(row):
    #     src_dst = str(row['srcSysname'])
    #     dst_dst = str(row['dstSysname'])
    #     return 'F5' in src_dst and src_dst != dst_dst + '-F5'
    # containment_mask = result_df.apply(check_containment, axis=1)
    # best_matches = result_df[containment_mask]
    # best_matches.to_csv("result/not_fit.csv", index=False)

    # result_df_none_type = result_df[result_df['msgType'].isna()]
    # result_df_none_type.to_csv("result/result_df_none_type.csv", index=False)

    # result_df[(result_df['msgType'].isna()) & () & ()]
    result_df['src_ip_1'] = result_df['src_ip'].str.split(".").str[0]
    result_df['src_ip_2'] = result_df['dst_ip'].str.split(".").str[0]
    # result_df_filter = result_df[(result_df['src_ip_1']!=result_df['src_ip_2']) & (df['srcSysname'].str.contains('F5', na=False))]
    # result_df_filter.to_csv("result/result_df_filter.csv", index=False)
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
            # df.to_csv("result/data_1117.csv", index=False)
        df = pd.read_csv("data/data_20251117.csv")

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
        df = df.sort_values(by='start_at_ms').reset_index(drop=True)
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

# 读取起始条件
def load_root_conditions(file_path='root_conditions.txt'):
    """读取 root_conditions.txt 并返回系统和交易码的映射"""
    root_conditions = []
    system_to_msgTypes = defaultdict(set)
    all_msgTypes = set()
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    parts = line.split()
                    if len(parts) >= 2:
                        system, msgType = parts[0], parts[1]
                        root_conditions.append((system, msgType))
                        system_to_msgTypes[system].add(msgType)
                        all_msgTypes.add(msgType)
        
        logger.info(f"成功加载 {len(root_conditions)} 个起始条件")
    except Exception as e:
        logger.error(f"读取 root_conditions.txt 失败: {e}")
    
    return root_conditions, system_to_msgTypes, all_msgTypes

# 在程序启动时加载起始条件
ROOT_CONDITIONS, SYSTEM_TO_MSGTYPES, ALL_MSGTYPES = load_root_conditions()

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
        fac.AntdInputNumber(id='left-offset',value=0),
        fac.AntdText("right offset",strong=True,style={"color":"white"}),
        fac.AntdInputNumber(id='right-offset',value=0),
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
            {"title": "candidate_parents", "dataIndex": "候选父节点"},
            {"title": "parent_row_id", "dataIndex": "候选父节点_msg过滤"},
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
            {"title": "candidate_parents", "dataIndex": "候选父节点"},
            {"title": "parent_row_id", "dataIndex": "候选父节点_msg过滤"},
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
            fac.AntdInput(id='system-sysname', placeholder='please input sysname', style={'width':'350px'})
        ]),
        # fac.AntdCol([
        #     fac.AntdButton("查看链路",id="view-system-link",type='primary',icon=fac.AntdIcon(icon='antd-eye'),autoSpin=True)
        # ]),

    ])

    system_trace_result = html.Iframe(id="diagram-frame-system", style={"width": "100%","minHeight":'1000px',"height":'2000px',"border": 0})

    # 准备 Sysname 下拉框选项
    sysname_options = [{'label': sys, 'value': sys} for sys in sorted(SYSTEM_TO_MSGTYPES.keys())]
    
    # 准备所有 msgType 选项
    all_msgtype_options = [{'label': msg, 'value': msg} for msg in sorted(ALL_MSGTYPES)]
    
    msgType_select = fac.AntdRow([
        fac.AntdCol([
            fac.AntdSpace([
                fac.AntdText("Sysname: ",strong=True),
                fac.AntdSelect(
                    id='src-sysname',
                    placeholder='请选择系统',
                    style={'width':'350px'},
                    options=sysname_options,
                    allowClear=True
                ),
                fac.AntdText("msgType: ",strong=True),
                fac.AntdSelect(
                    id='src-msgType',
                    placeholder='请选择交易码',
                    style={'width':'350px'},
                    options=all_msgtype_options,
                    allowClear=True
                ),
                fac.AntdButton('查询', id="query-button",type='primary',size='large',style={'marginLeft': '10px'})
            ], style={'gap': '16px'})
        ])
    ])
    
    # 图结构统计和选择
    graph_stats_info = fac.AntdRow([
        fac.AntdCol([
            fac.AntdTag(id='graph-stats-tag',content='图结构统计: --', color='blue', icon=fac.AntdIcon(icon='antd-bar-chart')),
        ]),
    ], style={'marginTop': '16px'})
    
    graph_structure_select = fac.AntdRow([
        fac.AntdCol([
            fac.AntdSpace([
                fac.AntdText("选择图结构: ",strong=True),
                fac.AntdSelect(
                    id='graph-structure-select',
                    placeholder='请先查询，然后选择图结构',
                    style={'width':'600px'},
                    options=[],
                    disabled=True
                )
            ], style={'gap': '16px'})
        ])
    ], style={'marginTop': '16px'})

    msgType_trace_result = html.Iframe(
        id="diagram-frame-msgType", 
        style={
            "width": "100%",
            "height": "700px",
            "border": "1px solid #e5e7eb",
            "borderRadius": "6px",
            "overflow": "auto"
        }
    )
    
    msgType_single_graph_result = html.Iframe(
        id="diagram-frame-msgType-single", 
        style={
            "width": "100%",
            "height": "700px",
            "border": "1px solid #e5e7eb",
            "borderRadius": "6px",
            "overflow": "auto"
        }
    )


    # msgType 子 tabs：聚合图和单个图
    msgType_sub_tabs = fac.AntdTabs(
        id="msgType-sub-tabs",
        items=[
            {
                "key": "aggregate",
                "label": "聚合链路图",
                "children": [msgType_trace_result]
            },
            {
                "key": "single",
                "label": "单个图结构",
                "children": [msgType_single_graph_result]
            }
        ],
        defaultActiveKey="aggregate",
        style={"marginTop": "16px"},
        type='line'
    )
    
    tables_tabs = fac.AntdTabs(
        id="tabs-performance",
        items=[
            {"key":"apm","label":"APM","children":[apm_static,fac.AntdDivider(), apm_table]},
            {"key":"trace","label":"Trace","children":[trace_select, fac.AntdDivider(), trace_table, trace_result]},
            {"key":"sysname","label":"Sysname","children":[system_select, fac.AntdDivider(), system_trace_result]},
            {"key":"msgType","label":"msgType","children":[
                msgType_select, 
                graph_stats_info,
                graph_structure_select,
                fac.AntdDivider(),
                msgType_sub_tabs
            ]},
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
    "df": pd.DataFrame(),
    "graph_stats_df": pd.DataFrame(),
    "matching_graphs": pd.DataFrame(),
    "matching_trace_ids": [],
    "start": "",
    "end": ""
}

@app.callback(
    Output('src-msgType', 'options'),
    Input('src-sysname', 'value'),
    prevent_initial_call=False
)
def update_msgtype_options(selected_system):
    """根据选择的系统更新 msgType 下拉框选项"""
    if selected_system is None or selected_system == '':
        # 如果未选择系统，显示所有 msgType
        return [{'label': msg, 'value': msg} for msg in sorted(ALL_MSGTYPES)]
    else:
        # 如果选择了系统，显示该系统对应的 msgType
        msgTypes = SYSTEM_TO_MSGTYPES.get(selected_system, set())
        return [{'label': msg, 'value': msg} for msg in sorted(msgTypes)]


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
    State("left-offset", "value"),
    State("right-offset", "value"),
    prevent_initial_call=True,
)
def on_query(n_clicks, date_range, left_offset, right_offset):
    if date_range and len(date_range)==2:
        start = date_range[0]
        end = date_range[1]
    else:
        start = ""
        end = ""
    
    # 使用用户输入的offset值，如果为空则使用默认值0
    left_ms = left_offset if left_offset is not None else 0
    right_ms = right_offset if right_offset is not None else 0
    
    t1 = time.time()
    df = search_apm_data(start, end)
    t2 = time.time()
    data_nums = len(df)
    data_time = round(t2-t1,1)
    t1 = time.time()
    df, graph_stats_df = chain_df(df, left_ms=left_ms, right_ms=right_ms, output_prefix="chain_analysis",
                                  discard_mode='chain', candidate_method='downstream')
    df['msgType'] = df['msgType'].fillna('None')
    #logger.info(list(df['msgType'].unique()))
    msgTypes = list(df['msgType'].unique())
    #msgTypes.remove("")
    df.to_csv(f"result/PSISADP_test_1117.csv",index=False)
    #logger.info(df['msgType'].unique())
    cached["df"] = df
    cached["graph_stats_df"] = graph_stats_df  # 存储 graph_stats_df
    cached["start"] = start
    cached["end"] = end
    t2 = time.time()
    link_nums = len(df['trace_id'].unique())
    link_time = round(t2-t1,1)
    global_id_rate = round(len(df[~df['global_id'].isna()])/len(df) * 100, 1)
    global_id_nums = len(df['global_id'].unique())
    
    def convert_to_string(x):
        """将列表转换为字符串，处理各种边界情况"""
        if x is None or (isinstance(x, (list, tuple)) and len(x) == 0):
            return ''
        if isinstance(x, str):
            # 如果已经是字符串，直接返回
            return x
        if isinstance(x, (list, tuple)):
            # 展平嵌套列表
            result = []
            for i in x:
                if isinstance(i, (list, tuple)):
                    result.extend([str(j) for j in i])
                else:
                    result.append(str(i))
            return ','.join(result)
        return str(x)
    
    df['候选父节点'] = df['候选父节点'].apply(convert_to_string)
    df['候选父节点_msg过滤'] = df['候选父节点_msg过滤'].apply(convert_to_string)
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
        trace_df = df[df['trace_id']==int(trace_id)].copy()
        
        # 检查列的数据类型，只有在是列表类型时才转换
        # 如果已经是字符串，说明在第 660 行已经转换过了，不需要再次转换
        if len(trace_df) > 0:
            first_val = trace_df['候选父节点'].iloc[0]
            if isinstance(first_val, (list, tuple)):
                # 只有在是列表时才转换
                trace_df['候选父节点'] = trace_df['候选父节点'].apply(lambda x: ','.join([str(i) for i in x]) if isinstance(x, (list, tuple)) and len(x) > 0 else '')
            
            first_val_msg = trace_df['候选父节点_msg过滤'].iloc[0]
            if isinstance(first_val_msg, (list, tuple)):
                # 只有在是列表时才转换
                trace_df['候选父节点_msg过滤'] = trace_df['候选父节点_msg过滤'].apply(lambda x: ','.join([str(i) for i in x]) if isinstance(x, (list, tuple)) and len(x) > 0 else '')
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
    Input("system-sysname", "value"),
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
import pandas as pd
def filter_trans_from_entry_point(df, entry_point_indices):
    if df.empty:
        return df

    # 1. 确保入口集合类型匹配 (假设 ID 是字符串或数字，保持原样)
    entry_points_set = set(entry_point_indices)

    # 2. 性能优化：使用 zip 代替 iterrows 构建邻接表
    # 假设 'index' 和 'parent_row_id' 列存在且类型一致
    # 填充 NaN 为特殊值 (如 -1) 以防转换错误，或者确保数据预处理过
    ids = df['index'].values
    parent_ids = df['parent_row_id'].fillna(-1).values

    children_map = defaultdict(list)
    # 仅当 parent_id 有效时建立映射
    # 假设有效 ID 都是非负数，或者字符串。这里保留原逻辑 >= 0
    # 为了通用性，建议不强转 int，除非确定全是数字
    for child_id, parent_id in zip(ids, parent_ids):
        # 保持原逻辑：parent_id >= 0 视为有效父节点
        # 如果 ID 是字符串，这里比较逻辑需要调整 (例如 check if parent_id in valid_ids)
        try:
            pid = int(parent_id)
            cid = int(child_id)
            if pid >= 0:
                children_map[pid].append(cid)
        except (ValueError, TypeError):
            # 处理非数字 ID 的情况，或者直接跳过
            continue

    rows_to_keep = set()  # 使用 set 避免不同 trace 产生重复（虽然 trace ID 隔离了，但安全起见）

    # 3. 按 Trace 分组处理
    # 优化：不直接 groupby 遍历 DataFrame，而是获取 Trace ID 列表，逻辑更清晰
    # 或者为了利用 groupby 的便利性，保持原样，但逻辑要改

    # 这里的优化思路：
    # 只要找到所有的入口点，直接把它们扔进 BFS 队列即可。
    # 其实甚至不需要 groupby('trace_id')，因为 children_map 已经定义了连接关系。
    # 除非同一个 index ID 在不同 trace 中重复使用（这种情况很罕见，通常 index 是全局唯一的）。
    # 如果 index 是全局唯一的，我们甚至不需要 groupby。

    # 假设 index 全局唯一（或至少在处理上下文中唯一）：

    # 找出所有存在于当前 DF 中的入口点
    valid_entry_points = [uid for uid in ids if uid in entry_points_set]

    if not valid_entry_points:
        return pd.DataFrame(columns=df.columns)

    # 4. 逻辑修正：将所有入口点放入队列，不再筛选“最小深度”
    # 这样可以同时保留 Trace 中的多个分支 (A->B, A->C, 入口为 [B,C])
    queue = deque(valid_entry_points)

    # 用来记录 BFS 访问过的节点，防止死循环
    visited = set(valid_entry_points)

    # 最终结果集
    indices_to_keep = set(valid_entry_points)

    while queue:
        current_index = queue.popleft()

        # 查找子节点
        # 注意：需要保证 children_map 的 key 类型与 queue 中元素类型一致
        # 前面构建 map 时转了 int，这里也要转
        try:
            curr_int = int(current_index)
            children = children_map.get(curr_int, [])
        except:
            children = []

        for child in children:
            if child not in visited:
                visited.add(child)
                indices_to_keep.add(child)
                queue.append(child)

    # 5. 返回过滤后的 DataFrame
    if not indices_to_keep:
        return pd.DataFrame(columns=df.columns)

    result_df = df[df['index'].isin(indices_to_keep)].copy()
    result_df = result_df.reset_index(drop=True)

    return result_df


@app.callback(
    Output('diagram-frame-msgType', 'srcDoc'),
    Output('graph-stats-tag', 'content'),
    Output('graph-structure-select', 'options'),
    Output('graph-structure-select', 'disabled'),
    Output('graph-structure-select', 'value'),  # 添加：清空选择
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
        return dash.no_update, dash.no_update, [], True, dash.no_update

    try:
        # 1. 输入校验
        if not selected_system or not selected_msgType:
            logger.warning("System or Message Type not selected.")
            return dash.no_update, dash.no_update, [], True, dash.no_update

        df = cached['df']
        graph_stats_df = cached.get('graph_stats_df', pd.DataFrame())  # 获取 graph_stats_df
        start = cached['start']
        end = cached['end']

        # ---------------------------------------------------------------------
        # 2. 从 graph_stats_df 中过滤满足条件的图结构
        # ---------------------------------------------------------------------
        if graph_stats_df.empty or 'root_conditions' not in graph_stats_df.columns:
            logger.warning("graph_stats_df is empty or missing root_conditions column")
            html_doc = build_html_doc_system("graph TD; A[未找到图结构数据]", selected_system, start, end, 0)
            return html_doc, "图结构统计: 0个", [], True, None
        
        # 过滤满足条件的图结构
        matching_graphs = graph_stats_df[
            graph_stats_df['root_conditions'].apply(
                lambda x: any(
                    (cond[0] == selected_system and cond[1] == selected_msgType) 
                    for cond in x
                )
            )
        ].copy()
        
        if matching_graphs.empty:
            logger.info("No graph structures found matching the criteria.")
            html_doc = build_html_doc_system("graph TD; A[未找到符合条件的图结构]", selected_system, start, end, 0)
            return html_doc, "图结构统计: 0个", [], True, None
        
        # 统计信息
        total_graphs = len(matching_graphs)
        total_trace_ids = []
        for trace_id_list in matching_graphs['trace_id']:
            total_trace_ids.extend(trace_id_list)
        total_chains = len(total_trace_ids)
        
        logger.info(f"Found {total_graphs} graph structures with {total_chains} chains")
        
        # 准备下拉框选项
        select_options = []
        for idx, row in matching_graphs.iterrows():
            graph_id = row['graph_id']
            chain_count = len(row['trace_id'])
            node_count = row['节点数']
            edge_count = row['边数']
            label = f"图结构#{graph_id} - {chain_count}条链路, {node_count}节点, {edge_count}边"
            select_options.append({'label': label, 'value': graph_id})
        
        # 将匹配的图结构信息存入 cached，供后续使用
        cached['matching_graphs'] = matching_graphs
        cached['matching_trace_ids'] = total_trace_ids

        # ---------------------------------------------------------------------
        # 3. 聚合所有匹配的 trace_id，生成聚合链路图
        # ---------------------------------------------------------------------
        relevant_df = df[df['trace_id'].isin(total_trace_ids)].copy()
        
        if relevant_df.empty:
            html_doc = build_html_doc_system("graph TD; A[未找到对应的链路数据]", selected_system, start, end, 0)
            return html_doc, f"图结构统计: {total_graphs}个结构, {total_chains}条链路", select_options, False, None

        # 数据清洗和聚合
        relevant_df = relevant_df.fillna({'msgType': 'None'})

        # 聚合步骤
        new_trace_df = relevant_df.groupby(['trace_id', 'srcSysname', 'dstSysname', 'msgType'])[
            'start_at_ms'].min().reset_index().drop('trace_id', axis=1)
        dfs = new_trace_df.groupby(['srcSysname', 'dstSysname', 'msgType'])
        agg_trace_list = []

        # 聚合所有匹配的链路
        for k, v in dfs:
            agg_trace_list.append([k[0], k[1], k[2], v['start_at_ms'].min(), list(v['start_at_ms'])])

        agg_trace_df = pd.DataFrame(agg_trace_list, columns=['srcSysname', 'dstSysname', 'msgType', 'first_start_at_ms',
                                                             'start_at_ms_list'])

        # 排序和图表生成
        agg_trace_df['agg_trace_sort_priority'] = agg_trace_df['srcSysname'].str.contains('F5').astype(int)
        agg_trace_df = agg_trace_df.sort_values(by=['first_start_at_ms', 'agg_trace_sort_priority'], ascending=True)
        agg_trace_df = agg_trace_df.drop('agg_trace_sort_priority', axis=1)

        agg_trace_df = agg_trace_df.reset_index(drop=True)
        agg_trace_rows = agg_trace_df.to_dict('records')

        # 构建聚合 Mermaid 图和 HTML 文档
        mermaid_text = build_mermaid_from_rows(agg_trace_rows)
        title = f"{selected_system} - {selected_msgType} (聚合)"
        html_doc = build_html_doc_system(mermaid_text, title, start, end, total_chains)
        
        stats_text = f"图结构统计: {total_graphs}个结构, {total_chains}条链路"
        return html_doc, stats_text, select_options, False, None

    except Exception as e:
        logger.error(e)
        return dash.no_update, dash.no_update, [], True, None


@app.callback(
    Output('diagram-frame-msgType-single', 'srcDoc'),
    Input('graph-structure-select', 'value'),
    prevent_initial_call=False  # 允许初始调用，以便清空时也能触发
)
def on_graph_structure_select(selected_graph_id):
    """根据选择的图结构显示详细链路图"""
    if selected_graph_id is None:
        # 返回空白提示页面
        return """
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>请选择图结构</title>
    <style>
        body {
            margin: 0;
            padding: 0;
            display: flex;
            align-items: center;
            justify-content: center;
            height: 100vh;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background: #f5f7fa;
        }
        .message {
            text-align: center;
            color: #999;
            font-size: 16px;
        }
    </style>
</head>
<body>
    <div class="message">
        <p>请从上方下拉框中选择一个图结构</p>
    </div>
</body>
</html>
"""
    
    try:
        # 从 cached 中获取匹配的图结构
        matching_graphs = cached.get('matching_graphs', pd.DataFrame())
        df = cached.get('df', pd.DataFrame())
        start = cached.get('start', '')
        end = cached.get('end', '')
        
        if matching_graphs.empty or df.empty:
            return dash.no_update
        
        # 根据 graph_id 查找对应的行
        selected_rows = matching_graphs[matching_graphs['graph_id'] == selected_graph_id]
        if selected_rows.empty:
            logger.warning(f"Graph ID {selected_graph_id} not found in matching_graphs")
            return "<!DOCTYPE html><html><body><h3>未找到该图结构</h3></body></html>"
        
        selected_row = selected_rows.iloc[0]
        trace_ids = selected_row['trace_id']
        chain_count = len(trace_ids)
        
        logger.info(f"Selected graph structure #{selected_graph_id} with {chain_count} chains")
        
        # 获取该图结构的所有链路数据
        relevant_df = df[df['trace_id'].isin(trace_ids)].copy()
        
        if relevant_df.empty:
            return "<!DOCTYPE html><html><body><h3>未找到对应的链路数据</h3></body></html>"
        
        # 数据清洗和聚合
        relevant_df = relevant_df.fillna({'msgType': 'None'})
        
        # 聚合步骤
        new_trace_df = relevant_df.groupby(['trace_id', 'srcSysname', 'dstSysname', 'msgType'])[
            'start_at_ms'].min().reset_index().drop('trace_id', axis=1)
        dfs = new_trace_df.groupby(['srcSysname', 'dstSysname', 'msgType'])
        agg_trace_list = []
        
        for k, v in dfs:
            agg_trace_list.append([k[0], k[1], k[2], v['start_at_ms'].min(), list(v['start_at_ms'])])
        
        agg_trace_df = pd.DataFrame(agg_trace_list, columns=['srcSysname', 'dstSysname', 'msgType', 'first_start_at_ms',
                                                             'start_at_ms_list'])
        
        # 排序
        agg_trace_df['agg_trace_sort_priority'] = agg_trace_df['srcSysname'].str.contains('F5').astype(int)
        agg_trace_df = agg_trace_df.sort_values(by=['first_start_at_ms', 'agg_trace_sort_priority'], ascending=True)
        agg_trace_df = agg_trace_df.drop('agg_trace_sort_priority', axis=1)
        
        agg_trace_df = agg_trace_df.reset_index(drop=True)
        agg_trace_rows = agg_trace_df.to_dict('records')
        
        # 构建 Mermaid 图
        mermaid_text = build_mermaid_from_rows(agg_trace_rows)
        title = f"图结构 #{selected_graph_id}"
        html_doc = build_html_doc_system(mermaid_text, title, start, end, chain_count)
        
        return html_doc
    
    except Exception as e:
        logger.error(f"Error in graph structure selection: {e}")
        return dash.no_update


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5556, debug=True)