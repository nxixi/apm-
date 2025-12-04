import numpy as np
import pandas as pd
import json
from tqdm import tqdm
from bisect import bisect_left, bisect_right
from datetime import datetime, timedelta
import time
import dash
from dash import html, dcc, Output, Input, State
import feffery_antd_components as fac
from chain_transaction import chain_df  # 假设这个函数返回带有父子关系的DataFrame
import warnings
from typing import List, Dict, Set, Tuple, Literal
from collections import deque, defaultdict
from funcs import *

warnings.filterwarnings("ignore")

import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# --- Mermaid Helper Functions (更新以支持更多信息) ---

# 保持其他辅助函数（如 update_f5_source_system_merge_asof3, search_apm_data, circled, build_mermaid_from_rows 等）不变...

def build_html_doc(mermaid_text: str, choose_index: int, tooltip_data: Dict = None) -> str:
    """
    构建包含Mermaid图和tooltip功能的HTML文档
    
    Parameters:
        mermaid_text: Mermaid图的文本定义
        trace_id: 链路ID
        tooltip_data: 节点详细信息字典 {node_id: {key: value}}
    """
    # 将tooltip_data转换为JSON字符串，用于JavaScript
    import json
    tooltip_json = json.dumps(tooltip_data or {})
    
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
  
  /* 起始节点高亮样式 */
  .mermaid svg .node.start-node rect {{
    fill: #fef3c7 !important;
    stroke: #f59e0b !important;
    stroke-width: 2.5px !important;
  }}
  .mermaid svg .node.start-node text {{
    fill: #92400e !important;
    font-weight: 700 !important;
  }}
  
  /* Tooltip 样式 */
  .node-tooltip {{
    position: absolute;
    background: rgba(0, 0, 0, 0.9);
    color: #fff;
    padding: 12px 16px;
    border-radius: 8px;
    font-size: 13px;
    line-height: 1.6;
    pointer-events: none;
    z-index: 9999;
    box-shadow: 0 4px 12px rgba(0,0,0,0.3);
    max-width: 400px;
    white-space: nowrap;
    display: none;
  }}
  .node-tooltip .tooltip-row {{
    margin: 2px 0;
  }}
  .node-tooltip .tooltip-label {{
    color: #94a3b8;
    font-weight: 600;
    margin-right: 8px;
  }}
  .node-tooltip .tooltip-value {{
    color: #e2e8f0;
  }}
</style>
</head><body>
<div class=\"container\"><div class=\"card\"><div class=\"title\">choose_index = {choose_index} </div>
<div class=\"mermaid\">{mermaid_text}</div></div></div>
<div id="tooltip" class="node-tooltip"></div>
<script src="/assets/mermaid.min.js"></script>
<script>
  const tooltipData = {tooltip_json};
  
  mermaid.initialize({{
    startOnLoad: true,
    theme: 'default',
    securityLevel: 'loose',
    flowchart: {{
      curve: 'basis',
      rankSpacing: 120,
      nodeSpacing: 70
    }}
  }});
  
  // 等待Mermaid渲染完成后添加tooltip事件和高亮起始节点
  setTimeout(() => {{
    const tooltip = document.getElementById('tooltip');
    const nodes = document.querySelectorAll('.node');
    
    console.log('找到节点数量:', nodes.length);
    console.log('tooltipData keys:', Object.keys(tooltipData));
    
    nodes.forEach(node => {{
      // Mermaid可能使用 'flowchart-N_xx-xx' 这样的格式
      let nodeId = node.id;
      console.log('原始节点ID:', nodeId);
      
      // 尝试提取真实的节点ID (支持多种格式)
      let actualId = nodeId;
      
      // 尝试匹配模式: flowchart-N_xx-xx 或 N_xx
      const match = nodeId.match(/N_(\d+)/);
      if (match) {{
        actualId = 'N_' + match[1];
      }}
      
      console.log('匹配后的ID:', actualId, '是否有数据:', !!tooltipData[actualId]);
      
      const data = tooltipData[actualId];
      
      if (data) {{
        // 如果是起始节点，添加高亮样式类
        if (data.is_start) {{
          node.classList.add('start-node');
        }}
        
        node.style.cursor = 'pointer';
        
        node.addEventListener('mouseenter', (e) => {{
          const tooltipHTML = `
            <div class="tooltip-row"><span class="tooltip-label">Index:</span><span class="tooltip-value">${{data.index}}</span></div>
            <div class="tooltip-row"><span class="tooltip-label">Src System:</span><span class="tooltip-value">${{data.srcSysname}}</span></div>
            <div class="tooltip-row"><span class="tooltip-label">Dst System:</span><span class="tooltip-value">${{data.dstSysname}}</span></div>
            <div class="tooltip-row"><span class="tooltip-label">Src IP:</span><span class="tooltip-value">${{data.r_src_ip}}</span></div>
            <div class="tooltip-row"><span class="tooltip-label">Dst IP:</span><span class="tooltip-value">${{data.r_dst_ip}}</span></div>
            <div class="tooltip-row"><span class="tooltip-label">Start:</span><span class="tooltip-value">${{data.start_at_ms}}</span></div>
            <div class="tooltip-row"><span class="tooltip-label">End:</span><span class="tooltip-value">${{data.end_at_ms}}</span></div>
            <div class="tooltip-row"><span class="tooltip-label">Latency:</span><span class="tooltip-value">${{data.latency_msec}}ms</span></div>
            <div class="tooltip-row"><span class="tooltip-label">MsgType:</span><span class="tooltip-value">${{data.msgType}}</span></div>
            <div class="tooltip-row"><span class="tooltip-label">GlobalID:</span><span class="tooltip-value">${{data.global_id}}</span></div>
          `;
          
          tooltip.innerHTML = tooltipHTML;
          tooltip.style.display = 'block';
        }});
        
        node.addEventListener('mousemove', (e) => {{
          tooltip.style.left = (e.pageX + 15) + 'px';
          tooltip.style.top = (e.pageY + 15) + 'px';
        }});
        
        node.addEventListener('mouseleave', () => {{
          tooltip.style.display = 'none';
        }});
      }} else {{
        console.warn('节点没有数据:', actualId);
      }}
    }});
  }}, 1000);
</script>
</body></html>
"""


def build_html_doc_system(mermaid_text: str, sysname: str, start, end, trace_length) -> str:
    # 保持不变，用于通用 HTML 包装
    # ... (保持原函数内容不变)
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


def build_mermaid_from_graph_data(nodes_data: List[Dict], edges_data: List[Dict]) -> Tuple[str, Dict]:
    """
    将 APMTraceFinder 返回的图形数据转换为 Mermaid 流程图文本，
    节点标签只显示 srcSysname，详细信息存储在 tooltip_data 中。
    
    Returns:
        Tuple[str, Dict]: (mermaid_text, tooltip_data)
    """

    def sanitize_label(text: str) -> str:
        """清理标签中的特殊字符，避免 Mermaid 语法错误。"""
        if text is None or text == "":
            return "None"
        text = str(text).replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;')
        return text

    node_map = {}
    tooltip_data = {}  # 存储每个节点的详细信息用于tooltip
    
    for node in nodes_data:
        node_id = node['id']
        data = node['data']
        is_start = data.get('is_start', False)

        # 格式化时间戳为可读格式
        start_time = datetime.fromtimestamp(data['start_at_ms'] / 1000).strftime('%H:%M:%S.%f')[:-3]
        end_time = datetime.fromtimestamp(data['end_at_ms'] / 1000).strftime('%H:%M:%S.%f')[:-3]

        # 节点显示：优先使用 srcSysname，如果是 None、空或以"N_"开头，则使用 dstSysname
        src_sysname = data.get('srcSysname', '')
        dst_sysname = data.get('dstSysname', '')
        
        # 判断 srcSysname 是否有效
        if pd.isna(src_sysname) or src_sysname == '' or str(src_sysname).startswith('N_'):
            # 尝试使用 dstSysname
            if pd.notna(dst_sysname) and dst_sysname != '' and not str(dst_sysname).startswith('N_'):
                display_name = sanitize_label(dst_sysname)
            else:
                # 都无效时，显示索引
                display_name = f"Node_{node_id}"
        else:
            display_name = sanitize_label(src_sysname)
        
        # 不再添加 [START] 标记，改用背景高亮
        label = display_name

        node_map[node_id] = {'label': label, 'is_start': is_start}
        
        # 存储详细信息用于tooltip
        tooltip_data[f"N_{node_id}"] = {
            'index': node_id,
            'is_start': is_start,  # 记录是否为起始节点
            'srcSysname': sanitize_label(data.get('srcSysname', 'N/A')),
            'dstSysname': sanitize_label(data.get('dstSysname', 'N/A')),
            'r_src_ip': sanitize_label(data.get('r_src_ip', 'N/A')),
            'r_dst_ip': sanitize_label(data.get('r_dst_ip', 'N/A')),
            'start_at_ms': start_time,
            'end_at_ms': end_time,
            'latency_msec': data.get('latency_msec', 'N/A'),
            'msgType': sanitize_label(data.get('msgType', 'N/A')),
            'global_id': sanitize_label(data.get('global_id', 'N/A'))
        }

    # 聚合边关系
    pairs = defaultdict(list)
    for edge in edges_data:
        src = edge['source']
        dst = edge['target']
        label = edge.get('label', 'Call')
        pairs.setdefault((src, dst), []).append(sanitize_label(label))

    lines = ["flowchart LR"]

    # 显式定义节点
    for node_id, data in node_map.items():
        safe_id = f"N_{node_id}"
        lines.append(f'    {safe_id}["{data["label"]}"]')

    # 绘制边
    for (src_id, dst_id), labels in pairs.items():
        # 过滤掉空标签
        valid_labels = [l for l in labels if l and l.strip()]
        
        safe_src_id = f"N_{src_id}"
        safe_dst_id = f"N_{dst_id}"
        
        if valid_labels:
            # 有有效标签时显示标签
            label_text = '<br/>'.join(sorted(list(set(valid_labels))))
            lines.append(f"    {safe_src_id} -->|\"{label_text}\"| {safe_dst_id}")
        else:
            # 无标签时只显示箭头
            lines.append(f"    {safe_src_id} --> {safe_dst_id}")

    return "\n".join(lines) + "\n", tooltip_data


# --- Dash Layout (简化和新增 Candidate Trace) ---

now_dt = '2025-11-17 10:51:00'
start_dt = '2025-11-17 10:50:00'


def build_layout():
    # ... (查询栏 query_bar 保持不变)
    query_bar = fac.AntdSpace([
        fac.AntdIcon(icon='antd-field-time', style={"color": '#fff', "fontSize": 18}),
        fac.AntdText("时间范围", strong=True, style={"color": "white"}),
        fac.AntdDateRangePicker(
            id="picker-range",
            showTime=True,
            format='YYYY-MM-DD HH:mm:ss',
            defaultValue=[start_dt, now_dt, ],
            style={"width": 420}
        ),
        fac.AntdText("left offset", strong=True, style={"color": "white"}),
        fac.AntdInputNumber(id='left-offset', value=10),
        fac.AntdText("right offset", strong=True, style={"color": "white"}),
        fac.AntdInputNumber(id='right-offset', value=10),
        fac.AntdButton("串联", id="btn-query", type='primary', icon=fac.AntdIcon(icon='antd-link'), autoSpin=True),
    ], size=12, style={"padding": "8px 16px"})

    # ... (APM 统计和表格 apm_static, apm_table 保持不变)
    apm_static = fac.AntdRow([
        fac.AntdCol([fac.AntdTag(id='data-nums', content='查询数据--条', color='green',
                                 icon=fac.AntdIcon(icon='antd-check-circle'))]),
        fac.AntdCol([fac.AntdTag(id='data-time', content='查询耗时--秒', color='green',
                                 icon=fac.AntdIcon(icon='antd-check-circle'))]),
        # fac.AntdCol([fac.AntdTag(id='link-nums', content='串联链路--条', color='green',
        #                          icon=fac.AntdIcon(icon='antd-check-circle'))]),
        fac.AntdCol([fac.AntdTag(id='link-time', content='串联耗时--秒', color='green',
                                 icon=fac.AntdIcon(icon='antd-check-circle'))]),
        fac.AntdCol([fac.AntdTag(id='global-id-rate', content='global_id覆盖率--%', color='green',
                                 icon=fac.AntdIcon(icon='antd-check-circle'))]),
        fac.AntdCol([fac.AntdTag(id='global-id-nums', content='global_id数量--个', color='green',
                                 icon=fac.AntdIcon(icon='antd-check-circle'))]),
    ])

    # 使用所有列定义表格
    apm_table_cols = [
        {"title": col, "dataIndex": col} for col in [
            "index", "start_at_ms", "end_at_ms", "latency_msec", "msgType", "global_id",
            "srcSysname", "dstSysname", "r_src_ip", "r_dst_ip",
            "candidate_parents", "candidate_children", "same_msgtype_parents",
            "same_msgtype_children", "same_globalid_parents", "same_globalid_children"
        ]
    ]

    apm_table = fac.AntdTable(id="apm-table", columns=apm_table_cols, data=[], bordered=True,
                              pagination={"pageSize": 10, 'showSizeChanger': True,
                                          "pageSizeOptions": [5, 10, 20, 50, 100]}, maxWidth=1000,
                              style={"width": "100%", "minHeight": "800px"})

    # --- 新增：Candidate Trace Tab ---

    candidate_trace_controls = fac.AntdSpace([
        fac.AntdText("选择索引 (Index):", strong=True),
        fac.AntdInputNumber(id='start-index-input', min=0, placeholder='输入起始行index', style={'width': 120}),

        fac.AntdText("查找模式:", strong=True),
        fac.AntdRadioGroup(
            id='trace-type-radio',
            options=[
                {'label': '模式 1 (严格路径)', 'value': '1'},
                {'label': '模式 2 (完整子图)', 'value': '2'}
            ],
            defaultValue='1',
            optionType='button',
            buttonStyle='solid'
        ),

        fac.AntdDivider(direction='vertical'),

        fac.AntdCheckboxGroup(
            id='relation-type-checkbox',
            options=[
                {'label': 'MsgType 关联', 'value': 'msgType'},
                {'label': 'GlobalID 关联', 'value': 'global_id'}
            ],
            value=[],
            # 设置按钮样式，不可同时选中通过回调实现
            style={'display': 'inline-flex', 'gap': '10px'}
        ),

        fac.AntdDivider(direction='vertical'),

        fac.AntdText("向上步长:", strong=True),
        fac.AntdInputNumber(id='max-up-steps-input', min=-1, placeholder='-1=不限制', value=-1, style={'width': 100}),
        
        fac.AntdText("向下步长:", strong=True),
        fac.AntdInputNumber(id='max-down-steps-input', min=-1, placeholder='-1=不限制', value=-1, style={'width': 100}),

        fac.AntdButton("查询并绘制链路", id="btn-candidate-trace", type='primary',
                       icon=fac.AntdIcon(icon='antd-compass'), autoSpin=False),

    ], size=4, style={"padding": "16px 0"})

    candidate_trace_tab = [
        candidate_trace_controls,
        fac.AntdDivider(),
        html.Iframe(id="diagram-frame-candidate",
                    style={"width": "100%", "minHeight": '1000px', "height": '2000px', "border": 0})
    ]

    # --- Tabs 定义 (只保留 APM 和 Candidate Trace) ---
    tables_tabs = fac.AntdTabs(
        id="tabs-performance",
        items=[
            {"key": "apm", "label": "APM", "children": [apm_static, fac.AntdDivider(), apm_table]},
            {"key": "candidate-trace", "label": "Candidate Trace", "children": candidate_trace_tab},
        ],
        defaultActiveKey="apm",
        style={"background": "#fff", "padding": 12, "borderRadius": 6, "boder": "1px solid #e5e7eb",
               "minHeight": "900px"},
        type='card'
    )

    return fac.AntdLayout([
        fac.AntdHeader(fac.AntdRow([query_bar]), style={"background": "#001529"}),
        fac.AntdContent([tables_tabs], style={"padding": 16})
    ])


# --- Dash App Initialization ---
# ... (保持不变)
app = dash.Dash(__name__)
app.title = "交易链路串联"
app.layout = build_layout()

cached = {
    "df": pd.DataFrame()
}


# --- Callbacks ---
# ... (on_query 保持不变，但需确保在 chain_df 结果中添加 'end_at_ms')

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
        df['end_at_ms'] = df['start_at_ms'] + df['latency_msec']
        df = df[(df['start_at_ms'] >= start_timestamp) & (df['end_at_ms'] <= end_timestamp)]

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

@app.callback(
    Output("apm-table", "data", allow_duplicate=True),
    Output("btn-query", "loading"),
    Output("data-nums", "content"),
    Output("data-time", "content"),
    # Output("link-nums", "content"),
    Output("link-time", "content"),
    Output("global-id-rate", "content"),
    Output("global-id-nums", "content"),
    Input("btn-query", "nClicks"),
    State("picker-range", "value"),
    State("left-offset", "value"),
    State("right-offset", "value"),
    prevent_initial_call=True,
)
def on_query(n_clicks, date_range, left_offset, right_offset):
    if date_range and len(date_range) == 2:
        start = date_range[0]
        end = date_range[1]
    else:
        start = ""
        end = ""
    t1 = time.time()
    df = search_apm_data(start, end)
    t2 = time.time()
    data_nums = len(df)
    data_time = round(t2 - t1, 1)

    t1 = time.time()
    df['end_at_ms'] = df['start_at_ms'] + df['latency_msec']
    linker = APMDataLinker(left_offset=left_offset, right_offset=right_offset)
    df = linker.link_apm_data(df)[
        ['latency_msec', 'msgType', 'global_id', 'start_at_ms', 'end_at_ms', 'srcSysname', 'dstSysname', 'r_src_ip',
         'r_dst_ip', 'index', 'candidate_parents', 'same_msgtype_parents', 'same_globalid_parents',
         'candidate_children', 'same_msgtype_children', 'same_globalid_children']]
    df.to_csv("result/candidate_result_20251117_10min.csv", index=False)
    # df = pd.read_csv("candidate_result_20251117_1min.csv")
    df['msgType'] = df['msgType'].fillna('None')
    # 确保子节点列是字符串，便于查找器使用
    for col in ['candidate_children', 'candidate_parents', 'same_msgtype_children', 'same_msgtype_parents',
                'same_globalid_children', 'same_globalid_parents']:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: str(x) if pd.notna(x) and x != -1 else '-1')

    cached["df"] = df
    cached["start"] = start
    cached["end"] = end
    t2 = time.time()
    link_time = round(t2 - t1, 1)

    # 修复原 global_id 覆盖率计算：使用 fillna('') != ''
    global_id_rate = round(len(df[df['global_id'].fillna('') != '']) / len(df) * 100, 1)
    global_id_nums = len(df['global_id'].unique())

    return df[:1000].to_dict(
        'records'), False, f"查询数据：{data_nums}条", f"查询耗时：{data_time}秒", f"串联耗时：{link_time}秒", f"global_id覆盖率: {global_id_rate}%", f"global_id数量: {global_id_nums}"


# --- 新增/修改 Callback: Candidate Trace View ---

@app.callback(
    Output('relation-type-checkbox', 'value'),
    Input('relation-type-checkbox', 'value'),
    State('relation-type-checkbox', 'value'),
    prevent_initial_call=True
)
def limit_single_checkbox(current_values, state_values):
    """确保 msgType 和 global_id 只能选中一个"""
    # 如果用户尝试选中第二个，我们只保留最新的一个
    if len(current_values) > 1:
        # 确定哪个是新选中的
        newly_selected = [v for v in current_values if v not in state_values]
        if newly_selected:
            return newly_selected[:1]  # 只保留最新的那个
        else:
            # 这通常不应该发生，但作为回退，只保留第一个
            return current_values[:1]
    return current_values


@app.callback(
    # 输出：[图表HTML内容, 按钮加载状态]
    Output('diagram-frame-candidate', 'srcDoc'),
    Output('btn-candidate-trace', 'loading'),

    # 输入：只有按钮点击才会触发
    Input('btn-candidate-trace', 'nClicks'),

    # 状态：点击按钮时读取所有参数
    State('start-index-input', 'value'),
    State('trace-type-radio', 'value'),
    State('relation-type-checkbox', 'value'),
    State('max-up-steps-input', 'value'),
    State('max-down-steps-input', 'value'),
    prevent_initial_call=True
)
def on_find_candidate_trace(n_clicks, start_index, trace_type_str, relation_checkbox_list, max_up_steps, max_down_steps):
    # 1. 初始化检查和触发检查
    # 只有当 n_clicks 有效时才继续（因为 n_clicks 是唯一的 Input）
    if n_clicks is None or n_clicks < 1:
        return dash.no_update, False

    # 【重要修改】移除立即返回 loading=True 的逻辑
    # 因为它会导致后面的查询代码被跳过。
    # 加载状态的 True/False 必须在函数开始/结束时，通过其他机制或通过函数末尾的返回统一处理。

    # 2. 参数验证（在查询开始前检查必要参数）
    if start_index is None or trace_type_str is None:
        error_mermaid = "graph TD; A[请输入起始索引和链路类型]"
        # 立即返回错误信息，并将 loading 设为 False
        return build_html_doc(error_mermaid, -1), False

    # 注意：函数到达这里时，由于它是同步执行的，UI 仍然会处于冻结状态直到函数结束。
    # 只有在函数末尾返回 False 时，loading 状态才会被解除。

    try:
        start_index = int(start_index)
        trace_type = int(trace_type_str)
        # df = cached['df']
        df = pd.read_csv("result/candidate_result_20251117_10min.csv")
        # print(df.columns) # 生产环境中可以注释掉

        if df.empty or start_index not in df.index:
            # 数据集为空或索引无效
            # 使用 start_index if start_index is not None else -1 确保 build_html_doc 接收有效值
            error_mermaid = f"graph TD; A[起始索引 {start_index} 无效或数据为空]"
            return build_html_doc(error_mermaid, start_index), False

        # 3. 确定关系列名 (核心逻辑，使用 State 传入的复选框值)
        parent_rel = 'candidate_parents'
        child_rel = 'candidate_children'

        if relation_checkbox_list:
            if 'msgType' in relation_checkbox_list:
                parent_rel = 'same_msgtype_parents'
                child_rel = 'same_msgtype_children'
            if 'global_id' in relation_checkbox_list:
                # 如果同时选中，这里会覆盖 msgType 的设置
                parent_rel = 'same_globalid_parents'
                child_rel = 'same_globalid_children'

        # 4. 处理步长参数（-1 表示不限制，转换为 None）
        max_up_steps_val = None if (max_up_steps is None or max_up_steps == -1) else int(max_up_steps)
        max_down_steps_val = None if (max_down_steps is None or max_down_steps == -1) else int(max_down_steps)

        # 5. 初始化并执行查找器
        finder = APMTraceFinder(df)  # 假设 APMTraceFinder 类已定义
        trace_data = finder.find_trace(
            start_index=start_index,
            trace_type=trace_type,
            parent_rel=parent_rel,
            child_rel=child_rel,
            max_up_steps=max_up_steps_val,
            max_down_steps=max_down_steps_val
        )

        # 6. 转换为 Mermaid 格式并渲染 HTML
        mermaid_text, tooltip_data = build_mermaid_from_graph_data(
            trace_data['nodes'],
            trace_data['edges']
        )
        html_doc = build_html_doc(mermaid_text, choose_index=start_index, tooltip_data=tooltip_data)

        # logger.info(f"Candidate Trace 查找完成...") # 生产环境中可以注释掉

        # 6. 返回结果，并将 loading 设为 False (解除加载状态)
        return html_doc, False

    except Exception as e:
        # logger.error(f"渲染 Candidate Trace 失败: {e}") # 生产环境中可以注释掉
        error_mermaid = f"graph TD; Error[查找失败: {e!s}]"

        # 7. 返回错误信息，并将 loading 设为 False (解除加载状态)
        return build_html_doc(error_mermaid, start_index if start_index is not None else -1, {}), False

# --- 保持其他辅助函数（如 filter_trans_from_entry_point 等）不变，但它们不再绑定到任何当前可见的标签页回调。---
# ... (其余代码保持不变)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5557, debug=True)