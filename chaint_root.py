import dash
from dash import html, dcc, Input, Output, State
import feffery_antd_components as fac
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collections import deque, defaultdict
import json
import re
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler('app.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ================= Utility Functions =================

def load_root_conditions(file_path='root_conditions.txt'):
    """读取 root_conditions.txt 并返回系统和交易码映射"""
    root_conditions = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    parts = line.split()
                    if len(parts) >= 2:
                        root_conditions.append((parts[0], parts[1]))
        logger.info(f"成功加载 {len(root_conditions)} 个起始条件")
    except Exception as e:
        logger.error(f"读取 {file_path} 失败: {e}")
    return root_conditions


def build_system_msgtype_map(root_conditions):
    """构建系统到交易码的映射"""
    from collections import defaultdict
    system_to_msgtypes = defaultdict(set)
    all_systems = set()
    all_msgtypes = set()
    
    for system, msgtype in root_conditions:
        system_to_msgtypes[system].add(msgtype)
        all_systems.add(system)
        all_msgtypes.add(msgtype)
    
    return system_to_msgtypes, sorted(all_systems), sorted(all_msgtypes)


# 在程序启动时加载并构建映射
ROOT_CONDITIONS = load_root_conditions()
SYSTEM_TO_MSGTYPES, ALL_SYSTEMS, ALL_MSGTYPES = build_system_msgtype_map(ROOT_CONDITIONS)


def update_f5_source_system_merge_asof3(df, latency_threshold):
    result_df = df.copy()
    f5_src_mask = result_df['srcSysname'] == 'F5'
    f5_dst_mask = result_df['dstSysname'].str.contains('F5', na=False)
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
    best_matches = pd.merge_asof(
        left=f5_src_df,
        right=f5_dst_df,
        on='start_at_ms',
        by=['msgType', 'match_key', 'match_ip'],
        tolerance=latency_threshold,
        direction='backward'
    )
    best_matches = best_matches.dropna(subset=['dstSysname'])
    if best_matches.empty:
        return result_df
    update_df = best_matches.set_index('index')[['dstSysname', 'dst_ip']]
    update_df.rename(columns={'dstSysname': 'srcSysname', 'dst_ip': 'src_ip'}, inplace=True)
    result_df.update(update_df)

    result_df['src_ip_1'] = result_df['src_ip'].str.split(".").str[0]
    result_df['src_ip_2'] = result_df['dst_ip'].str.split(".").str[0]
    del result_df['src_ip_1']
    del result_df['src_ip_2']
    return result_df


def search_apm_data(start, end, limit=5000000):
    if start == "" or end == "":
        logger.warning("【数据查询】未配置开始和结束时间！")
        return pd.DataFrame()
    start_timestamp = int(datetime.strptime(start, '%Y-%m-%d %H:%M:%S').timestamp() * 1000)
    end_timestamp = int(datetime.strptime(end, '%Y-%m-%d %H:%M:%S').timestamp() * 1000)
    df = pd.read_csv("data/data_20251117.csv")
    df['end_at_ms'] = df['start_at_ms'] + df['latency_msec']
    df = df[(df['start_at_ms'] >= start_timestamp) & (df['end_at_ms'] <= end_timestamp)]
    df = update_f5_source_system_merge_asof3(df, 36000000)
    # 优化：使用向量化操作过滤 MQ
    df = df[~((df['srcSysname'].str.contains('MQ', na=False)) | (df['dstSysname'].str.contains('MQ', na=False)))]
    df['r_src_ip'] = df['src_ip']
    df['r_dst_ip'] = df['dst_ip']
    # 优化：一次性处理列重命名和删除
    if 'ori_src_ip' in df.columns:
        df['src_ip'] = df['ori_src_ip']
    if 'ori_dst_ip' in df.columns:
        df['dst_ip'] = df['ori_dst_ip']
    # 优化：一次性删除多个列
    df = df.drop(columns=[col for col in ['ori_src_ip', 'ori_dst_ip'] if col in df.columns], errors='ignore')
    df = df.sort_values(by='start_at_ms').reset_index(drop=True)
    df["index"] = df.index
    return df


def esb_requires_same_msg(parent_row, child_row):
    """判断是否属于 ESB/F5 特殊结构"""
    p_src = str(parent_row.get('srcSysname', '') or '')
    p_dst = str(parent_row.get('dstSysname', '') or '')
    c_src = str(child_row.get('srcSysname', '') or '')
    c_dst = str(child_row.get('dstSysname', '') or '')

    cond1 = (c_src == 'ESB-F5' and c_dst == 'ESB' and p_dst == 'ESB-F5')
    cond2 = (c_src == 'ESB' and c_dst.endswith('-F5') and p_src == 'ESB-F5' and p_dst == 'ESB')
    prefix = c_src[:-3] if c_src.endswith('-F5') else ''
    cond3 = (c_src.endswith('-F5') and c_dst == prefix and p_src == 'ESB' and p_dst == c_src)

    return cond1 or cond2 or cond3


def _find_parents_for_node(current_idx, current_row, window_df, df_for_parents, dst_groups,
                           left_offset, right_offset, use_msgtype, use_gid, node_parents):
    """
    为指定节点查找候选父节点（从全量数据中）
    条件：parent.dst_ip == current.src_ip 且满足时间包含关系
    """
    # 条件：parent.dst_ip == current.src_ip 且满足时间包含关系
    current_src_ip = str(current_row['r_src_ip'])
    parent_candidate_indices = dst_groups.get(current_src_ip, [])
    if parent_candidate_indices is None or len(parent_candidate_indices) == 0:
        return
    
    parent_candidate_rows = df_for_parents.loc[parent_candidate_indices]
    
    # 检查时间包含关系：父节点的时间范围（考虑 offset）应该包含子节点的时间范围
    # 条件：parent_start - left_offset <= child_start 且 parent_end + right_offset >= child_end
    child_start = int(current_row['start_at_ms'])
    child_end = int(current_row['end_at_ms'])
    
    # 时间过滤：只保留满足时间包含关系的候选父节点
    time_filtered_parents = parent_candidate_rows[
        (parent_candidate_rows['start_at_ms'] - (left_offset or 0) <= child_start) &
        (parent_candidate_rows['end_at_ms'] + (right_offset or 0) >= child_end)
    ]
    
    if time_filtered_parents.empty:
        return
    
    # 进一步应用 msgType 和 global_id 过滤
    # 注意：这里 current_row 是子节点，parent_row 是父节点
    for parent_idx, parent_row in time_filtered_parents.iterrows():
        if parent_idx == current_idx:
            continue
        
        # 检查 msgType 和 global_id 过滤
        parent_msg = parent_row.get('msgType')
        parent_gid = parent_row.get('global_id')
        parent_gid = parent_gid if pd.notna(parent_gid) else None
        current_msg = current_row.get('msgType')
        current_gid = current_row.get('global_id')
        current_gid = current_gid if pd.notna(current_gid) else None
        
        # msgType 过滤（ESB/F5 结构）
        if use_msgtype and esb_requires_same_msg(parent_row, current_row):
            if not (pd.isna(parent_msg) or pd.isna(current_msg)) and parent_msg != current_msg:
                continue
        
        # global_id 过滤
        if use_gid and parent_gid is not None and current_gid is not None and parent_gid != current_gid:
            continue
        
        # 添加到候选父节点列表
        if int(parent_idx) not in node_parents.get(current_idx, []):
            if current_idx not in node_parents:
                node_parents[current_idx] = []
            node_parents[current_idx].append(int(parent_idx))


def filter_candidates(parent_row, candidate_rows, use_msgtype=False, use_gid=False):
    parent_msg = parent_row.get('msgType')
    parent_gid = parent_row.get('global_id')
    parent_gid = parent_gid if pd.notna(parent_gid) else None
    results = []
    for _, row in candidate_rows.iterrows():
        child_msg = row.get('msgType')
        child_gid = row.get('global_id')
        child_gid = child_gid if pd.notna(child_gid) else None

        # msgType 过滤
        if use_msgtype and esb_requires_same_msg(parent_row, row):
            if not (pd.isna(parent_msg) or pd.isna(child_msg)) and parent_msg != child_msg:
                continue

        # global_id 过滤
        if use_gid and parent_gid is not None and child_gid is not None and parent_gid != child_gid:
            continue

        results.append(row)
    if not results:
        return pd.DataFrame()
    return pd.DataFrame(results)


def build_downstream_trace(df, start_index, left_offset, right_offset,
                           use_msgtype=False, use_gid=False, max_down_steps=-1):
    if df.empty or start_index not in df['index'].values:
        return pd.DataFrame()

    start_row = df[df['index'] == start_index].iloc[0]
    start_time = int(start_row['start_at_ms']) - (left_offset or 0)
    end_time = int(start_row['end_at_ms']) + (right_offset or 0)

    window_df = df[(df['start_at_ms'] >= start_time) & (df['end_at_ms'] <= end_time)].copy()
    if window_df.empty:
        return pd.DataFrame()

    window_df = window_df.sort_values(by='start_at_ms')
    window_df.set_index('index', inplace=True)

    # 候选子节点从 window_df 中找（子节点应该在时间窗口内）
    src_groups = window_df.groupby('r_src_ip').groups
    
    # 候选父节点从全量数据 df 中找（父节点可能在时间窗口之前）
    # 构建全量数据的 IP 索引，用于查找候选父节点
    df_for_parents = df.copy()
    df_for_parents.set_index('index', inplace=True)
    dst_groups = df_for_parents.groupby('r_dst_ip').groups

    visited = set()
    queue = deque([(start_index, 0)])
    visited.add(start_index)

    node_children = defaultdict(list)
    node_parents = defaultdict(list)
    node_steps = {}

    while queue:
        current_idx, depth = queue.popleft()
        node_steps[current_idx] = depth
        current_row = window_df.loc[current_idx]

        # 步长限制
        if max_down_steps is not None and max_down_steps >= 0 and depth >= max_down_steps:
            # 即使达到步长限制，也要查找候选父节点
            _find_parents_for_node(current_idx, current_row, window_df, df_for_parents, dst_groups, 
                                  left_offset, right_offset, use_msgtype, use_gid, node_parents)
            continue

        # 为当前节点查找候选子节点
        candidate_indices = src_groups.get(str(current_row['r_dst_ip']), [])
        # groups 字典的值是 pandas.Index，直接用 if not candidate_indices 会触发
        # "truth value of an Index is ambiguous" 错误，这里统一用长度判断
        if candidate_indices is None or len(candidate_indices) == 0:
            # 没有候选子节点（叶子节点），但仍需要查找候选父节点
            _find_parents_for_node(current_idx, current_row, window_df, df_for_parents, dst_groups, 
                                  left_offset, right_offset, use_msgtype, use_gid, node_parents)
            continue

        candidate_rows = window_df.loc[candidate_indices]
        
        # 检查时间包含关系：父节点的时间范围（考虑 offset）应该包含子节点的时间范围
        # 条件：parent_start - left_offset <= child_start 且 parent_end + right_offset >= child_end
        current_start = int(current_row['start_at_ms'])
        current_end = int(current_row['end_at_ms'])
        parent_start_lower = current_start - (left_offset or 0)
        parent_end_upper = current_end + (right_offset or 0)
        
        # 时间过滤：只保留满足时间包含关系的候选子节点
        time_filtered = candidate_rows[
            (candidate_rows['start_at_ms'] >= parent_start_lower) &
            (candidate_rows['end_at_ms'] <= parent_end_upper)
        ]
        
        if time_filtered.empty:
            # 时间过滤后没有子节点（叶子节点），但仍需要查找候选父节点
            _find_parents_for_node(current_idx, current_row, window_df, df_for_parents, dst_groups, 
                                  left_offset, right_offset, use_msgtype, use_gid, node_parents)
            continue
        
        # 进一步应用 msgType 和 global_id 过滤
        filtered_children = filter_candidates(current_row, time_filtered, use_msgtype, use_gid)
        if filtered_children.empty:
            # 过滤后没有子节点（叶子节点），但仍需要查找候选父节点
            _find_parents_for_node(current_idx, current_row, window_df, df_for_parents, dst_groups, 
                                  left_offset, right_offset, use_msgtype, use_gid, node_parents)
            continue

        # 有子节点的情况：记录子节点并加入队列
        for child_idx, child_row in filtered_children.iterrows():
            node_children[current_idx].append(int(child_idx))
            if child_idx not in visited:
                visited.add(child_idx)
                queue.append((int(child_idx), depth + 1))
        
        # 为当前节点查找候选父节点（从全量数据中）
        # 注意：无论是否有子节点，都要查找候选父节点
        _find_parents_for_node(current_idx, current_row, window_df, df_for_parents, dst_groups, 
                              left_offset, right_offset, use_msgtype, use_gid, node_parents)

    if not visited:
        return pd.DataFrame()

    result_rows = []
    for idx in visited:
        row = window_df.loc[idx].copy()
        row_dict = row.to_dict()
        row_dict['index'] = int(idx)
        row_dict['step'] = node_steps.get(idx, 0)
        row_dict['候选子节点'] = node_children.get(idx, [])
        row_dict['候选父节点'] = node_parents.get(idx, [])
        result_rows.append(row_dict)

    result_df = pd.DataFrame(result_rows)
    result_df.sort_values(by=['step', 'start_at_ms'], inplace=True)
    result_df.reset_index(drop=True, inplace=True)
    return result_df


# ================ Mermaid 图构建 ================
def _sanitize_id(name: str) -> str:
    """将业务系统名转为 mermaid 可用的节点 id"""
    if name is None:
        return "unknown"
    return re.sub(r'[^a-zA-Z0-9_]', '_', str(name)) or "unknown"


def _escape_label(label: str) -> str:
    """
    将 label 中 mermaid 可能无法解析的字符做转义/替换
    - 双引号、反引号、竖线用空格替换
    - 反斜杠转义
    """
    if label is None:
        return ""
    text = str(label)
    text = text.replace("\\", "\\\\")
    text = re.sub(r'["`|]', ' ', text)
    return text


def build_mermaid_from_downstream(rows: list, start_index: int):
    """
    将下游链路表格数据转换为 mermaid 文本，并返回节点/边的 tooltip 数据
    节点：业务系统名；hover 显示 IP
    边：label 显示 msgType；hover 显示 start/end/latency/global_id
    """
    nodes = {}
    edges = []

    for row in rows:
        src_label = str(row.get('srcSysname') or 'UNKNOWN_SRC')
        dst_label = str(row.get('dstSysname') or 'UNKNOWN_DST')
        src_ip = str(row.get('r_src_ip') or '')
        dst_ip = str(row.get('r_dst_ip') or '')
        src_id = f"N_{_sanitize_id(src_label)}"
        dst_id = f"N_{_sanitize_id(dst_label)}"
        edge_idx = row.get('index')
        label_orig = str(row.get('msgType') or 'None')
        # 在 label 上附加 index 标记并换行，便于视觉区分与反查唯一边
        label_with_idx = (
            f"{label_orig}<br/>index{edge_idx}" if edge_idx is not None else label_orig
        )

        # 记录节点信息（重复节点只保留首次）
        if src_id not in nodes:
            nodes[src_id] = {"label": src_label, "ip": src_ip, "is_start": False}
        if dst_id not in nodes:
            nodes[dst_id] = {"label": dst_label, "ip": dst_ip, "is_start": False}

        # 标记起始行对应的源节点
        if row.get('index') == start_index:
            nodes[src_id]["is_start"] = True

        edges.append({
            "edge_id": str(edge_idx) if edge_idx is not None else "",
            "src": src_id,
            "dst": dst_id,
            "label": label_orig,
            "label_with_idx": label_with_idx,
            "start_at_ms": str(row.get('start_at_ms') or ''),
            "end_at_ms": str(row.get('end_at_ms') or ''),
            "latency_msec": str(row.get('latency_msec') or ''),
            "global_id": str(row.get('global_id') or '')
        })

    # 生成 mermaid 文本
    lines = ["flowchart LR"]
    for node_id, info in nodes.items():
        lines.append(f'{node_id}["{_escape_label(info["label"])}"]')
    for e in edges:
        lines.append(f'{e["src"]} -->|{_escape_label(e["label_with_idx"])}| {e["dst"]}')

    return "\n".join(lines) + "\n", nodes, edges


def build_html_doc_downstream(mermaid_text: str, nodes: dict, edges: list, title: str) -> str:
    """生成带 tooltip 的 Mermaid HTML 文档"""
    tooltip_json_nodes = json.dumps(nodes)
    tooltip_json_edges = json.dumps(edges)
    return f"""
<!DOCTYPE html>
<html lang=\"zh-CN\"><head><meta charset=\"UTF-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
<title>下游链路</title>
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
  .mermaid svg .node.start-node rect {{ fill: #fef3c7 !important; stroke: #f59e0b !important; stroke-width: 2.2px !important; }}
  .mermaid svg .node.start-node text {{ fill: #92400e !important; font-weight: 700 !important; }}
  .node-tooltip {{
    position: absolute; background: rgba(0,0,0,0.9); color: #fff; padding: 12px 16px;
    border-radius: 8px; font-size: 13px; line-height: 1.6; pointer-events: none; z-index: 9999;
    box-shadow: 0 4px 12px rgba(0,0,0,0.3); max-width: 420px; white-space: nowrap; display: none;
  }}
  .node-tooltip .tooltip-row {{ margin: 2px 0; }}
  .node-tooltip .tooltip-label {{ color: #94a3b8; font-weight: 600; margin-right: 8px; }}
  .node-tooltip .tooltip-value {{ color: #e2e8f0; }}
</style>
</head><body>
<div class=\"container\"><div class=\"card\"><div class=\"title\">{title}</div>
<div class=\"mermaid\">{mermaid_text}</div></div></div>
<div id="tooltip" class="node-tooltip"></div>
<script src="/assets/mermaid.min.js"></script>
<script>
  const nodeData = {tooltip_json_nodes};
  const edgeData = {tooltip_json_edges};
  mermaid.initialize({{
    startOnLoad: true,
    theme: 'default',
    securityLevel: 'loose',
    // 使用线性曲线减少大弧度，避免 label 离连线过远
    flowchart: {{ curve: 'linear', rankSpacing: 120, nodeSpacing: 70 }}
  }});

  setTimeout(() => {{
    const tooltip = document.getElementById('tooltip');

    // 1) 适配 mermaid 生成的节点 id（形如 flowchart-xxxx-N_xxx）
    const extractNodeId = (rawId) => {{
      const m = String(rawId).match(/(N_[A-Za-z0-9_]+)/);
      return m ? m[1] : String(rawId);
    }};

    // 节点 tooltip
    document.querySelectorAll('.node').forEach(node => {{
      const nid = extractNodeId(node.id);
      const data = nodeData[nid];
      if (data) {{
        if (data.is_start) {{ node.classList.add('start-node'); }}
        node.style.cursor = 'pointer';
        node.addEventListener('mouseenter', (e) => {{
          tooltip.innerHTML = `
            <div class="tooltip-row"><span class="tooltip-label">系统:</span><span class="tooltip-value">${{data.label}}</span></div>
            <div class="tooltip-row"><span class="tooltip-label">IP:</span><span class="tooltip-value">${{data.ip}}</span></div>
          `;
          tooltip.style.display = 'block';
        }});
        node.addEventListener('mousemove', (e) => {{
          tooltip.style.left = (e.pageX + 15) + 'px';
          tooltip.style.top = (e.pageY + 15) + 'px';
        }});
        node.addEventListener('mouseleave', () => {{ tooltip.style.display = 'none'; }});
      }}
    }});

    // 2) 边 tooltip：按 edge_id（index 唯一）匹配，避免错位
    document.querySelectorAll('.edgeLabel').forEach((el) => {{
      const labelText = el.textContent.trim();
      const m = labelText.match(/index(\d+)/);
      const edgeId = m ? m[1] : '';

      let data = null;
      for (let i = 0; i < edgeData.length; i++) {{
        if ((edgeData[i].edge_id || '') === edgeId) {{
          data = edgeData[i];
          break;
        }}
      }}
      if (!data) return;

      el.style.cursor = 'pointer';
      el.addEventListener('mouseenter', () => {{
        tooltip.innerHTML = `
          <div class="tooltip-row"><span class="tooltip-label">msgType:</span><span class="tooltip-value">${{data.label}}</span></div>
          <div class="tooltip-row"><span class="tooltip-label">start_at_ms:</span><span class="tooltip-value">${{data.start_at_ms}}</span></div>
          <div class="tooltip-row"><span class="tooltip-label">end_at_ms:</span><span class="tooltip-value">${{data.end_at_ms}}</span></div>
          <div class="tooltip-row"><span class="tooltip-label">latency_msec:</span><span class="tooltip-value">${{data.latency_msec}}</span></div>
          <div class="tooltip-row"><span class="tooltip-label">global_id:</span><span class="tooltip-value">${{data.global_id}}</span></div>
        `;
        tooltip.style.display = 'block';
      }});
      el.addEventListener('mousemove', (e) => {{
        tooltip.style.left = (e.pageX + 15) + 'px';
        tooltip.style.top = (e.pageY + 15) + 'px';
      }});
      el.addEventListener('mouseleave', () => {{ tooltip.style.display = 'none'; }});
    }});
  }}, 800);
</script>
</body></html>
"""


# ================= Dash App =================

# now_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
# start_dt = (datetime.now() - timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
now_dt = '2025-11-17 11:00:00'
start_dt ='2025-11-17 10:50:00'

app = dash.Dash(__name__)
app.title = "重点交易码下游链路"

cached = {
    "df": pd.DataFrame(),
    "root_df": pd.DataFrame(),
    "start": "",
    "end": "",
    "left_offset": 0,
    "right_offset": 0
}


def build_layout():
    query_bar = fac.AntdSpace([
        fac.AntdIcon(icon='antd-field-time', style={"color": '#fff', "fontSize": 18}),
        fac.AntdText("时间范围", strong=True, style={"color": "white"}),
        fac.AntdDateRangePicker(
            id="picker-range",
            showTime=True,
            format='YYYY-MM-DD HH:mm:ss',
            defaultValue=[start_dt, now_dt],
            style={"width": 350}
        ),
        fac.AntdText("left offset(ms)", strong=True, style={"color": "white"}),
        fac.AntdInputNumber(id='left-offset', value=0, style={"width": 70}),
        fac.AntdText("right offset(ms)", strong=True, style={"color": "white"}),
        fac.AntdInputNumber(id='right-offset', value=0, style={"width": 70}),
        fac.AntdText("业务系统:", strong=True, style={"color": "white"}),
        fac.AntdSelect(
            id='root-system-select',
            placeholder='请选择业务系统（不选则显示全部）',
            style={'width': 150},
            options=[{'label': sys, 'value': sys} for sys in ALL_SYSTEMS],
            allowClear=True
        ),
        fac.AntdText("交易码:", strong=True, style={"color": "white"}),
        fac.AntdSelect(
            id='root-msgtype-select',
            placeholder='请选择交易码（不选则显示全部）',
            style={'width': 150},
            options=[],  # 初始为空，根据业务系统名动态更新
            allowClear=True
        ),
        fac.AntdButton("查询", id="btn-query", type='primary', icon=fac.AntdIcon(icon='antd-search'), autoSpin=True),
    ], size=8, style={"padding": "8px 16px", "display": "flex", "flexWrap": "wrap", "alignItems": "center"})

    root_table = fac.AntdTable(
        id="root-table",
        columns=[
            {"title": "index", "dataIndex": "index"},
            {"title": "srcSysname", "dataIndex": "srcSysname"},
            {"title": "dstSysname", "dataIndex": "dstSysname"},
            {"title": "msgType", "dataIndex": "msgType"},
            {"title": "global_id", "dataIndex": "global_id"},
            {"title": "start_time", "dataIndex": "start_time"},
            {"title": "start_at_ms", "dataIndex": "start_at_ms"},
            {"title": "end_at_ms", "dataIndex": "end_at_ms"},
            {"title": "latency_msec", "dataIndex": "latency_msec"},
            {"title": "r_src_ip", "dataIndex": "r_src_ip"},
            {"title": "r_dst_ip", "dataIndex": "r_dst_ip"}
        ],
        data=[],
        rowSelectionType='radio',
        selectedRowKeys=[],
        pagination={"pageSize": 10, 'showSizeChanger': True, "pageSizeOptions": [5, 10, 20, 50, 100], "showQuickJumper": True},
        bordered=True,
        style={"width": "100%"}
    )

    trace_controls = fac.AntdSpace([
        # 查找模式相关组件暂时停用
        # fac.AntdText("查找模式:", strong=True),
        # fac.AntdRadioGroup(
        #     id='trace-type-radio',
        #     options=[
        #         {'label': '模式 1 (严格路径)', 'value': '1'},
        #         {'label': '模式 2 (完整子图)', 'value': '2'}
        #     ],
        #     defaultValue='1',
        #     optionType='button',
        #     buttonStyle='solid'
        # ),
        # fac.AntdDivider(direction='vertical'),
        fac.AntdCheckboxGroup(
            id='relation-type-checkbox',
            options=[
                {'label': 'MsgType 过滤', 'value': 'msgType'},
                {'label': 'GlobalID 过滤', 'value': 'global_id'}
            ],
            value=[],
            style={'display': 'inline-flex', 'gap': '10px'}
        ),
        fac.AntdDivider(direction='vertical'),
        fac.AntdText("向下步长:", strong=True),
        fac.AntdInputNumber(id='max-down-steps-input', min=-1, value=-1, style={'width': 120}),
    ], size=8, style={"margin": "12px 0"})

    downstream_table = fac.AntdTable(
        id="downstream-table",
        columns=[
            {"title": "step", "dataIndex": "step"},
            {"title": "index", "dataIndex": "index"},
            {"title": "msgType", "dataIndex": "msgType"},
            {"title": "global_id", "dataIndex": "global_id"},
            {"title": "srcSysname", "dataIndex": "srcSysname"},
            {"title": "dstSysname", "dataIndex": "dstSysname"},
            {"title": "start_at_ms", "dataIndex": "start_at_ms"},
            {"title": "end_at_ms", "dataIndex": "end_at_ms"},
            {"title": "latency_msec", "dataIndex": "latency_msec"},
            {"title": "r_src_ip", "dataIndex": "r_src_ip"},
            {"title": "r_dst_ip", "dataIndex": "r_dst_ip"},
            {"title": "候选父节点", "dataIndex": "候选父节点"},
            {"title": "候选父节点(表中)", "dataIndex": "候选父节点_表中"},
            {"title": "候选子节点", "dataIndex": "候选子节点"},
        ],
        data=[],
        bordered=True,
        pagination={"pageSize": 10, 'showSizeChanger': True, "pageSizeOptions": [5, 10, 20, 50, 100], "showQuickJumper": True},
        # 宽度按内容撑开，结合外层容器的 overflowX 实现横向滚动
        style={"minWidth": "1000px", "minHeight": "400px"}
    )

    downstream_graph = html.Iframe(
        id="downstream-graph",
        style={
            "width": "100%",
            "minHeight": "500px",
            "border": "1px solid #e5e7eb",
            "borderRadius": "6px",
            "marginTop": "16px"
        }
    )

    layout = fac.AntdLayout([
        fac.AntdHeader(
            fac.AntdRow([fac.AntdCol(query_bar, span=24)]),
            style={"background": "#001529", "padding": "0 16px"}
        ),
        fac.AntdContent([
            # 第一张卡片：上方为查找配置组件，下方为重点交易码数据表
            fac.AntdCard(
                [trace_controls, root_table],
                title="重点交易码数据",
                style={"marginBottom": 16}
            ),
            # 第二张卡片：仅展示下游链路结果表，外层增加水平滚动容器
            fac.AntdCard(
                [
                    html.Div(
                        downstream_table,
                        style={"width": "100%", "overflowX": "auto"}
                    ),
                    downstream_graph
                ],
                title="下游链路结果",
            )
        ], style={"padding": 16})
    ])
    return layout


app.layout = build_layout()


# ================= Callbacks =================

@app.callback(
    Output("root-msgtype-select", "options"),
    Input("root-system-select", "value"),
    prevent_initial_call=False
)
def update_msgtype_options(selected_system):
    """根据选择的业务系统名更新交易码下拉框选项"""
    if selected_system is None or selected_system == '':
        # 如果未选择系统，显示所有交易码
        return [{'label': msg, 'value': msg} for msg in ALL_MSGTYPES]
    else:
        # 如果选择了系统，显示该系统对应的交易码
        msgtypes = SYSTEM_TO_MSGTYPES.get(selected_system, set())
        return [{'label': msg, 'value': msg} for msg in sorted(msgtypes)]


@app.callback(
    Output("root-table", "data"),
    Output("root-table", "selectedRowKeys"),
    Output("btn-query", "loading"),
    Input("btn-query", "nClicks"),
    State("picker-range", "value"),
    State("left-offset", "value"),
    State("right-offset", "value"),
    State("root-system-select", "value"),
    State("root-msgtype-select", "value"),
    prevent_initial_call=True
)
def on_query(n_clicks, date_range, left_offset, right_offset, selected_system, selected_msgtype):
    if date_range and len(date_range) == 2:
        start = date_range[0]
        end = date_range[1]
    else:
        start = ""
        end = ""

    left_ms = left_offset if left_offset is not None else 0
    right_ms = right_offset if right_offset is not None else 0

    t1 = time.time()
    df = search_apm_data(start, end)
    t2 = time.time()
    logger.info(f"数据查询耗时：{round(t2 - t1, 2)}s，返回 {len(df)} 条记录")
    if df.empty:
        cached["df"] = pd.DataFrame()
        cached["root_df"] = pd.DataFrame()
        return [], [], False

    # 根据选择的条件确定要使用的 root_conditions
    if selected_system and selected_msgtype:
        # 如果同时选择了系统和交易码，只使用这个条件
        root_conditions = [(selected_system, selected_msgtype)]
    elif selected_system:
        # 如果只选择了系统，使用该系统下的所有交易码
        root_conditions = [(selected_system, msg) for msg in SYSTEM_TO_MSGTYPES.get(selected_system, [])]
    else:
        # 如果都没选择，使用所有 root_conditions
        root_conditions = ROOT_CONDITIONS

    if not root_conditions:
        root_df = pd.DataFrame()
    else:
        # 优化：使用向量化操作一次性构建所有条件，避免循环过滤
        # 构建所有符合条件的 mask
        mask = pd.Series(False, index=df.index)
        root_conditions_map = {}
        
        # 为每个条件构建 mask 并记录映射关系
        for root_system, root_msg in root_conditions:
            condition_mask = (df['srcSysname'] == root_system + '-F5') & (df['msgType'] == root_msg)
            mask |= condition_mask
            # 优化：使用向量化操作批量更新映射关系
            matching_indices = df.loc[condition_mask, 'index'].values
            if len(matching_indices) > 0:
                # 批量更新字典，比循环更快
                root_conditions_map.update({idx: (root_system, root_msg) for idx in matching_indices})
        
        # 一次性过滤所有符合条件的节点
        root_df = df[mask].copy()
        
        if not root_df.empty:
            # 优化：使用 map 直接映射字典，比 lambda 更快
            root_df['root_system'] = root_df['index'].map(lambda idx: root_conditions_map.get(idx, ('', ''))[0])
            root_df['root_msgType'] = root_df['index'].map(lambda idx: root_conditions_map.get(idx, ('', ''))[1])

    cached["df"] = df
    cached["start"] = start
    cached["end"] = end
    cached["left_offset"] = left_ms
    cached["right_offset"] = right_ms

    if root_df.empty:
        return [], [], False

    # 按 start_at_ms 从小到大排序
    root_df = root_df.sort_values(by='start_at_ms', ascending=True).reset_index(drop=True)
    # 优化：使用向量化操作替代 apply，大幅提升性能
    # 将毫秒时间戳转换为 datetime 对象（向量化），考虑时区（UTC+8）
    # 时间戳是 UTC 时间，需要加上8小时转换为中国时区
    start_times = pd.to_datetime(root_df['start_at_ms'], unit='ms', errors='coerce', utc=True)
    # 转换为 UTC+8 时区（使用 dt.tz_convert 因为 start_times 是 Series）
    start_times = start_times.dt.tz_convert('Asia/Shanghai')
    # 格式化为字符串（向量化）：日期时间部分 + 毫秒部分
    milliseconds = (root_df['start_at_ms'] % 1000).astype(int).astype(str).str.zfill(3)
    root_df['start_time'] = start_times.dt.strftime('%Y-%m-%d %H:%M:%S') + '.' + milliseconds

    display_cols = ['index', 'srcSysname', 'dstSysname', 'msgType', 'global_id', 'start_time', 'start_at_ms', 'end_at_ms', 'latency_msec', 'r_src_ip', 'r_dst_ip']
    cached["root_df"] = root_df[display_cols]
    # 打印命中的交易码统计
    try:
        hit_stats = (
            root_df[['root_system', 'root_msgType']]
            .value_counts()
            .reset_index(name='count')
        )
        logger.info("命中的重点交易码：")
        for _, row in hit_stats.iterrows():
            logger.info(f"  {row['root_system']} {row['root_msgType']} -> {row['count']} 条")
    except Exception as e:
        logger.warning(f"打印命中的交易码统计失败: {e}")

    return root_df[display_cols].to_dict('records'), [], False


@app.callback(
    Output("downstream-table", "data"),
    Output("downstream-graph", "srcDoc"),
    Input("root-table", "selectedRowKeys"),
    Input("relation-type-checkbox", "value"),
    Input("max-down-steps-input", "value"),
    prevent_initial_call=True
)
def on_build_trace(selected_keys, relation_types, max_down_steps):
    df = cached.get("df", pd.DataFrame())
    root_df = cached.get("root_df", pd.DataFrame())
    if not selected_keys or df.empty or root_df.empty:
        return [], ""

    # feffery_antd_table 默认 rowKey 为行序号，因此 selected_keys 是行号
    row_pos = int(selected_keys[0])
    if row_pos < 0 or row_pos >= len(root_df):
        return []

    try:
        start_index = int(root_df.iloc[row_pos]['index'])
    except Exception:
        return [], ""
    # ii = 0
    # for i in range(len(root_df)):
    #     start_index = int(root_df.iloc[i]['index'])
    #
    #     use_msgtype = relation_types and ('msgType' in relation_types)
    #     use_gid = relation_types and ('global_id' in relation_types)
    #
    #     result_df = build_downstream_trace(
    #         df=df.copy(),
    #         start_index=start_index,
    #         left_offset=cached.get("left_offset", 0),
    #         right_offset=cached.get("right_offset", 0),
    #         use_msgtype=use_msgtype,
    #         use_gid=use_gid,
    #         max_down_steps=max_down_steps if max_down_steps is not None else -1
    #     )
    #
    #     if len(result_df) > 1:
    #         ii += 1
    #     if (len(result_df) > 1) and (len(result_df) < 4):
    #         a = 1

    use_msgtype = relation_types and ('msgType' in relation_types)
    use_gid = relation_types and ('global_id' in relation_types)

    result_df = build_downstream_trace(
        df=df.copy(),
        start_index=start_index,
        left_offset=cached.get("left_offset", 0),
        right_offset=cached.get("right_offset", 0),
        use_msgtype=use_msgtype,
        use_gid=use_gid,
        max_down_steps=max_down_steps if max_down_steps is not None else -1
    )

    if result_df.empty:
        return [], ""

    # 构建 mermaid 图（使用转换前的原始数据）
    raw_rows = result_df.to_dict('records')
    mermaid_text, nodes_data, edges_data = build_mermaid_from_downstream(raw_rows, start_index)
    html_doc = build_html_doc_downstream(
        mermaid_text,
        nodes_data,
        edges_data,
        title=f"起始 index {start_index}"
    )

    # 获取表中所有节点的 index 集合
    table_indices = set(result_df['index'].tolist())
    
    # 计算只出现在表中的候选父节点（在转换为字符串之前计算）
    def filter_parents_in_table(parent_list):
        if isinstance(parent_list, list):
            filtered = [p for p in parent_list if int(p) in table_indices]
            return ','.join(map(str, filtered)) if filtered else ''
        return ''
    
    # 先计算候选父节点_表中（此时候选父节点还是列表格式）
    result_df['候选父节点_表中'] = result_df['候选父节点'].apply(filter_parents_in_table)
    
    # 然后将候选父节点和候选子节点转换为字符串
    result_df['候选父节点'] = result_df['候选父节点'].apply(lambda x: ','.join(map(str, x)) if isinstance(x, list) else str(x))
    result_df['候选子节点'] = result_df['候选子节点'].apply(lambda x: ','.join(map(str, x)) if isinstance(x, list) else str(x))
    
    return result_df[['step', 'index', 'msgType', 'global_id', 'srcSysname', 'dstSysname',
                      'start_at_ms', 'end_at_ms', 'latency_msec', 'r_src_ip', 'r_dst_ip', 
                      '候选父节点', '候选父节点_表中', '候选子节点']].to_dict('records'), html_doc


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5558, debug=True)

