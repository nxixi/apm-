import pandas as pd
import time
from datetime import datetime
from typing import List, Dict, Set, Tuple, Optional
from collections import defaultdict
from functools import cmp_to_key

import dash
from dash import html, dcc, Output, Input, State
import feffery_antd_components as fac
import dash_ag_grid as dag
import plotly.graph_objects as go

from src.chain import logger, search_apm_data_from_ck, chain_trace_from_selected_row, search_cloud_data_from_ck, query_chain_condition_data, query_full_link_log_data
from src.ck_client import query_ck, CK_DATABASE, build_apm_cte

HAS_AG_GRID = True


def build_tree_from_chain_data(chain_df: pd.DataFrame, cloud_df: pd.DataFrame) -> List[Dict]:
    """
    将串联结果（云下+云上）整理成树形结构（raw_tree格式）

    参数:
        chain_df: 云下串联结果 DataFrame
        cloud_df: 云上串联结果 DataFrame

    返回:
        拍平后的树形数据列表，每个元素包含 callPath 字段用于 AgGrid treeData
    """
    if chain_df.empty and cloud_df.empty:
        return []

    # 合并云下和云上数据
    if not cloud_df.empty:
        all_df = pd.concat([chain_df, cloud_df], ignore_index=True)
    else:
        all_df = chain_df.copy()

    # 构建索引：trace_id/span_id -> row
    trace_id_to_node = {}  # 云下：trace_id -> node
    span_id_to_node = {}  # 云上：span_id -> node
    all_nodes = {}  # 所有节点的统一索引

    # 处理云下数据
    for _, row in chain_df.iterrows():
        trace_id = row.get('trace_id')
        if trace_id is not None and str(trace_id) != 'nan':
            # 处理 start_at_time：如果不存在，从 start_at_ms 转换
            start_at_time = row.get('start_at_time')
            start_at_ms = row.get('start_at_ms')
            if not start_at_time and start_at_ms:
                try:
                    start_at_time = datetime.fromtimestamp(start_at_ms / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                except:
                    start_at_time = None
            
            # 计算is_esb和is_f5
            w_ip = row.get('w_ip')
            has_w_ip = w_ip is not None and str(w_ip) != '' and str(w_ip) != 'nan'
            deploy_unit = str(row.get('deployUnitObjectName', ''))
            is_esb = has_w_ip and deploy_unit.startswith('ESB')
            is_f5 = has_w_ip and not deploy_unit.startswith('ESB')
            
            node = {
                'id': f"down_{trace_id}",
                'trace_id': trace_id,
                'log_id': row.get('log_id'),
                'global_id': row.get('global_id'),
                'msgType': row.get('msgType'),
                'start_at_time': start_at_time,
                'start_at_ms': start_at_ms,
                'latency_msec': row.get('latency_msec'),
                'location': '云下',
                'deployUnitObjectName': row.get('deployUnitObjectName', ''),
                'parents': row.get('parents', ''),
                'children': row.get('children', ''),
                'span_id': row.get('span_id'),
                'parent_span_id': row.get('parent_span_id'),
                'src_ip': row.get('src_ip'),
                'srcSysName': row.get('srcSysName'),
                'is_esb': '是' if is_esb else '否',
                'is_f5': '是' if is_f5 else '否',
                'node_type': 'down'
            }
            trace_id_to_node[trace_id] = node
            all_nodes[node['id']] = node

    # 处理云上数据
    for _, row in cloud_df.iterrows():
        span_id = row.get('span_id')
        parent_span_id = row.get('parent_span_id')
        log_id = row.get('log_id', '')
        if span_id is not None and str(span_id) != 'nan':
            # 使用 span_id + log_id 保证 id 的唯一性
            unique_id = f"{span_id}_{log_id}" if log_id else str(span_id)
            node = {
                'id': f"up_{unique_id}",
                'span_id': span_id,
                'parent_span_id': parent_span_id,
                'log_id': log_id,
                'global_id': row.get('global_id'),
                'msgType': row.get('msgType'),
                'start_at_time': row.get('start_at_time'),
                'start_at_ms': row.get('start_at_ms'),
                'latency_msec': row.get('latency_msec'),
                'location': '云上',
                'deployUnitObjectName': row.get('deployUnitObjectName', ''),
                'parents': row.get('parents', ''),
                'children': row.get('children', ''),
                'trace_id': row.get('trace_id'),
                'src_ip': row.get('src_ip'),
                'srcSysName': row.get('srcSysName'),
                'is_esb': '否',  # 云上数据没有ESB标识
                'is_f5': '否',   # 云上数据没有F5标识
                'node_type': 'up'
            }
            span_id_to_node[span_id] = node
            all_nodes[node['id']] = node

    # 构建父子关系映射
    # 云下：通过 parents/children (trace_id)
    child_to_parent_down = {}  # 子节点ID -> 父节点ID

    # 方式1：通过 parents 字段（子节点 -> 父节点）
    for node_id, node in all_nodes.items():
        if node.get('node_type') == 'down' and node.get('parents'):
            # 找到父节点（云下）
            for parent_trace_id in node['parents']:
                if parent_trace_id in trace_id_to_node:
                    parent_node = trace_id_to_node[parent_trace_id]
                    child_to_parent_down[node_id] = parent_node['id']
                    break  # 只取第一个父节点

    # 方式2：通过 children 字段（父节点 -> 子节点）
    for node_id, node in all_nodes.items():
        if node.get('node_type') == 'down' and node.get('children'):
            # 当前节点是父节点，找到所有子节点
            for child_trace_id in node['children']:
                if child_trace_id in trace_id_to_node:
                    child_node = trace_id_to_node[child_trace_id]
                    child_node_id = child_node['id']
                    # 如果子节点还没有父节点，则设置
                    if child_node_id not in child_to_parent_down:
                        child_to_parent_down[child_node_id] = node_id

    # 云上：通过 parent_span_id
    child_to_parent_up = {}  # 子节点ID -> 父节点ID
    for node_id, node in all_nodes.items():
        if node.get('node_type') == 'up' and node.get('parent_span_id'):
            parent_span_id = node['parent_span_id']
            if parent_span_id in span_id_to_node:
                parent_node = span_id_to_node[parent_span_id]
                child_to_parent_up[node_id] = parent_node['id']

    # 云上和云下的串联：通过时间关联
    # 对于云上节点，查找时间最接近的云下节点作为父节点
    # 同时，对于云下节点，也可以查找时间接近的云上节点作为子节点
    time_based_links = {}  # 子节点ID -> 父节点ID

    # 云上节点 -> 云下节点（通过时间窗口）
    for up_node_id, up_node in all_nodes.items():
        if up_node.get('node_type') == 'up':
            up_start_ms = up_node.get('start_at_ms', 0)
            up_latency = up_node.get('latency_msec', 0)
            up_end_ms = up_start_ms + up_latency
            up_log_id = str(up_node.get('log_id', '') or '')
            # 找到log_id相同且时间最接近的云下节点（满足时间范围和ESB条件）
            best_match = None
            min_time_diff = float('inf')
            for down_node_id, down_node in all_nodes.items():
                if down_node.get('node_type') == 'down':
                    down_start_ms = down_node.get('start_at_ms', 0)
                    down_latency = down_node.get('latency_msec', 0)
                    down_end_ms = down_start_ms + down_latency
                    down_deploy_unit = str(down_node.get('deployUnitObjectName', '') or '')
                    down_log_id = str(down_node.get('log_id', '') or '')
                    
                    # 条件1：log_id相同
                    # 条件2：云上start_at_ms >= 云下start_at_ms
                    # 条件3：云上start_at_ms + 云上latency_msec <= 云下start_at_ms + 云下latency_msec
                    # 条件4：云下deployUnitObjectName以ESB开头
                    if (up_log_id == down_log_id and
                        up_start_ms >= down_start_ms and 
                        up_end_ms <= down_end_ms and
                        down_deploy_unit.startswith('ESB')):
                        time_diff = abs(up_start_ms - down_start_ms)
                        if time_diff < min_time_diff:
                            min_time_diff = time_diff
                            best_match = down_node_id
            if best_match:
                time_based_links[up_node_id] = best_match

    # 合并所有父子关系（优先级：云下关系 > 云上关系 > 时间关系）
    # 注意：如果同一个子节点有多个父节点关系，优先使用已有的关系
    child_to_parent = {}
    # 先添加云下关系
    for child_id, parent_id in child_to_parent_down.items():
        child_to_parent[child_id] = parent_id
    # 再添加云上关系（不覆盖已有的）
    for child_id, parent_id in child_to_parent_up.items():
        if child_id not in child_to_parent:
            child_to_parent[child_id] = parent_id
    # 最后添加时间关系（不覆盖已有的）
    for child_id, parent_id in time_based_links.items():
        if child_id not in child_to_parent:
            child_to_parent[child_id] = parent_id
    
    # 【新增】去除间接调用关系：如果存在 a->b, b->c, a->c，则删除 a->c
    # 使用传递闭包检测间接关系
    def has_indirect_path(src_id: str, dst_id: str, child_to_parent_map: Dict) -> bool:
        """
        检查从 src_id 到 dst_id 是否存在间接路径（通过中间节点）
        返回 True 表示存在间接路径
        """
        # 从 dst_id 开始，沿着父节点向上查找，看是否能通过中间节点到达 src_id
        visited = set()
        current = dst_id
        
        # 先找到 dst_id 的直接父节点
        if current not in child_to_parent_map:
            return False
        
        direct_parent = child_to_parent_map[current]
        if direct_parent == src_id:
            # dst_id 的直接父节点就是 src_id，不是间接关系
            return False
        
        # 从 dst_id 的直接父节点开始，继续向上查找
        queue = [direct_parent]
        visited.add(dst_id)
        visited.add(direct_parent)
        
        while queue:
            current = queue.pop(0)
            
            # 如果找到了 src_id，说明存在间接路径
            if current == src_id:
                return True
            
            # 继续向上查找
            if current in child_to_parent_map:
                parent = child_to_parent_map[current]
                if parent not in visited:
                    visited.add(parent)
                    queue.append(parent)
        
        return False
    
    # 过滤间接关系
    filtered_child_to_parent = {}
    for child_id, parent_id in child_to_parent.items():
        # 检查是否为间接关系
        if not has_indirect_path(parent_id, child_id, child_to_parent):
            filtered_child_to_parent[child_id] = parent_id
        else:
            logger.info(f"移除间接调用关系: {parent_id} -> {child_id}")
    
    # 使用过滤后的关系
    child_to_parent = filtered_child_to_parent

    # 构建反向映射：父节点 -> 子节点列表
    parent_to_children = defaultdict(list)
    for child_id, parent_id in child_to_parent.items():
        parent_to_children[parent_id].append(child_id)

    # 找到根节点（没有父节点的节点）
    root_nodes = []
    for node_id in all_nodes.keys():
        if node_id not in child_to_parent:
            root_nodes.append(node_id)

    # 如果没有根节点，使用第一个节点作为根节点
    if not root_nodes and all_nodes:
        root_nodes = [list(all_nodes.keys())[0]]

    # 递归构建树形结构
    def build_tree_node(node_id: str, visited: Set[str]) -> Dict:
        """递归构建树节点"""
        if node_id in visited:
            return None  # 防止循环
        visited.add(node_id)

        node = all_nodes.get(node_id)
        if not node:
            return None

        # 构建节点数据
        tree_node = {
            'id': node_id,
            'trace_id': node.get('trace_id'),
            'span_id': node.get('span_id'),
            'parent_span_id': node.get('parent_span_id'),
            'log_id': node.get('log_id'),
            'global_id': node.get('global_id'),
            'msgType': node.get('msgType', ''),
            'start_at_time': node.get('start_at_time'),
            'start_at_ms': node.get('start_at_ms'),
            'latency_msec': node.get('latency_msec'),
            'location': node.get('location', ''),
            'deployUnitObjectName': node.get('deployUnitObjectName', ''),
            'parents': node.get('parents'),
            'children': node.get('children'),
            'is_esb': node.get('is_esb', '否'),
            'is_f5': node.get('is_f5', '否'),
            'src_ip': node.get('src_ip'),
            'srcSysName': node.get('srcSysName'),
            'node_type': node.get('node_type'),
            'children_nodes': []  # 用于递归构建
        }

        # 添加子节点
        children_ids = parent_to_children.get(node_id, [])
        for child_id in children_ids:
            child_node = build_tree_node(child_id, visited)
            if child_node:
                tree_node['children_nodes'].append(child_node)

        return tree_node

    # 构建所有根节点的树
    raw_tree = []
    for root_id in root_nodes:
        tree_node = build_tree_node(root_id, set())
        if tree_node:
            raw_tree.append(tree_node)

    # 拍平树形结构，添加 callPath 字段
    flat_rows = []

    def walk_tree(node: Dict, path: List[str] = None):
        """遍历树，拍平并添加路径"""
        if path is None:
            path = []

        # 构建路径：使用 location + msgType 作为路径标识
        location = node.get('location', '')
        msg_type = node.get('msgType', '')
        log_id = node.get('log_id', '')

        # 构建节点标签
        if msg_type:
            node_label = f"{location}.{msg_type}"
        else:
            node_label = location or "未知"

        if log_id:
            node_label += f"({log_id})"

        current_path = path + [node_label]

        # 构建行数据（排除 children_nodes，用 children 展示原始 children 列）
        row = {k: v for k, v in node.items() if k not in ('children_nodes',)}

        # parents、children 若为列表，在展示层转换为用逗号分隔的字符串，便于表格阅读
        for col in ("parents", "children"):
            if col in row and isinstance(row[col], (list, tuple)):
                row[col] = ", ".join(str(x) for x in row[col])

        # 确保 callPath 一定是列表，避免前端 getDataPath 调用 .forEach 出错
        row['callPath'] = current_path if isinstance(current_path, list) else [str(current_path)]
        flat_rows.append(row)

        # 递归处理子节点
        for child in node.get('children_nodes', []):
            walk_tree(child, current_path)

    for root in raw_tree:
        walk_tree(root)

    # 按 start_at_ms 排序
    flat_rows.sort(key=lambda x: x.get('start_at_ms', 0))

    return flat_rows


def create_timeline_gantt_chart(
    df_down: pd.DataFrame,
    df_up: pd.DataFrame,
    df_full_link: pd.DataFrame = None,
) -> Tuple[go.Figure, List[Dict], List[Dict]]:
    """
    创建时间跨度甘特图
    
    参数:
        df_down: 云下数据
        df_up: 云上数据
    
    返回:
        (Plotly Figure 对象, 表格数据列表, all_data列表用于拓扑图)
    """
    def truncate_text(text, max_length=50):
        """截断文本，超过长度时显示省略号"""
        # 使用 pd.isna() 检查 pandas NA 值，避免 TypeError
        if text is None or pd.isna(text):
            return ''
        text_str = str(text)
        if len(text_str) <= max_length:
            return text_str
        return text_str[:max_length] + '...'
    
    fig = go.Figure()
    
    # 收集所有数据，后续统一通过 sort_timestamp/sort_send_time/sort_rtn_time 排序
    all_data = []
    
    # 处理云下数据
    for idx, row in df_down.iterrows():
        start_ms = int(row.get('start_at_ms', 0))
        latency = int(row.get('latency_msec', 0))
        end_ms = start_ms + latency
        
        is_esb = bool(row.get('is_esb', False))
        is_f5 = bool(row.get('is_f5', False))
        
        start = start_ms
        end = end_ms
        all_data.append({
            'start': start,
            'end': end,
            'duration': latency,
            'location': '云下',
            'trace_id': row.get('trace_id', ''),
            'log_id': row.get('log_id', ''),
            'msgType': row.get('msgType', ''),
            'deployUnitObjectName': row.get('deployUnitObjectName', ''),
            'global_id': row.get('global_id', ''),
            'chain_src_key': row.get('chain_src_key', ''),
            'chain_dst_key': row.get('chain_dst_key', ''),
            'src_ip': row.get('src_ip', ''),
            'srcSysName': row.get('srcSysName', ''),
            'start_time': datetime.fromtimestamp(start_ms / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] if start_ms else '',
            'is_esb': is_esb,
            'is_f5': is_f5,
            # 排序字段映射
            'sort_timestamp': start,
            'sort_send_time': start,
            'sort_rtn_time': end,
            'data_source': 'apm',
        })
    
    # 处理云上数据
    for idx, row in df_up.iterrows():
        # 云上数据的 start_time 可能是时间戳或时间字符串，需要转换
        start_time = row.get('start_time')
        if pd.isna(start_time):
            continue
        
        # 尝试转换为毫秒时间戳
        if isinstance(start_time, (int, float)):
            start_ms = int(start_time)
        elif isinstance(start_time, str):
            try:
                # 尝试解析时间字符串
                if 'T' in start_time:
                    dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                else:
                    dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
                start_ms = int(dt.timestamp() * 1000)
            except:
                logger.warning(f"无法解析云上数据时间: {start_time}")
                continue
        else:
            continue
        
        response_duration = int(row.get('response_duration', 0))
        end_ms = start_ms + response_duration
        
        start = start_ms
        end = end_ms
        all_data.append({
            'start': start,
            'end': end,
            'duration': response_duration,
            'location': '云上',
            'span_id': row.get('span_id', ''),
            'parent_span_id': row.get('parent_span_id', ''),
            'log_id': row.get('log_id', ''),
            'msgType': row.get('msg_type', ''),
            'deployUnitObjectName': row.get('physics_component_unit_name_desc', ''),
            'global_id': row.get('global_id', ''),
            'chain_src_key': '',  # 云上数据没有 chain_src_key，设为空字符串
            'chain_dst_key': '',  # 云上数据没有 chain_dst_key，设为空字符串
            'source': row.get('source', ''),  # 添加 source 字段
            'start_time': datetime.fromtimestamp(start_ms / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] if start_ms else '',
            'is_esb': False,  # 云上数据没有ESB标识
            'is_f5': False,   # 云上数据没有F5标识
            # 'src_ip': row.get('src_ip', ''),
            # 'srcSysName': row.get('srcSysName', ''),
            'server_ip': row.get('server_ip', ''),
            'service_status': row.get('service_status', ''),
            # 排序字段映射
            'sort_timestamp': start,
            'sort_send_time': start,
            'sort_rtn_time': end,
            'data_source': 'link',
        })
    # 追加 full_link_log 数据
    if df_full_link is not None and not df_full_link.empty:
        def _to_ms(v):
            if v is None or pd.isna(v):
                return None
            if isinstance(v, datetime):
                return int(v.timestamp() * 1000)
            if isinstance(v, str):
                try:
                    dt = datetime.fromisoformat(v.replace('Z', '+00:00')) if 'T' in v else datetime.strptime(v, "%Y-%m-%d %H:%M:%S")
                    return int(dt.timestamp() * 1000)
                except Exception:
                    return None
            if isinstance(v, (int, float)):
                return int(v)
            return None

        for _, row in df_full_link.iterrows():
            log_ts_ms = _to_ms(row.get('log_timestamp'))
            send_ms = _to_ms(row.get('send_time'))
            rtn_ms = _to_ms(row.get('rtn_time'))

            # 时间段：优先用 send~rtn，否则退化为时间点（使用 log_timestamp）
            if send_ms is not None and rtn_ms is not None:
                start_ms = send_ms
                end_ms = rtn_ms
            else:
                start_ms = log_ts_ms
                end_ms = log_ts_ms

            start = start_ms or 0
            end = end_ms or start

            module_type = str(row.get('module_type', '') or '').strip().upper()

            all_data.append({
                'start': start,
                'end': end,
                'duration': (end_ms - start_ms) if (start_ms is not None and end_ms is not None) else 0,
                'location': '全链路',
                'trace_id': None,
                'log_id': row.get('log_id', ''),
                'global_id': row.get('global_id', ''),
                'msgType': row.get('msg_type', ''),
                'deployUnitObjectName': row.get('server_name', ''),
                'chain_src_key': '',
                'chain_dst_key': '',
                # 'src_ip': row.get('request_ip', ''),
                # 'srcSysName': row.get('send_sysname', ''),
                'request_ip': row.get('request_ip', ''),
                'start_time': datetime.fromtimestamp((log_ts_ms or start) / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] if (log_ts_ms or start) else '',
                'is_esb': False,
                'is_f5': False,
                'span_id': row.get('span_id', ''),
                'parent_span_id': row.get('exts_span_id', ''),
                'source': 'full_link_log',
                'service_status': row.get('response_status', ''),
                'module_type': module_type,
                'log_timestamp': log_ts_ms,
                'send_time_ms': send_ms,
                'rtn_time_ms': rtn_ms,
                # 排序字段
                'sort_timestamp': log_ts_ms,
                'sort_send_time': send_ms,
                'sort_rtn_time': rtn_ms,
                'data_source': 'full_link_log',
            })

    # 排序规则（统一三类数据）：
    # 1) sort_timestamp 升序
    # 2) sort_send_time 升序（None 排后）
    # 3) sort_rtn_time 降序（None 排后）
    # 4) 再叠加原有的链路关系与云上 source 规则

    def _compare_gantt_row(a: Dict, b: Dict) -> int:
        def _cmp_ts(ax, bx, reverse: bool = False) -> int:
            a_none = ax is None
            b_none = bx is None
            if a_none and b_none:
                return 0
            if a_none != b_none:
                # None 排在最后
                return 1 if a_none else -1
            if ax == bx:
                return 0
            if not reverse:
                return -1 if ax < bx else 1
            else:
                return -1 if ax > bx else 1

        # 1) 统一时间排序
        c = _cmp_ts(a.get('sort_timestamp'), b.get('sort_timestamp'), reverse=False)
        if c != 0:
            return c

        # 2) send_time（None 排后）
        c = _cmp_ts(a.get('sort_send_time'), b.get('sort_send_time'), reverse=False)
        if c != 0:
            return c

        # 3) rtn_time（降序，None 排后）
        c = _cmp_ts(a.get('sort_rtn_time'), b.get('sort_rtn_time'), reverse=True)
        if c != 0:
            return c

        # 4) 保留原有开始/结束时间及链路关系排序逻辑
        if a['start'] != b['start']:
            return -1 if a['start'] < b['start'] else 1
        if a['end'] != b['end']:
            return -1 if a['end'] > b['end'] else 1

        a_src = (a.get('chain_src_key') or '').strip()
        a_dst = (a.get('chain_dst_key') or '').strip()
        b_src = (b.get('chain_src_key') or '').strip()
        b_dst = (b.get('chain_dst_key') or '').strip()

        # 如果 a 的输出正好是 b 的输入，则 a 应在 b 之前
        if a_dst and b_src and a_dst == b_src:
            return -1
        # 如果 b 的输出正好是 a 的输入，则 b 应在 a 之前
        if b_dst and a_src and b_dst == a_src:
            return 1

        # 对于云上数据，如果开始结束时间完全相同，transaction 排在 transaction_action 前面
        a_location = a.get('location', '')
        b_location = b.get('location', '')
        if a_location == '云上' and b_location == '云上':
            a_source = a.get('source', '')
            b_source = b.get('source', '')
            if a_source != 'transaction_action' and b_source == 'transaction_action':
                return -1
            if a_source == 'transaction_action' and b_source != 'transaction_action':
                return 1

        # 否则保持相对顺序
        return 0

    all_data.sort(key=cmp_to_key(_compare_gantt_row))
    
    if not all_data:
        # 如果没有数据，返回空图表
        fig.add_annotation(
            text="暂无数据",
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=16)
        )
        return fig, [], []
    
    # 为每条数据分配 y 轴位置（从上到下，最先发生的在最上方）
    # 使用索引作为唯一标识，正序排列（第一个在顶部）
    for i, data in enumerate(all_data):
        data['_index'] = i
        data['_y_pos'] = i + 1  # 正序，第一个在顶部
    
    # 绘制云下数据 - 根据ESB/F5使用不同颜色
    # 顺序：云下(F5), 云下（ESB实例）, 云下实例
    down_data = [d for d in all_data if d['location'] == '云下']
    if down_data:
        # 按ESB、F5、普通云下分组
        esb_data = [d for d in down_data if d.get('is_esb', False)]
        f5_data = [d for d in down_data if d.get('is_f5', False)]
        normal_down_data = [d for d in down_data if not d.get('is_esb', False) and not d.get('is_f5', False)]
        
        # 1. 绘制F5数据（紫色）- 第一个
        if f5_data:
            for idx, d in enumerate(f5_data):
                start_dt = datetime.fromtimestamp(d['start'] / 1000)
                end_dt = datetime.fromtimestamp(d['end'] / 1000)
                hover_text = (
                    f"<b>云下数据(F5)</b><br>"
                    f"trace_id: {truncate_text(d.get('trace_id', ''), 30)}<br>"
                    f"log_id: {truncate_text(d.get('log_id', ''), 60)}<br>"
                    f"msgType: {truncate_text(d.get('msgType', ''), 80)}<br>"
                    f"开始时间: {d.get('start_time', '')}<br>"
                    f"持续时间: {d.get('duration', 0)}ms<br>"
                    f"组件: {truncate_text(d.get('deployUnitObjectName', ''), 100)}<br>"
                    f"global_id: {truncate_text(d.get('global_id', ''), 50)}<br>"
                    f"chain_src_key: {truncate_text(d.get('chain_src_key', ''), 30)}<br>"
                    f"chain_dst_key: {truncate_text(d.get('chain_dst_key', ''), 30)}<br>"
                    f"src_ip: {truncate_text(d.get('src_ip', ''), 30)}<br>"
                    f"srcSysName: {truncate_text(d.get('srcSysName', ''), 50)}"
                )
                fig.add_trace(go.Scatter(
                    x=[start_dt, end_dt],
                    y=[d['_y_pos'], d['_y_pos']],
                    mode='lines+markers',
                    name='云下(F5)',
                    legendgroup='down_f5',
                    line=dict(color='#722ed1', width=2),
                    marker=dict(size=4, color='#722ed1'),
                    hovertemplate=hover_text + '<extra></extra>',
                    showlegend=(idx == 0),
                ))

                # 在线段右侧末端添加文字：src_ip, dst_ip, msgType
                src_ip_raw = d.get('chain_src_key')
                dst_ip_raw = d.get('chain_dst_key')
                msg_type_raw = d.get('msgType')

                src_ip = '' if src_ip_raw is None or pd.isna(src_ip_raw) else str(src_ip_raw).split(':')[0].strip()
                dst_ip = '' if dst_ip_raw is None or pd.isna(dst_ip_raw) else str(dst_ip_raw).split(':')[0].strip()
                msg_type = '' if msg_type_raw is None or pd.isna(msg_type_raw) else str(msg_type_raw).strip()

                label_parts = []
                if src_ip:
                    label_parts.append(f"{src_ip}->")
                if dst_ip:
                    label_parts.append(f"{dst_ip}")
                if msg_type:
                    label_parts.append(f"  {msg_type}")
                label_text = ''.join(label_parts).strip()
                if label_text:
                    fig.add_annotation(
                        x=end_dt,
                        y=d['_y_pos'],
                        text=label_text,
                        showarrow=False,
                        xanchor='left',
                        yanchor='middle',
                        xshift=6,
                        font=dict(size=10, color='#333'),
                        align='left',
                    )
        
        # 2. 绘制ESB数据（绿色）- 第二个
        if esb_data:
            for idx, d in enumerate(esb_data):
                start_dt = datetime.fromtimestamp(d['start'] / 1000)
                end_dt = datetime.fromtimestamp(d['end'] / 1000)
                hover_text = (
                    f"<b>云下数据(ESB)</b><br>"
                    f"trace_id: {truncate_text(d.get('trace_id', ''), 30)}<br>"
                    f"log_id: {truncate_text(d.get('log_id', ''), 60)}<br>"
                    f"msgType: {truncate_text(d.get('msgType', ''), 80)}<br>"
                    f"开始时间: {d.get('start_time', '')}<br>"
                    f"持续时间: {d.get('duration', 0)}ms<br>"
                    f"组件: {truncate_text(d.get('deployUnitObjectName', ''), 100)}<br>"
                    f"global_id: {truncate_text(d.get('global_id', ''), 50)}<br>"
                    f"chain_src_key: {truncate_text(d.get('chain_src_key', ''), 30)}<br>"
                    f"chain_dst_key: {truncate_text(d.get('chain_dst_key', ''), 30)}<br>"
                    f"src_ip: {truncate_text(d.get('src_ip', ''), 30)}<br>"
                    f"srcSysName: {truncate_text(d.get('srcSysName', ''), 50)}"
                )
                fig.add_trace(go.Scatter(
                    x=[start_dt, end_dt],
                    y=[d['_y_pos'], d['_y_pos']],
                    mode='lines+markers',
                    name='云下(ESB实例)',
                    legendgroup='down_esb',
                    line=dict(color='#52c41a', width=2),
                    marker=dict(size=4, color='#52c41a'),
                    hovertemplate=hover_text + '<extra></extra>',
                    showlegend=(idx == 0),
                ))

                # 在线段右侧末端添加文字：src_ip, dst_ip, msgType
                src_ip_raw = d.get('chain_src_key')
                dst_ip_raw = d.get('chain_dst_key')
                msg_type_raw = d.get('msgType')

                src_ip = '' if src_ip_raw is None or pd.isna(src_ip_raw) else str(src_ip_raw).split(':')[0].strip()
                dst_ip = '' if dst_ip_raw is None or pd.isna(dst_ip_raw) else str(dst_ip_raw).split(':')[0].strip()
                msg_type = '' if msg_type_raw is None or pd.isna(msg_type_raw) else str(msg_type_raw).strip()

                label_parts = []
                if src_ip:
                    label_parts.append(f"{src_ip}->")
                if dst_ip:
                    label_parts.append(f"{dst_ip}")
                if msg_type:
                    label_parts.append(f"  {msg_type}")
                label_text = ''.join(label_parts).strip()
                if label_text:
                    fig.add_annotation(
                        x=end_dt,
                        y=d['_y_pos'],
                        text=label_text,
                        showarrow=False,
                        xanchor='left',
                        yanchor='middle',
                        xshift=6,
                        font=dict(size=10, color='#333'),
                        align='left',
                    )
        
        # 3. 绘制普通云下数据（蓝色）- 第三个
        if normal_down_data:
            for idx, d in enumerate(normal_down_data):
                start_dt = datetime.fromtimestamp(d['start'] / 1000)
                end_dt = datetime.fromtimestamp(d['end'] / 1000)
                hover_text = (
                    f"<b>云下数据</b><br>"
                    f"trace_id: {truncate_text(d.get('trace_id', ''), 30)}<br>"
                    f"log_id: {truncate_text(d.get('log_id', ''), 60)}<br>"
                    f"msgType: {truncate_text(d.get('msgType', ''), 80)}<br>"
                    f"开始时间: {d.get('start_time', '')}<br>"
                    f"持续时间: {d.get('duration', 0)}ms<br>"
                    f"组件: {truncate_text(d.get('deployUnitObjectName', ''), 100)}<br>"
                    f"global_id: {truncate_text(d.get('global_id', ''), 50)}<br>"
                    f"chain_src_key: {truncate_text(d.get('chain_src_key', ''), 30)}<br>"
                    f"chain_dst_key: {truncate_text(d.get('chain_dst_key', ''), 30)}<br>"
                    f"src_ip: {truncate_text(d.get('src_ip', ''), 30)}<br>"
                    f"srcSysName: {truncate_text(d.get('srcSysName', ''), 50)}"
                )
                fig.add_trace(go.Scatter(
                    x=[start_dt, end_dt],
                    y=[d['_y_pos'], d['_y_pos']],
                    mode='lines+markers',
                    name='云下实例',
                    legendgroup='down_normal',
                    line=dict(color='#1890ff', width=2),
                    marker=dict(size=4, color='#1890ff'),
                    hovertemplate=hover_text + '<extra></extra>',
                    showlegend=(idx == 0),
                ))

                # 在线段右侧末端添加文字：src_ip, dst_ip, msgType
                src_ip_raw = d.get('chain_src_key')
                dst_ip_raw = d.get('chain_dst_key')
                msg_type_raw = d.get('msgType')

                src_ip = '' if src_ip_raw is None or pd.isna(src_ip_raw) else str(src_ip_raw).split(':')[0].strip()
                dst_ip = '' if dst_ip_raw is None or pd.isna(dst_ip_raw) else str(dst_ip_raw).split(':')[0].strip()
                msg_type = '' if msg_type_raw is None or pd.isna(msg_type_raw) else str(msg_type_raw).strip()

                label_parts = []
                if src_ip:
                    label_parts.append(f"{src_ip}->")
                if dst_ip:
                    label_parts.append(f"{dst_ip}")
                if msg_type:
                    label_parts.append(f"  {msg_type}")
                label_text = ''.join(label_parts).strip()
                if label_text:
                    fig.add_annotation(
                        x=end_dt,
                        y=d['_y_pos'],
                        text=label_text,
                        showarrow=False,
                        xanchor='left',
                        yanchor='middle',
                        xshift=6,
                        font=dict(size=10, color='#333'),
                        align='left',
                    )
    
    # 绘制云上数据 - 使用 Scatter 绘制线段
    # source 等于 'transaction_action' 时为橙色，否则为红色
    up_data = [d for d in all_data if d['location'] == '云上']
    if up_data:
        # 跟踪已显示图例的类型
        shown_legend_types = set()
        for idx, d in enumerate(up_data):
            # 将毫秒时间戳转换为 datetime 对象
            start_dt = datetime.fromtimestamp(d['start'] / 1000)
            end_dt = datetime.fromtimestamp(d['end'] / 1000)
            
            # 根据 source 决定颜色
            source = d.get('source', '')
            if source == 'transaction_action':
                color = '#ff7f0e'  # 橙色
                name = '云上(transaction_action)'
                legend_key = 'transaction_action'
            else:
                color = '#ff0000'  # 红色
                name = '云上(transaction)'
                legend_key = 'other'
            
            # 判断是否显示图例：该类型的第一条数据显示图例
            show_legend = legend_key not in shown_legend_types
            if show_legend:
                shown_legend_types.add(legend_key)
            
            hover_text = (
                f"<b>云上数据</b><br>"
                f"source: {truncate_text(source, 30)}<br>"
                f"span_id: {truncate_text(d.get('span_id', ''), 30)}<br>"
                f"parent_span_id: {truncate_text(d.get('parent_span_id', ''), 30)}<br>"
                f"log_id: {truncate_text(d.get('log_id', ''), 60)}<br>"
                f"msgType: {truncate_text(d.get('msgType', ''), 80)}<br>"
                f"开始时间: {d.get('start_time', '')}<br>"
                f"持续时间: {d.get('duration', 0)}ms<br>"
                f"组件: {truncate_text(d.get('deployUnitObjectName', ''), 100)}<br>"
                f"global_id: {truncate_text(d.get('global_id', ''), 50)}<br>"
                f"server_ip: {truncate_text(d.get('server_ip', ''), 30)}<br>"
                f"service_status: {truncate_text(d.get('service_status', ''), 50)}"
            )
            
            fig.add_trace(go.Scatter(
                x=[start_dt, end_dt],
                y=[d['_y_pos'], d['_y_pos']],
                mode='lines+markers',
                name=name,
                legendgroup='up_' + legend_key,  # 云上数据按类型分组
                line=dict(color=color, width=2),
                marker=dict(size=4, color=color),
                hovertemplate=hover_text + '<extra></extra>',
                showlegend=show_legend,  # 每种类型的第一条显示图例
            ))

            # 在线段右侧末端添加文字：server_ip, msg_type
            server_ip_raw = d.get('server_ip')
            msg_type_raw = d.get('msgType')

            server_ip = '' if server_ip_raw is None or pd.isna(server_ip_raw) else str(server_ip_raw).strip()
            msg_type = '' if msg_type_raw is None or pd.isna(msg_type_raw) else str(msg_type_raw).strip()

            label_parts = []
            if server_ip:
                label_parts.append(f"{server_ip}")
            if msg_type:
                label_parts.append(f"{msg_type}")
            label_text = '  '.join(label_parts).strip()
            if label_text:
                fig.add_annotation(
                    x=end_dt,
                    y=d['_y_pos'],
                    text=label_text,
                    showarrow=False,
                    xanchor='left',
                    yanchor='middle',
                    xshift=6,
                    font=dict(size=10, color='#333'),
                    align='left',
                )
    
    # 绘制全链路 full_link_log 数据
    full_link_data = [d for d in all_data if d.get('data_source') == 'full_link_log']
    if full_link_data:
        # 跟踪已显示图例的组
        shown_legend_groups = set()
        # CS/CR 一组，SR/SS 一组
        for idx, d in enumerate(full_link_data):
            start_ms = d['start']
            end_ms = d['end']
            log_ts_ms = d.get('log_timestamp') or d['start']
            module_type = (d.get('module_type') or '').upper()

            # 颜色与形状：每个 module_type 独立
            if module_type == 'CS':
                color = '#ffd700'  # 金色
                legend_name = '全链路(CS)'
                legend_group = 'fulllink_cs'
                marker_symbol = 'triangle-up-open'
                line_style = 'dash'  # 虚线
                legend_rank = 10
            elif module_type == 'CR':
                color = '#ffd700'  # 金色
                legend_name = '全链路(CR)'
                legend_group = 'fulllink_cr'
                marker_symbol = 'triangle-up'
                line_style = 'solid'  # 实线
                legend_rank = 20
            elif module_type == 'SR':
                color = '#13c2c2'  # 青色
                legend_name = '全链路(SR)'
                legend_group = 'fulllink_sr'
                marker_symbol = 'square-open'
                line_style = 'dash'  # 虚线
                legend_rank = 30
            else:  # SS
                color = '#13c2c2'  # 青色
                legend_name = '全链路(SS)'
                legend_group = 'fulllink_ss'
                marker_symbol = 'square'
                line_style = 'solid'  # 实线
                legend_rank = 40

            # y 轴位置
            y_pos = d['_y_pos']

            start_dt = datetime.fromtimestamp(d['start'] / 1000)
            end_dt = datetime.fromtimestamp(d['end'] / 1000)

            # 格式化悬停信息中的时间
            send_time_str = datetime.fromtimestamp(d['send_time_ms'] / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] if d.get('send_time_ms') else '无'
            rtn_time_str = datetime.fromtimestamp(d['rtn_time_ms'] / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] if d.get('rtn_time_ms') else '无'

            hover_text = (
                f"<b>全链路日志</b><br>"
                f"module_type: {truncate_text(module_type, 10)}<br>"
                f"log_timestamp: {truncate_text(d.get('start_time', ''), 50)}<br>"
                f"send_time: {send_time_str}<br>"
                f"rtn_time: {rtn_time_str}<br>"
                f"log_id: {truncate_text(d.get('log_id', ''), 60)}<br>"
                f"global_id: {truncate_text(d.get('global_id', ''), 50)}<br>"
                f"msgType: {truncate_text(d.get('msgType', ''), 80)}<br>"
                f"server_name: {truncate_text(d.get('deployUnitObjectName', ''), 100)}<br>"
                f"request_ip: {truncate_text(d.get('request_ip', ''), 30)}<br>"
                f"response_status: {truncate_text(d.get('service_status', ''), 50)}"
            )

            # Check if legend for this group has been shown
            show_legend_for_group = legend_group not in shown_legend_groups
            if show_legend_for_group:
                shown_legend_groups.add(legend_group)

            # 获取 send_time 和 rtn_time
            send_time_ms = d.get('send_time_ms')
            rtn_time_ms = d.get('rtn_time_ms')
            
            # 判断 send_time 和 rtn_time 的情况
            has_send = send_time_ms is not None
            has_rtn = rtn_time_ms is not None
            
            # 画线（如果 send_time 和 rtn_time 都有值）
            if has_send and has_rtn:
                send_dt = datetime.fromtimestamp(send_time_ms / 1000)
                rtn_dt = datetime.fromtimestamp(rtn_time_ms / 1000)
                fig.add_trace(go.Scatter(
                    x=[send_dt, rtn_dt],
                    y=[y_pos, y_pos],
                    mode='lines',
                    name=legend_name,
                    legendgroup=legend_group,
                    line=dict(color=color, width=2),
                    hovertemplate=hover_text + '<extra></extra>',
                    showlegend=False,  # 标记的 trace 显示图例，线不显示
                ))
            # 如果只有 send_time 有值，在 send_time 位置画一个圆点
            elif has_send and not has_rtn:
                send_dt = datetime.fromtimestamp(send_time_ms / 1000)
                fig.add_trace(go.Scatter(
                    x=[send_dt],
                    y=[y_pos],
                    mode='markers',
                    name=legend_name,
                    legendgroup=legend_group,
                    marker=dict(
                        size=2,
                        color=color,
                        symbol='circle',  # 使用圆点
                        line=dict(width=2, color=color),
                    ),
                    hovertemplate=hover_text + '<extra></extra>',
                    showlegend=False,  # 不显示图例，log_timestamp 点会显示图例
                ))
            # 如果只有 rtn_time 有值，在 rtn_time 位置画一个圆点
            elif not has_send and has_rtn:
                rtn_dt = datetime.fromtimestamp(rtn_time_ms / 1000)
                fig.add_trace(go.Scatter(
                    x=[rtn_dt],
                    y=[y_pos],
                    mode='markers',
                    name=legend_name,
                    legendgroup=legend_group,
                    marker=dict(
                        size=10,
                        color=color,
                        symbol='circle',  # 使用圆点
                        line=dict(width=2, color=color),
                    ),
                    hovertemplate=hover_text + '<extra></extra>',
                    showlegend=False,  # 不显示图例，log_timestamp 点会显示图例
                ))

            # 画 timestamp 点（使用 log_timestamp）- 这个 trace 用于显示图例中的标记形状
            # log_timestamp 点始终显示（用于图例和标记）
            ts_dt = datetime.fromtimestamp(log_ts_ms / 1000) if log_ts_ms else None
            if ts_dt:
                fig.add_trace(go.Scatter(
                    x=[ts_dt],
                    y=[y_pos],
                    mode='markers',
                    name=legend_name,
                    legendgroup=legend_group,
                    marker=dict(
                        size=10,
                        color=color,
                        symbol=marker_symbol,
                        line=dict(width=2, color=color),
                    ),
                    hovertemplate=hover_text + '<extra></extra>',
                    showlegend=show_legend_for_group,  # 标记的 trace 显示图例，这样图例中会显示标记形状
                    legendrank=legend_rank,
                ))

                # 在线段右侧末端添加文字：request_ip, msg_type
                request_ip_raw = d.get('request_ip')
                msg_type_raw = d.get('msgType')

                request_ip = '' if request_ip_raw is None or pd.isna(request_ip_raw) else str(request_ip_raw).strip()
                msg_type = '' if msg_type_raw is None or pd.isna(msg_type_raw) else str(msg_type_raw).strip()

                label_parts = []
                if request_ip:
                    label_parts.append(f"{request_ip}")
                if msg_type:
                    label_parts.append(f"{msg_type}")
                label_text = '  '.join(label_parts).strip()
                if label_text:
                    fig.add_annotation(
                        x=ts_dt,
                        y=y_pos,
                        text=label_text,
                        showarrow=False,
                        xanchor='left',
                        yanchor='middle',
                        xshift=6,
                        font=dict(size=10, color='#333'),
                        align='left',
                    )
    
    # 计算时间范围，决定显示格式
    if all_data:
        time_span = max(d['end'] for d in all_data) - min(d['start'] for d in all_data)
        # 如果时间跨度小于1秒，显示毫秒；否则只显示到秒
        if time_span < 1000:
            tickformat = '%H:%M:%S.%L'  # 显示毫秒
        else:
            tickformat = '%H:%M:%S'  # 只显示到秒
    else:
        tickformat = '%H:%M:%S'
    
    # 计算y轴范围：从1到最大y值，然后反转（使第一个在顶部）
    max_y = len(all_data) if all_data else 1
    y_range = [max_y + 0.5, 0.5]  # 反转y轴，使第一个（时间最早的）在顶部
    
    # 设置布局
    fig.update_layout(
        title='时间跨度甘特图',
        legend=dict(groupclick='togglegroup'),
        xaxis_title='时间',
        yaxis_title='数据行（按时间顺序，从上到下）',
        barmode='overlay',
        height=max(400, len(all_data) * 30),
        hovermode='closest',
        xaxis=dict(
            type='date',
            tickformat=tickformat,
            tickangle=-45,  # 倾斜标签以便阅读
            showgrid=True,  # 显示网格线
        ),
        yaxis=dict(
            tickmode='linear',
            tick0=1,
            dtick=1,
            range=y_range,  # 明确设置y轴范围并反转，避免出现负数
            autorange=False,  # 禁用自动调整范围
            fixedrange=True,  # 固定y轴范围，防止缩放时改变
        ),
    )
    
    # 生成表格数据，顺序与图由上到下的顺序一致
    table_data = []
    for data in all_data:
        row = {
            'location': data.get('location', ''),
            'trace_id': data.get('trace_id', ''),
            'log_id': data.get('log_id', ''),
            'global_id': data.get('global_id', ''),
            'msgType': data.get('msgType', ''),
            'start_time': data.get('start_time', ''),
            'duration': data.get('duration', 0),
            'deployUnitObjectName': data.get('deployUnitObjectName', ''),
            'chain_src_key': data.get('chain_src_key', ''),
            'chain_dst_key': data.get('chain_dst_key', ''),
            'is_esb': '是' if data.get('is_esb', False) else '否',
            'is_f5': '是' if data.get('is_f5', False) else '否',
            'src_ip': data.get('src_ip', ''),
            'srcSysName': data.get('srcSysName', ''),
        }
        table_data.append(row)
    
    return fig, table_data, all_data


def build_mermaid_topology(all_data: List[Dict], show_f5: bool = True) -> Tuple[str, Dict, List[Dict]]:
    """
    根据时间包含关系构建 mermaid 拓扑图
    
    参数:
        all_data: 甘特图数据列表
        show_f5: 是否显示 F5 节点
    
    返回:
        (mermaid 文本, 节点数据字典, 边数据列表)
    """
    import json
    
    if not all_data:
        return "graph TD; A[暂无数据]", {}, []
    
    # 过滤 F5 数据（如果需要）
    if not show_f5:
        all_data = [d for d in all_data if not d.get('is_f5', False)]
    
    # 每个数据行对应一个节点（不聚合）
    node_data = {}  # node_id -> 节点详细信息
    data_to_node_id = {}  # 数据索引 -> node_id 的映射
    
    for idx, data in enumerate(all_data):
        node_id = f"N_{idx}"
        data_to_node_id[idx] = node_id
        
        deploy_unit_raw = data.get('deployUnitObjectName', '')
        if deploy_unit_raw is None or (hasattr(pd, 'isna') and pd.isna(deploy_unit_raw)):
            deploy_unit = ''
        else:
            deploy_unit = str(deploy_unit_raw).strip()
        if not deploy_unit or deploy_unit == 'nan':
            deploy_unit = '未知组件'
        
        # 确定节点颜色
        location = data.get('location', '')
        is_esb = data.get('is_esb', False)
        is_f5 = data.get('is_f5', False)
        source = data.get('source', '')
        
        if location == '云下':
            if is_f5:
                node_color = '#722ed1'  # 紫色
                node_type = '云下(F5)'
            elif is_esb:
                node_color = '#52c41a'  # 绿色
                node_type = '云下(ESB)'
            else:
                node_color = '#1890ff'  # 蓝色
                node_type = '云下'
        else:  # 云上
            if source == 'transaction_action':
                node_color = '#ff7f0e'  # 橙色
                node_type = '云上(transaction_action)'
            else:
                node_color = '#ff0000'  # 红色
                node_type = '云上(transaction)'
        
        # 每个节点只包含一条记录
        node_data[node_id] = {
            'label': deploy_unit,
            'color': node_color,
            'node_type': node_type,
            'location': location,
            'all_records': [data]  # 只包含当前数据行
        }
    
    # 构建边：简单逻辑 - 甘特图从上到下，检查两两数据之间是否有时间包含关系
    # 如果有包含关系，就在拓扑图中加入调用边
    edges = []  # [(src_node_id, dst_node_id)]
    edge_data_list = []  # 边数据列表，用于 tooltip
    edge_set = set()  # 用于去重
    
    # 辅助函数：检查节点 i 是否包含节点 j
    def contains(i_idx, j_idx):
        """检查节点 i 的时间范围是否包含节点 j"""
        data_i = all_data[i_idx]
        data_j = all_data[j_idx]
        start_i = data_i.get('start', 0)
        end_i = data_i.get('end', 0)
        start_j = data_j.get('start', 0)
        end_j = data_j.get('end', 0)
        
        if start_i <= 0 or end_i <= 0 or end_i <= start_i:
            return False
        if start_j <= 0 or end_j <= 0 or end_j <= start_j:
            return False
        
        # i 包含 j：i 的开始时间 <= j 的开始时间 且 i 的结束时间 >= j 的结束时间
        return start_i <= start_j and end_i >= end_j
    
    # 辅助函数：检查两个节点之间是否满足调用关系条件
    def check_call_relation(src_idx, dst_idx):
        """
        检查 src -> dst 是否满足调用关系条件
        返回 True 表示满足条件
        """
        src_data = all_data[src_idx]
        dst_data = all_data[dst_idx]
        src_location = src_data.get('location', '')
        dst_location = dst_data.get('location', '')
        
        # 如果父节点和子节点都是云下数据，需要检查 chain_dst_key 和 chain_src_key 是否匹配
        if src_location == '云下' and dst_location == '云下':
            src_chain_dst_key = str(src_data.get('chain_dst_key', '') or '').strip()
            dst_chain_src_key = str(dst_data.get('chain_src_key', '') or '').strip()
            
            # 如果 chain_dst_key 或 chain_src_key 为空，不满足条件
            if not src_chain_dst_key or not dst_chain_src_key:
                return False
            
            # 检查是否匹配
            return src_chain_dst_key == dst_chain_src_key
        
        # 如果是云下->云上，需要检查：msgType相同 且 云下组件以ESB开头
        elif src_location == '云下' and dst_location == '云上':
            src_msg_type = str(src_data.get('msgType', '') or '').strip()
            dst_msg_type = str(dst_data.get('msgType', '') or '').strip()
            src_deploy_unit_raw = src_data.get('deployUnitObjectName', '')
            if src_deploy_unit_raw is None or (hasattr(pd, 'isna') and pd.isna(src_deploy_unit_raw)):
                src_deploy_unit = ''
            else:
                src_deploy_unit = str(src_deploy_unit_raw).strip()
            
            # 检查 msgType 是否相同
            if not src_msg_type or not dst_msg_type or src_msg_type != dst_msg_type:
                return False
            
            # 检查云下组件是否以 ESB 开头
            if not src_deploy_unit.startswith('ESB'):
                return False
            
            return True
        
        else:
            # 其他情况（云上->云下、云上->云上），只需要时间包含关系即可
            return True
    
    # 第一步：收集所有可能的边（满足时间包含关系和调用条件）
    potential_edges = []  # [(src_idx, dst_idx, src_node_id, dst_node_id)]
    
    for i, data_i in enumerate(all_data):
        node_i_id = data_to_node_id.get(i)
        if not node_i_id:
            continue
        
        for j, data_j in enumerate(all_data):
            if i == j:
                continue
            
            node_j_id = data_to_node_id.get(j)
            if not node_j_id:
                continue
            
            # 检查 i 是否包含 j
            i_contains_j = contains(i, j)
            # 检查 j 是否包含 i
            j_contains_i = contains(j, i)
            
            # 确定边的方向
            if i_contains_j and not j_contains_i:
                # i 包含 j，且 j 不包含 i：i -> j
                src_idx, dst_idx = i, j
                src_node_id, dst_node_id = node_i_id, node_j_id
            elif j_contains_i and not i_contains_j:
                # j 包含 i，且 i 不包含 j：j -> i
                src_idx, dst_idx = j, i
                src_node_id, dst_node_id = node_j_id, node_i_id
            elif i_contains_j and j_contains_i:
                # 互为包含关系（时间完全相同）：按照甘特图顺序，上面的（索引小的）指向下面的（索引大的）
                if i < j:
                    src_idx, dst_idx = i, j
                    src_node_id, dst_node_id = node_i_id, node_j_id
                else:
                    continue  # 已经处理过了（i > j 的情况会在 j 的循环中处理）
            else:
                # 没有包含关系，跳过
                continue
            
            # 检查是否满足调用关系条件
            if not check_call_relation(src_idx, dst_idx):
                continue
            
            # 添加到候选边列表
            potential_edges.append((src_idx, dst_idx, src_node_id, dst_node_id))
    
    # 第二步：过滤间接调用关系
    # 对于涉及云上数据的边（云下->云上、云上->云下、云上->云上），检查是否存在间接路径
    direct_edges = []
    
    for src_idx, dst_idx, src_node_id, dst_node_id in potential_edges:
        src_data = all_data[src_idx]
        dst_data = all_data[dst_idx]
        src_location = src_data.get('location', '')
        dst_location = dst_data.get('location', '')
        
        # 如果是纯云下调用（云下->云下），不需要检查间接关系（已经通过 chain_key 保证了直接关系）
        if src_location == '云下' and dst_location == '云下':
            direct_edges.append((src_idx, dst_idx, src_node_id, dst_node_id))
            continue
        
        # 如果涉及云上数据，需要检查是否存在间接路径
        # 检查是否存在中间节点 k，使得 src->k->dst（间接关系）
        is_direct = True
        # for k, data_k in enumerate(all_data):
        #     if k == src_idx or k == dst_idx:
        #         continue
        #
        #     # 检查 src 是否包含 k（时间包含关系），且 k 是否包含 dst（时间包含关系）
        #     src_contains_k = contains(src_idx, k)
        #     k_contains_dst = contains(k, dst_idx)
        #
        #     if not (src_contains_k and k_contains_dst):
        #         continue  # src 不包含 k 或 k 不包含 dst，不是中间节点
        #
        #     # 检查 src->k 和 k->dst 是否都满足调用关系条件
        #     src_to_k_ok = check_call_relation(src_idx, k)
        #     k_to_dst_ok = check_call_relation(k, dst_idx)
        #
        #     # 如果 src->k 和 k->dst 都满足条件，那么 src->dst 是间接关系
        #     if src_to_k_ok and k_to_dst_ok:
        #         is_direct = False
        #         logger.info(f"移除间接调用关系: {src_data.get('deployUnitObjectName', '')} -> {dst_data.get('deployUnitObjectName', '')} (通过 {data_k.get('deployUnitObjectName', '')})")
        #         break
        #
        # 如果是直接关系，添加到结果列表
        if is_direct:
            direct_edges.append((src_idx, dst_idx, src_node_id, dst_node_id))
    
    # 第三步：构建最终的边和边数据
    # 按先后顺序排序（优先按目标节点开始时间，其次按源节点开始时间，再按索引保证稳定）
    direct_edges_sorted = sorted(
        direct_edges,
        key=lambda t: (
            all_data[t[1]].get('start', 0) or 0,
            all_data[t[0]].get('start', 0) or 0,
            t[0],
            t[1],
        ),
    )

    edge_set = set()  # 用于去重（保持 direct_edges_sorted 顺序）
    edges_unique = []  # [(src_idx, dst_idx, src_node_id, dst_node_id)]
    for src_idx, dst_idx, src_node_id, dst_node_id in direct_edges_sorted:
        key = (src_node_id, dst_node_id)
        if key in edge_set:
            continue
        edge_set.add(key)
        edges_unique.append((src_idx, dst_idx, src_node_id, dst_node_id))

    for edge_seq, (src_idx, dst_idx, src_node_id, dst_node_id) in enumerate(edges_unique, start=1):
        edges.append((src_node_id, dst_node_id))

        # 构建边数据
        src_data = all_data[src_idx]
        dst_data = all_data[dst_idx]
        deploy_unit_src_raw = src_data.get('deployUnitObjectName', '')
        if deploy_unit_src_raw is None or (hasattr(pd, 'isna') and pd.isna(deploy_unit_src_raw)):
            deploy_unit_src = '' or '未知组件'
        else:
            deploy_unit_src = str(deploy_unit_src_raw).strip() or '未知组件'
        deploy_unit_dst_raw = dst_data.get('deployUnitObjectName', '')
        if deploy_unit_dst_raw is None or (hasattr(pd, 'isna') and pd.isna(deploy_unit_dst_raw)):
            deploy_unit_dst = '' or '未知组件'
        else:
            deploy_unit_dst = str(deploy_unit_dst_raw).strip() or '未知组件'

        edge_data = {
            'edge_id': len(edge_data_list),
            'seq': edge_seq,
            'src': deploy_unit_src,
            'dst': deploy_unit_dst,
            'msgType': dst_data.get('msgType', ''),
            'start_at_ms': dst_data.get('start', 0),
            'end_at_ms': dst_data.get('end', 0),
            'duration': dst_data.get('duration', 0),
            'location': dst_data.get('location', ''),
        }
        edge_data_list.append(edge_data)
    
    # 构建 mermaid 文本
    # 根据节点数量决定布局方向：节点多时使用 TB（上下），少时使用 LR（左右）
    node_count = len(node_data)
    if node_count > 8:
        layout_direction = "TB"  # 上下布局，适合节点多的情况
    else:
        layout_direction = "LR"  # 左右布局，适合节点少的情况
    lines = [f"flowchart {layout_direction}"]
    
    # 收集所有唯一的颜色，为每种颜色定义一个样式类
    color_to_class = {}
    class_counter = 0
    for node_id, data in node_data.items():
        color = data['color']
        if color not in color_to_class:
            class_name = f"colorClass{class_counter}"
            color_to_class[color] = class_name
            class_counter += 1
    
    # 定义节点样式类（在节点定义之前）
    for color, class_name in color_to_class.items():
        lines.append(f"    classDef {class_name} fill:{color},stroke:#334155,stroke-width:2px,color:#fff")
    
    # 添加节点定义
    for node_id, data in node_data.items():
        label = data['label']
        # 转义特殊字符，处理中文字符
        label_escaped = label.replace('"', '&quot;').replace("'", "&#39;")
        lines.append(f"    {node_id}[\"{label_escaped}\"]")
        # 应用样式
        color = data['color']
        class_name = color_to_class[color]
        lines.append(f"    class {node_id} {class_name}")
    
    # 添加边（按 seq 顺序，并在边上标注序号）
    for edge in edge_data_list:
        src_id = None
        dst_id = None
        # 通过节点 label 反查 node_id：这里使用 direct_edges_sorted 产生的 node_id 更可靠
        # 因为 edge_data_list 与 edges 构建顺序一致，我们直接基于 edges 的同序关系取 src/dst node_id
        # 为避免额外映射开销，使用 edge_id 对应 edges 下标
        if 0 <= edge.get('edge_id', -1) < len(edges):
            src_id, dst_id = edges[edge['edge_id']]
        else:
            continue

        seq = edge.get('seq', '')
        seq_label = str(seq) if seq is not None else ''
        if seq_label:
            lines.append(f"    {src_id} -->|{seq_label}| {dst_id}")
        else:
            lines.append(f"    {src_id} --> {dst_id}")
    
    mermaid_text = "\n".join(lines) + "\n"
    
    return mermaid_text, node_data, edge_data_list


def build_html_doc_topology(mermaid_text: str, node_data: Dict, edge_data_list: List[Dict], title: str = "拓扑图") -> str:
    """
    构建包含 tooltip 的 HTML 文档
    
    参数:
        mermaid_text: Mermaid 图文本
        node_data: 节点数据字典
        edge_data_list: 边数据列表
        title: 标题
    
    返回:
        HTML 文档字符串
    """
    import json
    
    # 将节点数据转换为 JSON（用于 tooltip）
    # 注意：需要处理 JSON 中的特殊字符，特别是不能直接序列化 datetime 等对象
    # 先清理数据，确保可以 JSON 序列化
    clean_node_data = {}
    for node_id, data in node_data.items():
        clean_node_data[node_id] = {
            'label': data.get('label', ''),
            'color': data.get('color', ''),
            'node_type': data.get('node_type', ''),
            'location': data.get('location', ''),
            'all_records': []
        }
        # 清理记录数据，只保留可序列化的字段
        for record in data.get('all_records', []):
            clean_record = {}
            for key, value in record.items():
                # 跳过以 _ 开头的内部字段
                if key.startswith('_'):
                    continue
                # 转换值为字符串或基本类型
                if value is None:
                    clean_record[key] = None
                elif isinstance(value, (str, int, float, bool)):
                    clean_record[key] = value
                else:
                    clean_record[key] = str(value)
            clean_node_data[node_id]['all_records'].append(clean_record)
    
    tooltip_json_nodes = json.dumps(clean_node_data, ensure_ascii=False)
    tooltip_json_edges = json.dumps(edge_data_list, ensure_ascii=False)
    
    return f"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{title}</title>
    <style>
        body {{
            margin: 0;
            background: #f5f7fa;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, 'Noto Sans', 'PingFang SC', 'Hiragino Sans GB', 'Microsoft YaHei', sans-serif;
        }}
        .container {{
            padding: 16px;
            width: 100%;
            box-sizing: border-box;
        }}
        .card {{
            background: #ffffff;
            border: 1px solid #e5e7eb;
            border-radius: 12px;
            box-shadow: 0 6px 24px rgba(15,23,42,0.08);
            padding: 16px;
            position: relative;
            overflow: visible;
            width: 100%;
        }}
        .title {{
            text-align: center;
            font-weight: 700;
            font-size: 18px;
            margin: 4px 0 12px;
            color: #0f172a;
            letter-spacing: .2px;
        }}
        .mermaid {{
            background: transparent;
            width: 100%;
            height: 100%;
            min-height: 500px;
        }}
        .mermaid svg {{
            color: #0f172a;
            width: 100%;
            height: auto;
            max-width: 100%;
        }}
        .node-tooltip {{
            position: absolute;
            background: #1e293b;
            color: #e2e8f0;
            padding: 12px 16px;
            border-radius: 8px;
            font-size: 14px;
            pointer-events: none;
            z-index: 1000;
            display: none;
            box-shadow: 0 4px 12px rgba(0,0,0,0.3);
            max-width: 400px;
            line-height: 1.6;
        }}
        .tooltip-row {{
            margin: 4px 0;
        }}
        .tooltip-label {{
            color: #94a3b8;
            font-weight: 600;
            margin-right: 8px;
        }}
        .tooltip-value {{
            color: #e2e8f0;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="card">
            <div class="title">{title}</div>
            <div class="mermaid">{mermaid_text}</div>
        </div>
    </div>
    <div id="tooltip" class="node-tooltip"></div>
    <script src="/assets/mermaid.min.js"></script>
    <script>
        const nodeData = {tooltip_json_nodes};
        const edgeData = {tooltip_json_edges};
        
        mermaid.initialize({{
            startOnLoad: true,
            theme: 'default',
            securityLevel: 'loose',
            flowchart: {{
                curve: 'basis',
                rankSpacing: 100,
                nodeSpacing: 60,
                useMaxWidth: true,
                htmlLabels: true
            }}
        }});
        
        // 等待 Mermaid 渲染完成后添加 tooltip 事件
        setTimeout(() => {{
            const tooltip = document.getElementById('tooltip');
            
            // 提取节点 ID 的辅助函数
            const extractNodeId = (rawId) => {{
                const m = String(rawId).match(/(N_\\d+)/);
                return m ? m[1] : String(rawId);
            }};
            
            // 节点 tooltip
            document.querySelectorAll('.node').forEach(node => {{
                const nid = extractNodeId(node.id);
                const data = nodeData[nid];
                
                if (data) {{
                    node.style.cursor = 'pointer';
                    
                    node.addEventListener('mouseenter', (e) => {{
                        // 构建 tooltip 内容
                        const records = data.all_records || [];
                        let tooltipHTML = '<div class="tooltip-row"><span class="tooltip-label">组件:</span><span class="tooltip-value">' + data.label + '</span></div>';
                        tooltipHTML += '<div class="tooltip-row"><span class="tooltip-label">类型:</span><span class="tooltip-value">' + data.node_type + '</span></div>';
                        tooltipHTML += '<div class="tooltip-row"><span class="tooltip-label">位置:</span><span class="tooltip-value">' + data.location + '</span></div>';
                        tooltipHTML += '<div class="tooltip-row"><span class="tooltip-label">记录数:</span><span class="tooltip-value">' + records.length + '</span></div>';
                        
                        // 显示第一条记录的详细信息（与甘特图相同）
                        if (records.length > 0) {{
                            const first = records[0];
                            tooltipHTML += '<div class="tooltip-row" style="margin-top: 8px; border-top: 1px solid #334155; padding-top: 8px;"><span class="tooltip-label">详细信息:</span></div>';
                            
                            // 云下数据字段
                            if (first.trace_id) tooltipHTML += '<div class="tooltip-row"><span class="tooltip-label">trace_id:</span><span class="tooltip-value">' + (first.trace_id || '') + '</span></div>';
                            
                            // 云上数据字段
                            if (first.span_id) tooltipHTML += '<div class="tooltip-row"><span class="tooltip-label">span_id:</span><span class="tooltip-value">' + (first.span_id || '') + '</span></div>';
                            if (first.parent_span_id) tooltipHTML += '<div class="tooltip-row"><span class="tooltip-label">parent_span_id:</span><span class="tooltip-value">' + (first.parent_span_id || '') + '</span></div>';
                            if (first.source) tooltipHTML += '<div class="tooltip-row"><span class="tooltip-label">source:</span><span class="tooltip-value">' + (first.source || '') + '</span></div>';
                            
                            // 通用字段
                            if (first.log_id) tooltipHTML += '<div class="tooltip-row"><span class="tooltip-label">log_id:</span><span class="tooltip-value">' + (first.log_id || '') + '</span></div>';
                            if (first.global_id) tooltipHTML += '<div class="tooltip-row"><span class="tooltip-label">global_id:</span><span class="tooltip-value">' + (first.global_id || '') + '</span></div>';
                            if (first.msgType) tooltipHTML += '<div class="tooltip-row"><span class="tooltip-label">msgType:</span><span class="tooltip-value">' + (first.msgType || '') + '</span></div>';
                            if (first.start_time) tooltipHTML += '<div class="tooltip-row"><span class="tooltip-label">开始时间:</span><span class="tooltip-value">' + (first.start_time || '') + '</span></div>';
                            if (first.duration !== undefined) tooltipHTML += '<div class="tooltip-row"><span class="tooltip-label">持续时间:</span><span class="tooltip-value">' + first.duration + 'ms</span></div>';
                            
                            // 云下特有字段
                            if (first.chain_src_key) tooltipHTML += '<div class="tooltip-row"><span class="tooltip-label">chain_src_key:</span><span class="tooltip-value">' + (first.chain_src_key || '') + '</span></div>';
                            if (first.chain_dst_key) tooltipHTML += '<div class="tooltip-row"><span class="tooltip-label">chain_dst_key:</span><span class="tooltip-value">' + (first.chain_dst_key || '') + '</span></div>';
                            
                            // 其他字段
                            if (first.location === '云下') {{
                                if (first.src_ip) tooltipHTML += '<div class="tooltip-row"><span class="tooltip-label">src_ip:</span><span class="tooltip-value">' + (first.src_ip || '') + '</span></div>';
                                if (first.srcSysName) tooltipHTML += '<div class="tooltip-row"><span class="tooltip-label">srcSysName:</span><span class="tooltip-value">' + (first.srcSysName || '') + '</span></div>';
                            }} else if (first.location === '云上') {{
                                if (first.server_ip) tooltipHTML += '<div class="tooltip-row"><span class="tooltip-label">server_ip:</span><span class="tooltip-value">' + (first.server_ip || '') + '</span></div>';
                            }} else if (first.location === '全链路') {{
                                if (first.request_ip) tooltipHTML += '<div class="tooltip-row"><span class="tooltip-label">request_ip:</span><span class="tooltip-value">' + (first.request_ip || '') + '</span></div>';
                            }}
                            if (first.service_status) tooltipHTML += '<div class="tooltip-row"><span class="tooltip-label">service_status:</span><span class="tooltip-value">' + (first.service_status || '') + '</span></div>';
                        }}
                        
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
                }}
            }});
            
            // 边 tooltip
            document.querySelectorAll('.edgeLabel').forEach((el) => {{
                const labelText = el.textContent.trim();
                // 从边标签中提取序号（例如："1"）
                const m = labelText.match(/\d+/);
                const seq = m ? parseInt(m[0], 10) : null;
                
                el.style.cursor = 'pointer';
                
                el.addEventListener('mouseenter', (e) => {{
                    // 显示边的信息（如果有）
                    if (edgeData && edgeData.length > 0) {{
                        let edge = null;
                        if (seq !== null) {{
                            edge = edgeData.find(ed => ed.seq === seq);
                        }}
                        if (!edge) {{
                            edge = edgeData[0];
                        }}

                        let tooltipHTML = '<div class="tooltip-row"><span class="tooltip-label">源:</span><span class="tooltip-value">' + (edge.src || '') + '</span></div>';
                        tooltipHTML += '<div class="tooltip-row"><span class="tooltip-label">目标:</span><span class="tooltip-value">' + (edge.dst || '') + '</span></div>';
                        if (edge.seq !== undefined) tooltipHTML += '<div class="tooltip-row"><span class="tooltip-label">序号:</span><span class="tooltip-value">' + edge.seq + '</span></div>';
                        if (edge.msgType) tooltipHTML += '<div class="tooltip-row"><span class="tooltip-label">msgType:</span><span class="tooltip-value">' + edge.msgType + '</span></div>';
                        if (edge.duration !== undefined) tooltipHTML += '<div class="tooltip-row"><span class="tooltip-label">持续时间:</span><span class="tooltip-value">' + edge.duration + 'ms</span></div>';
                        
                        tooltip.innerHTML = tooltipHTML;
                        tooltip.style.display = 'block';
                    }}
                }});
                
                el.addEventListener('mousemove', (e) => {{
                    tooltip.style.left = (e.pageX + 15) + 'px';
                    tooltip.style.top = (e.pageY + 15) + 'px';
                }});
                
                el.addEventListener('mouseleave', () => {{
                    tooltip.style.display = 'none';
                }});
            }});
        }}, 800);
    </script>
</body>
</html>
"""


def build_layout():
    """构建界面布局"""
    # 查询条件栏
    query_bar = fac.AntdSpace(
        [
            fac.AntdIcon(icon="antd-field-time", style={"color": "#fff", "fontSize": 16}),
            fac.AntdText("时间范围", strong=True, style={"fontSize": 14}),
            fac.AntdDateRangePicker(
                id="picker-range",
                showTime=True,
                format="YYYY-MM-DD HH:mm:ss",
                defaultValue=[
                    "2026-01-19 14:00:00",
                    "2026-01-19 15:00:00",
                ],
                style={"width": 340},
            ),
            fac.AntdText("msgType", strong=True, style={"fontSize": 14}),
            fac.AntdInput(
                id="target-msgType",
                placeholder="请输入交易码",
                defaultValue="scs",  # 默认 msgType 为 scs
                style={"width": 160},
            ),
            fac.AntdText("查询条数", strong=True, style={"fontSize": 14}),
            fac.AntdSelect(
                id="query-limit",
                options=[
                    {"label": "100", "value": 100},
                    {"label": "1000", "value": 1000},
                    {"label": "10000", "value": 10000},
                ],
                defaultValue=100,
                style={"width": 100},
            ),
            fac.AntdButton(
                "查询",
                id="btn-query",
                type="primary",
                icon=fac.AntdIcon(icon="antd-search"),
                autoSpin=True,
            ),
        ],
        size=6,
        style={"padding": "6px 10px"},
    )

    # 云下数据表格（带单选框）
    down_table = html.Div(
        fac.AntdTable(
        id="down-table",
        columns=[
            {"title": "trace_id", "dataIndex": "trace_id"},
            {"title": "log_id", "dataIndex": "log_id"},
            {"title": "global_id", "dataIndex": "global_id"},
            {"title": "msgType", "dataIndex": "msgType"},
            {"title": "deployUnitObjectName", "dataIndex": "deployUnitObjectName"},
            {"title": "start_time", "dataIndex": "start_at_time"},
            {"title": "start_at_ms", "dataIndex": "start_at_ms"},
            {"title": "latency_msec", "dataIndex": "latency_msec"},
            {"title": "chain_src_key", "dataIndex": "chain_src_key"},
            {"title": "chain_dst_key", "dataIndex": "chain_dst_key"},
            {"title": "是否ESB", "dataIndex": "is_esb"},
            {"title": "是否F5", "dataIndex": "is_f5"},
            {"title": "src_ip", "dataIndex": "src_ip"},
            {"title": "srcSysName", "dataIndex": "srcSysName"},
        ],
        data=[],
        bordered=True,
        rowSelectionType="radio",
        selectedRowKeys=[],
        pagination={"pageSize": 10, "showSizeChanger": True, "pageSizeOptions": [5, 10, 20, 50, 100]},
        style={"width": "100%", "minHeight": "400px"},
        ),
        style={
            "width": "100%",
            "overflowX": "auto",
            "overflowY": "auto",
        },
    )

    # 串联结果表格（使用 AgGrid 树形展示）
    if HAS_AG_GRID:
        result_table = html.Div(
            dag.AgGrid(
            id="result-table",
            className="ag-theme-alpine",
            columnDefs=[
                {"headerName": "location", "field": "location", "width": 80},
                {"headerName": "trace_id", "field": "trace_id", "width": 100},
                {"headerName": "log_id", "field": "log_id", "width": 120},
                {"headerName": "global_id", "field": "global_id", "width": 120},
                {"headerName": "msgType", "field": "msgType", "width": 150},
                {"headerName": "start_time", "field": "start_at_time", "width": 150},
                {"headerName": "start_at_ms", "field": "start_at_ms", "width": 150, "type": "rightAligned"},
                {"headerName": "latency_msec", "field": "latency_msec", "width": 120, "type": "rightAligned"},
                {"headerName": "deployUnitObjectName", "field": "deployUnitObjectName", "width": 200},
                {"headerName": "parents", "field": "parents", "width": 160},
                {"headerName": "children", "field": "children", "width": 160},
                {"headerName": "span_id", "field": "span_id", "width": 100},
                {"headerName": "parent_span_id", "field": "parent_span_id", "width": 120},
                {"headerName": "是否ESB", "field": "is_esb", "width": 100},
                {"headerName": "是否F5", "field": "is_f5", "width": 100},
                {"headerName": "src_ip", "field": "src_ip", "width": 140},
                {"headerName": "srcSysName", "field": "srcSysName", "width": 160},
            ],
            rowData=[],
            defaultColDef={
                "resizable": True,
                "sortable": False,
                "filter": False,
                "flex": 1,
                "minWidth": 80,
            },
            dashGridOptions={
                "treeData": True,
                "getDataPath": {"function": "getDataPath(params)"},
                "autoGroupColumnDef": {
                    "headerName": "调用路径",
                    "minWidth": 400,
                    "cellRendererParams": {
                        "suppressCount": True,
                    },
                },
                "groupDefaultExpanded": 3,
                "animateRows": True,
                "enableCellTextSelection": True,
                "rowSelection": "single",
            },
            enableEnterpriseModules=True,
            # 固定表格最小宽度，触发外层容器出现横向滚动条
            style={"height": "600px", "width": "1400px", "minWidth": "1400px"},
            ),
            style={
                "width": "100%",
                "overflowX": "auto",
                "overflowY": "auto",
            },
        )
    else:
        # 降级方案：使用普通表格
        result_table = html.Div(
            fac.AntdTable(
            id="result-table",
            columns=[
                {"title": "location", "dataIndex": "location"},
                {"title": "trace_id", "dataIndex": "trace_id"},
                {"title": "log_id", "dataIndex": "log_id"},
                {"title": "global_id", "dataIndex": "global_id"},
                {"title": "msgType", "dataIndex": "msgType"},
                {"title": "start_time", "dataIndex": "start_at_time"},
                {"title": "start_at_ms", "dataIndex": "start_at_ms"},
                {"title": "latency_msec", "dataIndex": "latency_msec"},
                {"title": "deployUnitObjectName", "dataIndex": "deployUnitObjectName"},
                # 展示用字符串列，避免与 AntdTable 的 childrenColumnName 冲突
                {"title": "parents", "dataIndex": "parents_str"},
                {"title": "children", "dataIndex": "children_str"},
                {"title": "span_id", "dataIndex": "span_id"},
                {"title": "parent_span_id", "dataIndex": "parent_span_id"},
                {"title": "是否ESB", "dataIndex": "is_esb"},
                {"title": "是否F5", "dataIndex": "is_f5"},
                {"title": "src_ip", "dataIndex": "src_ip"},
                {"title": "srcSysName", "dataIndex": "srcSysName"},
            ],
            data=[],
            bordered=True,
                pagination={"pageSize": 10, "showSizeChanger": True, "pageSizeOptions": [10, 20, 50, 100]},
                # 固定表格最小宽度，触发外层容器出现横向滚动条
                style={"width": "1400px", "minWidth": "1400px", "minHeight": "300px"},
            ),
            style={
                "width": "100%",
                "overflowX": "auto",
                "overflowY": "auto",
            },
        )

    # 串联加载提示
    result_loading = dcc.Loading(
        id="result-loading",
        type="default",
        custom_spinner=fac.AntdText("串联中，请稍候..."),
        children=result_table,
        style={"width": "100%"},
        )

    layout = fac.AntdLayout([
        # 顶部标题
        fac.AntdHeader(
            fac.AntdRow([
                fac.AntdCol(
                    fac.AntdText(
                        "交易链路串联",
                        strong=True,
                        style={"color": "#fff", "fontSize": 20}
                    )
                )
            ]),
            style={"background": "#001529"}
        ),
        # 主体内容
        fac.AntdContent(
            [
                # 查询条件
                fac.AntdRow([
                    fac.AntdCol(
                        fac.AntdCard(
                            [
                                fac.AntdSpace(
                                    query_bar.children,
                                    style={
                                        "width": "100%",
                                        "flexWrap": "wrap",
                                        "rowGap": 8,
                                        "columnGap": 10,
                                    },
                                ),
                            ],
                            title=fac.AntdSpace(
                                [
                                    fac.AntdText("查询条件", strong=True, style={"fontSize": 16}),
                                    # fac.AntdDivider(direction="vertical", style={"height": 20, "margin": "0 8px"}),
                                ],
                                style={
                                    "width": "100%",
                                    "flexWrap": "wrap",
                                    "rowGap": 8,
                                    "columnGap": 12,
                                },
                            ),
                            style={
                                "width": "100%",
                                "padding": 8,
                                "border": "1px solid #e5e7eb",
                                "borderRadius": 6,
                                "background": "#ffffff",
                            },
                        ),
                        span=24,
                        style={"marginBottom": 8},
                    )
                ]),
                # 云下数据表格
                fac.AntdRow([
                    fac.AntdCol(
                        fac.AntdCard(
                            [
                                down_table,
                            ],
                            title=fac.AntdSpace(
                                [
                                    fac.AntdText("云下数据", strong=True, style={"fontSize": 16}),
                                    fac.AntdDivider(direction="vertical", style={"height": 20, "margin": "0 8px"}),
                                    fac.AntdText("串联方向", strong=True, style={"fontSize": 14}),
                                    fac.AntdRadioGroup(
                                        id="chain-direction",
                                        options=[
                                            {"label": "向下", "value": "down"},
                                            {"label": "双向", "value": "both"},
                                        ],
                                        defaultValue="down",
                                        optionType="button",
                                        size="small",
                                    ),
                                    fac.AntdText("数据范围", strong=True, style={"fontSize": 14}),
                                    fac.AntdRadioGroup(
                                        id="data-scope",
                                        options=[
                                            {"label": "云下", "value": "down"},
                                            {"label": "云下+云上", "value": "both"},
                                        ],
                                        defaultValue="both",
                                        optionType="button",
                                        size="small",
                                    ),
                                ],
                                style={
                                    "width": "100%",
                                    "flexWrap": "wrap",
                                    "rowGap": 8,
                                    "columnGap": 12,
                                },
                            ),
                            style={
                                "width": "100%",
                                "padding": 8,
                                "border": "1px solid #e5e7eb",
                                "borderRadius": 6,
                                "background": "#ffffff",
                            },
                        ),
                        span=24,
                        style={"marginBottom": 8},
                    )
                ]),
                # 串联结果表格
                fac.AntdRow([
                    fac.AntdCol(
                        fac.AntdCard(
                            [
                                result_loading,
                            ],
                            title=fac.AntdSpace(
                                [
                                    fac.AntdText("串联结果（云下 + 云上）", strong=True, style={"fontSize": 16}),
                                ],
                                style={
                                    "width": "100%",
                                    "flexWrap": "wrap",
                                    "rowGap": 8,
                                    "columnGap": 12,
                                },
                            ),
                            style={
                                "width": "100%",
                                "padding": 8,
                                "border": "1px solid #e5e7eb",
                                "borderRadius": 6,
                                "background": "#ffffff",
                            },
                        ),
                        span=24,
                        style={"marginBottom": 8},
                    )
                ]),
                # 串联条件时间跨度查询
                fac.AntdRow([
                    fac.AntdCol(
                        fac.AntdCard(
                            [
                                fac.AntdSpace(
                                    [
                                        fac.AntdText("log_id", strong=True, style={"fontSize": 14}),
                                        fac.AntdInput(
                                            id="log-id-input",
                                            placeholder="请输入 log_id",
                                            style={"width": 300},
                                        ),
                                        fac.AntdText("global_id", strong=True, style={"fontSize": 14}),
                                        fac.AntdInput(
                                            id="global-id-input",
                                            placeholder="请输入 global_id",
                                            style={"width": 300},
                                        ),
                                        fac.AntdButton(
                                            "查询时间跨度",
                                            id="btn-query-log-id",
                                            type="primary",
                                            icon=fac.AntdIcon(icon="antd-search"),
                                            autoSpin=True,
                                        ),
                                    ],
                                    size=6,
                                    style={"width": "100%", "flexWrap": "wrap", "rowGap": 8, "columnGap": 10},
                                ),
                                html.Div(
                                    dcc.Graph(id="log-id-timeline-chart"),
                                    style={"width": "100%", "marginTop": 16},
                                ),
                                html.Div(
                                    fac.AntdTable(
                                        id="log-id-timeline-table",
                                        columns=[
                                            {"title": "位置", "dataIndex": "location", "width": 80},
                                            {"title": "trace_id", "dataIndex": "trace_id", "width": 120},
                                            {"title": "log_id", "dataIndex": "log_id", "width": 150},
                                            {"title": "global_id", "dataIndex": "global_id", "width": 120},
                                            {"title": "msgType", "dataIndex": "msgType", "width": 150},
                                            {"title": "开始时间", "dataIndex": "start_time", "width": 180},
                                            {"title": "持续时间(ms)", "dataIndex": "duration", "width": 120, "type": "rightAligned"},
                                            {"title": "组件", "dataIndex": "deployUnitObjectName", "width": 200},
                                            {"title": "chain_src_key", "dataIndex": "chain_src_key", "width": 150},
                                            {"title": "chain_dst_key", "dataIndex": "chain_dst_key", "width": 150},
                                            {"title": "是否ESB", "dataIndex": "is_esb", "width": 100},
                                            {"title": "是否F5", "dataIndex": "is_f5", "width": 100},
                                            {"title": "src_ip", "dataIndex": "src_ip", "width": 140},
                                            {"title": "srcSysName", "dataIndex": "srcSysName", "width": 160},
                                        ],
                                        data=[],
                                        bordered=True,
                                        pagination={"pageSize": 10, "showSizeChanger": True, "pageSizeOptions": [10, 20, 50, 100]},
                                        style={"width": "100%", "minHeight": "300px", "marginTop": 16},
                                    ),
                                    style={
                                        "width": "100%",
                                        "overflowX": "auto",
                                        "overflowY": "auto",
                                    },
                                ),
                                html.Div(
                                    fac.AntdSpace(
                                        [
                                            fac.AntdText("显示F5节点", strong=True, style={"fontSize": 14}),
                                            fac.AntdSwitch(
                                                id="show-f5-switch",
                                                checked=True,
                                                checkedChildren="显示",
                                                unCheckedChildren="隐藏",
                                            ),
                                        ],
                                        size=6,
                                        style={"width": "100%", "marginTop": 16},
                                    ),
                                ),
                                html.Div(
                                    html.Iframe(
                                        id="log-id-topology-frame",
                                        style={
                                            "width": "100%",
                                            "height": "800px",
                                            "minHeight": "600px",
                                            "border": "1px solid #e5e7eb",
                                            "borderRadius": "6px",
                                            "marginTop": 16,
                                            "display": "block",
                                        }
                                    ),
                                    style={
                                        "width": "100%",
                                        "overflow": "hidden",
                                    }
                                ),
                            ],
                            title=fac.AntdText("串联条件时间跨度查询", strong=True, style={"fontSize": 16}),
                            style={
                                "width": "100%",
                                "padding": 8,
                                "border": "1px solid #e5e7eb",
                                "borderRadius": 6,
                                "background": "#ffffff",
                            },
                        ),
                        span=24,
                        style={"marginBottom": 8},
                    )
                ]),
            ],
            style={"padding": 8, "background": "#f5f7fa"},
        ),
    ])

    return layout


app = dash.Dash(__name__)
app.title = "交易链路串联"
app.layout = build_layout()

# 缓存数据
cached = {
    "down_df": pd.DataFrame(),  # 云下查询结果
}

# 缓存查询结果，用于开关切换时更新拓扑图
cached_query_result = {
    "all_data": [],
    "log_id": None,
    "global_id": None,
}


@app.callback(
    Output("down-table", "data"),
    Output("down-table", "selectedRowKeys"),
    Output("btn-query", "loading"),
    Input("btn-query", "nClicks"),
    State("picker-range", "value"),
    State("target-msgType", "value"),
    State("query-limit", "value"),
    prevent_initial_call=True,
)
def on_query(n_clicks, date_range, msg_type, query_limit):
    """查询按钮回调"""
    if n_clicks is None:
        return dash.no_update, [], False

    if not date_range or len(date_range) != 2:
        logger.warning("未选择时间范围")
        return dash.no_update, [], False

    start = date_range[0]
    end = date_range[1]

    # 查询条数限制，默认 100
    try:
        limit = int(query_limit) if query_limit is not None else 100
    except Exception:
        limit = 100

    # 查询云下数据
    t1 = time.time()
    df = search_apm_data_from_ck(start, end, msg_type, limit=limit)
    t2 = time.time()
    logger.info(f"查询完成，耗时 {round(t2 - t1, 2)} 秒")

    # 添加ESB和F5标识列
    if not df.empty:
        # w_ip不为空代表目的地是ESB或F5
        w_ip_col = df.get('w_ip')
        if w_ip_col is None:
            has_w_ip = pd.Series([False] * len(df), index=df.index)
        else:
            has_w_ip = w_ip_col.notna() & (w_ip_col.astype(str) != '') & (w_ip_col.astype(str) != 'nan')
        
        deploy_unit = df.get('deployUnitObjectName', pd.Series([''] * len(df), index=df.index)).astype(str)
        
        # deployUnitObjectName以ESB开头的是ESB，其他为F5
        df['is_esb'] = has_w_ip & deploy_unit.str.startswith('ESB', na=False)
        df['is_f5'] = has_w_ip & (~deploy_unit.str.startswith('ESB', na=False))
        
        # 转换为字符串用于表格显示
        df['is_esb'] = df['is_esb'].apply(lambda x: '是' if x else '否')
        df['is_f5'] = df['is_f5'].apply(lambda x: '是' if x else '否')

    # 缓存数据
    cached["down_df"] = df

    if df.empty:
        return [], [], False

    # 转换为表格数据格式，添加 key 字段用于行选择
    table_data = df.to_dict("records")
    for i, row in enumerate(table_data):
        row['key'] = i  # 添加 key 字段，值为索引

    # 清空选中状态
    return table_data, [], False


@app.callback(
    Output("result-table", "rowData" if HAS_AG_GRID else "data"),
    Input("down-table", "selectedRowKeys"),
    State("down-table", "data"),
    State("chain-direction", "value"),
    State("data-scope", "value"),
    State("picker-range", "value"),
    prevent_initial_call=True,
)
def on_chain(selected_row_keys, table_data, chain_direction, data_scope, date_range):
    """选中行即串联"""
    if not selected_row_keys:
        logger.warning("未选中行")
        return []

    if isinstance(selected_row_keys, list) and len(selected_row_keys) == 0:
        logger.warning("未选中行")
        return []

    if not table_data:
        logger.warning("表格数据为空")
        return []

    if not date_range or len(date_range) != 2:
        logger.warning("未选择时间范围")
        return dash.no_update, [], False

    start = date_range[0]
    end = date_range[1]

    # 找到选中的行 key（selectedRowKeys 返回的是行的 key）
    selected_key = selected_row_keys[0] if isinstance(selected_row_keys, list) else selected_row_keys

    # 通过 key 字段匹配选中的行
    selected_row = None
    for row in table_data:
        if row.get('key') == selected_key:
            selected_row = row
            break

    # 如果没找到，尝试通过索引匹配（兼容处理）
    if not selected_row and isinstance(selected_key, int) and 0 <= selected_key < len(table_data):
        selected_row = table_data[selected_key]

    if not selected_row:
        logger.warning(f"未找到选中的行数据，selected_key: {selected_key}, table_data length: {len(table_data)}")
        return []

    logger.info(f"开始串联，选中行: {selected_row.get('trace_id', 'N/A')}")

    # 执行串联
    t1 = time.time()
    chain_df = chain_trace_from_selected_row(selected_row, direction=chain_direction or "down")
    t2 = time.time()
    logger.info(f"串联完成，耗时 {round(t2 - t1, 2)} 秒")

    if chain_df.empty:
        logger.warning("串联结果为空")
        return []

    # 为chain_df添加w_ip字段（如果缺失）和is_esb、is_f5标识（云下数据）
    if not chain_df.empty and 'w_ip' not in chain_df.columns:
        # 如果chain_df没有w_ip字段，根据trace_id批量查询获取w_ip
        trace_ids = chain_df['trace_id'].dropna().unique().tolist()
        if trace_ids:
            try:
                trace_ids_str = "', '".join(str(tid).replace("'", "''") for tid in trace_ids if tid is not None)
                sql = f"""
                SELECT trace_id, w_ip
                FROM {CK_DATABASE}.apm
                WHERE trace_id IN ('{trace_ids_str}')
                """
                w_ip_df = query_ck(sql)
                if not w_ip_df.empty:
                    # 将w_ip合并到chain_df
                    chain_df = chain_df.merge(w_ip_df[['trace_id', 'w_ip']], on='trace_id', how='left')
                    logger.info(f"成功获取 {len(w_ip_df)} 条记录的 w_ip 字段")
            except Exception as e:
                logger.warning(f"查询w_ip字段失败: {e}")

    # 计算is_esb和is_f5标识
    if not chain_df.empty:
        # 检查是否有w_ip字段
        if 'w_ip' in chain_df.columns:
            w_ip_col = chain_df['w_ip']
            has_w_ip = w_ip_col.notna() & (w_ip_col.astype(str) != '') & (w_ip_col.astype(str) != 'nan')
        else:
            # 如果没有w_ip字段，尝试通过deployUnitObjectName判断（不够准确，但至少能显示一些信息）
            deploy_unit = chain_df.get('deployUnitObjectName', pd.Series([''] * len(chain_df), index=chain_df.index)).astype(str)
            has_w_ip = deploy_unit.str.startswith('ESB', na=False) | deploy_unit.str.contains('F5', na=False, regex=False)
        
        deploy_unit = chain_df.get('deployUnitObjectName', pd.Series([''] * len(chain_df), index=chain_df.index)).astype(str)
        chain_df['is_esb'] = (has_w_ip & deploy_unit.str.startswith('ESB', na=False)).apply(lambda x: '是' if x else '否')
        chain_df['is_f5'] = (has_w_ip & (~deploy_unit.str.startswith('ESB', na=False))).apply(lambda x: '是' if x else '否')

    # 根据数据范围选择决定是否查询云上数据
    if data_scope == "both":
        # 获取链路中的所有 log_id
        log_ids = chain_df['log_id'].dropna().unique().tolist()
        logger.info(f"链路包含 {len(log_ids)} 个 log_id")

        # 查询云上数据
        cloud_df = search_cloud_data_from_ck(log_ids, start, end)
    else:
        # 只使用云下数据，不查询云上
        cloud_df = pd.DataFrame()
        logger.info("数据范围选择为'云下'，跳过云上数据查询")

    # 构建树形数据
    if HAS_AG_GRID:
        tree_data = build_tree_from_chain_data(chain_df, cloud_df)
        logger.info(f"树形数据构建完成: {len(tree_data)} 个节点")
        return tree_data
    else:
        # 降级方案：返回普通表格数据，保留 parents/children/span_id/parent_span_id
        if not cloud_df.empty:
            result_df = pd.concat([chain_df, cloud_df], ignore_index=True)
        else:
            result_df = chain_df.copy()

        # 确保列存在
        for col in ["parents", "children", "span_id", "parent_span_id", "is_esb", "is_f5"]:
            if col not in result_df.columns:
                result_df[col] = None
        
        # 为云下数据添加is_esb和is_f5标识（如果还没有）
        if not result_df.empty:
            # 判断哪些是云下数据（通过location或其他标识）
            # 假设有location列，如果没有则通过其他方式判断
            if 'location' in result_df.columns:
                down_mask = result_df['location'] == '云下'
            else:
                # 如果没有location列，假设所有数据都是云下数据
                down_mask = pd.Series([True] * len(result_df), index=result_df.index)
            
            # 为云下数据计算is_esb和is_f5
            if down_mask.any():
                w_ip_col = result_df.loc[down_mask, 'w_ip'] if 'w_ip' in result_df.columns else None
                if w_ip_col is not None:
                    has_w_ip = w_ip_col.notna() & (w_ip_col.astype(str) != '') & (w_ip_col.astype(str) != 'nan')
                    deploy_unit = result_df.loc[down_mask, 'deployUnitObjectName'].astype(str)
                    result_df.loc[down_mask, 'is_esb'] = (has_w_ip & deploy_unit.str.startswith('ESB', na=False)).apply(lambda x: '是' if x else '否')
                    result_df.loc[down_mask, 'is_f5'] = (has_w_ip & (~deploy_unit.str.startswith('ESB', na=False))).apply(lambda x: '是' if x else '否')
                else:
                    result_df.loc[down_mask, 'is_esb'] = '否'
                    result_df.loc[down_mask, 'is_f5'] = '否'
            
            # 为云上数据设置is_esb和is_f5为'否'
            if 'location' in result_df.columns:
                up_mask = result_df['location'] == '云上'
                result_df.loc[up_mask, 'is_esb'] = '否'
                result_df.loc[up_mask, 'is_f5'] = '否'
        
        # 处理 start_at_time：如果云下数据没有，从 start_at_ms 转换
        if "start_at_time" not in result_df.columns or result_df["start_at_time"].isna().all():
            if "start_at_ms" in result_df.columns:
                result_df["start_at_time"] = result_df["start_at_ms"].apply(
                    lambda ms: datetime.fromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] 
                    if pd.notna(ms) and ms else None
                )

        # parents、children 保留为列表，另外生成字符串列用于展示，防止 AntdTable 把 children 当成树节点字段
        for col in ["parents", "children"]:
            if col in result_df.columns:
                result_df[col + "_str"] = result_df[col].apply(
                    lambda v: ", ".join(str(x) for x in v) if isinstance(v, (list, tuple)) else ("" if v is None else str(v))
                )

        result_df = result_df.sort_values(by='start_at_ms', ascending=True).reset_index(drop=True)
        result_data = result_df.to_dict("records")
        return result_data


@app.callback(
    Output("log-id-timeline-chart", "figure"),
    Output("log-id-timeline-table", "data"),
    Output("log-id-topology-frame", "srcDoc"),
    Output("btn-query-log-id", "loading"),
    Input("btn-query-log-id", "nClicks"),
    State("log-id-input", "value"),
    State("global-id-input", "value"),
    State("show-f5-switch", "checked"),
    State("picker-range", "value"),
    prevent_initial_call=True,
)
def on_query_log_id(n_clicks, log_id, global_id, show_f5, date_range):
    """查询串联条件（log_id 和/或 global_id）的时间跨度"""
    if n_clicks is None:
        empty_html = """
<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>拓扑图</title></head>
<body style="margin:0;display:flex;align-items:center;justify-content:center;height:100vh;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;color:#999;">
    <div>请先查询数据</div>
</body>
</html>
"""
        return go.Figure(), [], empty_html, False

    if not date_range or len(date_range) != 2:
        logger.warning("未选择时间范围")
        return dash.no_update, [], False

    start = date_range[0]
    end = date_range[1]
    
    # 检查至少输入一个条件
    log_id_trimmed = log_id.strip() if log_id and log_id.strip() else None
    global_id_trimmed = global_id.strip() if global_id and global_id.strip() else None
    
    if not log_id_trimmed and not global_id_trimmed:
        logger.warning("未输入 log_id 或 global_id")
        empty_fig = go.Figure()
        empty_fig.add_annotation(
            text="请输入 log_id 或 global_id",
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=16)
        )
        empty_html = """
<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>拓扑图</title></head>
<body style="margin:0;display:flex;align-items:center;justify-content:center;height:100vh;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;color:#999;">
    <div>请输入 log_id 或 global_id</div>
</body>
</html>
"""
        return empty_fig, [], empty_html, False
    
    # 构建查询条件描述
    conditions = []
    if log_id_trimmed:
        conditions.append(f"log_id={log_id_trimmed}")
    if global_id_trimmed:
        conditions.append(f"global_id={global_id_trimmed}")
    condition_str = " AND ".join(conditions)
    logger.info(f"开始查询: {condition_str}")
    
    # 查询数据
    t1 = time.time()
    df_down, df_up = query_chain_condition_data(log_id=log_id_trimmed, global_id=global_id_trimmed, start_time=start, end_time=end)
    # 查询全链路日志数据
    df_full_link = query_full_link_log_data(log_id=log_id_trimmed, global_id=global_id_trimmed, start_time=start, end_time=end)
    t2 = time.time()
    logger.info(f"查询完成，耗时 {round(t2 - t1, 2)} 秒（包含全链路日志）")
    
    # 创建图表和表格数据（包含 full_link）
    fig, table_data, all_data = create_timeline_gantt_chart(df_down, df_up, df_full_link=df_full_link)
    
    # 缓存查询结果
    cached_query_result["all_data"] = all_data
    cached_query_result["log_id"] = log_id_trimmed
    cached_query_result["global_id"] = global_id_trimmed
    
    # 构建拓扑图
    if all_data:
        try:
            mermaid_text, node_data, edge_data_list = build_mermaid_topology(all_data, show_f5=show_f5)
            title = f"拓扑图 - {condition_str}"
            topology_html = build_html_doc_topology(mermaid_text, node_data, edge_data_list, title)
        except Exception as e:
            logger.error(f"构建拓扑图失败: {e}")
            topology_html = f"""
<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>拓扑图</title></head>
<body style="margin:0;display:flex;align-items:center;justify-content:center;height:100vh;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;color:#999;">
    <div>构建拓扑图失败: {str(e)}</div>
</body>
</html>
"""
    else:
        topology_html = """
<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>拓扑图</title></head>
<body style="margin:0;display:flex;align-items:center;justify-content:center;height:100vh;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;color:#999;">
    <div>暂无数据</div>
</body>
</html>
"""
    
    return fig, table_data, topology_html, False


@app.callback(
    Output("log-id-topology-frame", "srcDoc", allow_duplicate=True),
    Input("show-f5-switch", "checked"),
    prevent_initial_call=True,
)
def on_toggle_f5(show_f5):
    """切换 F5 显示时更新拓扑图"""
    all_data = cached_query_result.get("all_data", [])
    log_id = cached_query_result.get("log_id")
    global_id = cached_query_result.get("global_id")
    
    if not all_data:
        return """
<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>拓扑图</title></head>
<body style="margin:0;display:flex;align-items:center;justify-content:center;height:100vh;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;color:#999;">
    <div>请先查询数据</div>
</body>
</html>
"""
    
    try:
        mermaid_text, node_data, edge_data_list = build_mermaid_topology(all_data, show_f5=show_f5)
        conditions = []
        if log_id:
            conditions.append(f"log_id={log_id}")
        if global_id:
            conditions.append(f"global_id={global_id}")
        condition_str = " AND ".join(conditions) if conditions else "拓扑图"
        title = f"拓扑图 - {condition_str}"
        topology_html = build_html_doc_topology(mermaid_text, node_data, edge_data_list, title)
        return topology_html
    except Exception as e:
        logger.error(f"构建拓扑图失败: {e}")
        return f"""
<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>拓扑图</title></head>
<body style="margin:0;display:flex;align-items:center;justify-content:center;height:100vh;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;color:#999;">
    <div>构建拓扑图失败: {str(e)}</div>
</body>
</html>
"""


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5560, debug=True)
