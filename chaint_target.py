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

from src.chain import logger, search_apm_data_from_ck, chain_trace_from_selected_row, search_cloud_data_from_ck
from src.ck_client import query_ck, CK_DATABASE

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


def query_chain_condition_data(log_id: str = None, global_id: str = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    根据 log_id 和/或 global_id 查询云下和云上数据
    
    参数:
        log_id: 要查询的 log_id（可选）
        global_id: 要查询的 global_id（可选）
    
    返回:
        (云下数据DataFrame, 云上数据DataFrame)
    """
    if not log_id and not global_id:
        return pd.DataFrame(), pd.DataFrame()
    
    # 构建查询条件
    where_conditions_down = []
    where_conditions_up = []
    
    if log_id and log_id.strip():
        log_id_escaped = str(log_id).strip().replace("'", "''")
        where_conditions_down.append(f"log_id = '{log_id_escaped}'")
        where_conditions_up.append(f"log_id = '{log_id_escaped}'")
    
    if global_id and global_id.strip():
        global_id_escaped = str(global_id).strip().replace("'", "''")
        where_conditions_down.append(f"global_id = '{global_id_escaped}'")
        where_conditions_up.append(f"global_id = '{global_id_escaped}'")
    
    if not where_conditions_down:
        return pd.DataFrame(), pd.DataFrame()
    
    where_clause_down = " AND ".join(where_conditions_down)
    where_clause_up = " AND ".join(where_conditions_up)
    
    # 查询云下数据
    try:
        sql_down = f"""
        SELECT *
        FROM {CK_DATABASE}.apm
        WHERE {where_clause_down} AND `sync-flag`='同步'
        ORDER BY start_at_ms
        """
        df_down = query_ck(sql_down)
        logger.info(f"查询到 {len(df_down)} 条云下数据")
        
        # 添加ESB和F5标识列
        if not df_down.empty:
            # w_ip不为空代表目的地是ESB或F5
            w_ip_col = df_down.get('w_ip')
            if w_ip_col is None:
                has_w_ip = pd.Series([False] * len(df_down), index=df_down.index)
            else:
                has_w_ip = w_ip_col.notna() & (w_ip_col.astype(str) != '') & (w_ip_col.astype(str) != 'nan')
            
            deploy_unit = df_down.get('deployUnitObjectName', pd.Series([''] * len(df_down), index=df_down.index)).astype(str)
            
            # deployUnitObjectName以ESB开头的是ESB，其他为F5
            df_down['is_esb'] = has_w_ip & deploy_unit.str.startswith('ESB', na=False)
            df_down['is_f5'] = has_w_ip & (~deploy_unit.str.startswith('ESB', na=False))
    except Exception as e:
        logger.error(f"查询云下数据失败: {e}")
        df_down = pd.DataFrame()
    
    # 查询云上数据
    try:
        sql_up = f"""
        SELECT *
        FROM {CK_DATABASE}.link
        WHERE {where_clause_up}
        -- AND source = 'transaction_action'
        ORDER BY start_time
        """
        df_up = query_ck(sql_up)
        logger.info(f"查询到 {len(df_up)} 条云上数据")
    except Exception as e:
        logger.error(f"查询云上数据失败: {e}")
        df_up = pd.DataFrame()
    
    return df_down, df_up


def create_timeline_gantt_chart(df_down: pd.DataFrame, df_up: pd.DataFrame) -> Tuple[go.Figure, List[Dict]]:
    """
    创建时间跨度甘特图
    
    参数:
        df_down: 云下数据
        df_up: 云上数据
    
    返回:
        (Plotly Figure 对象, 表格数据列表)
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
    
    # 收集所有数据，按开始时间排序
    all_data = []
    
    # 处理云下数据
    for idx, row in df_down.iterrows():
        start_ms = int(row.get('start_at_ms', 0))
        latency = int(row.get('latency_msec', 0))
        end_ms = start_ms + latency
        
        is_esb = bool(row.get('is_esb', False))
        is_f5 = bool(row.get('is_f5', False))
        
        all_data.append({
            'start': start_ms,
            'end': end_ms,
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
        
        all_data.append({
            'start': start_ms,
            'end': end_ms,
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
            'src_ip': row.get('src_ip', ''),
            'srcSysName': row.get('srcSysName', ''),
        })
    
    # 排序规则：
    # 1. 先按开始时间排序（最早的在前）
    # 2. 开始时间相同时，结束时间越晚的越靠前
    # 3. 如果开始时间相同、结束时间也相同：
    #    按照“上一条的 chain_dst_key = 下一条的 chain_src_key”的规则排序，
    #    即使得相邻记录在链路上尽量首尾相接，便于观察调用链

    def _compare_gantt_row(a: Dict, b: Dict) -> int:
        # 1. 开始时间
        if a['start'] != b['start']:
            return -1 if a['start'] < b['start'] else 1
        # 2. 结束时间（长的在前）
        if a['end'] != b['end']:
            return -1 if a['end'] > b['end'] else 1

        # 3. 开始时间和结束时间都相同时，根据链路关系尝试排序
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

        # 否则保持相对顺序（稳定排序会保留原有顺序）
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
        return fig, []
    
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
                    line=dict(color='#722ed1', width=8),
                    marker=dict(size=10, color='#722ed1'),
                    hovertemplate=hover_text + '<extra></extra>',
                    showlegend=(idx == 0),
                ))
        
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
                    line=dict(color='#52c41a', width=8),
                    marker=dict(size=10, color='#52c41a'),
                    hovertemplate=hover_text + '<extra></extra>',
                    showlegend=(idx == 0),
                ))
        
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
                    line=dict(color='#1890ff', width=8),
                    marker=dict(size=10, color='#1890ff'),
                    hovertemplate=hover_text + '<extra></extra>',
                    showlegend=(idx == 0),
                ))
    
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
                f"src_ip: {truncate_text(d.get('src_ip', ''), 30)}<br>"
                f"srcSysName: {truncate_text(d.get('srcSysName', ''), 50)}"
            )
            
            fig.add_trace(go.Scatter(
                x=[start_dt, end_dt],
                y=[d['_y_pos'], d['_y_pos']],
                mode='lines+markers',
                name=name,
                line=dict(color=color, width=8),
                marker=dict(size=10, color=color),
                hovertemplate=hover_text + '<extra></extra>',
                showlegend=show_legend,  # 每种类型的第一条显示图例
            ))
    
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
    
    return fig, table_data


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
                    "2025-12-19 10:00:00",
                    "2025-12-19 10:05:00",
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
    prevent_initial_call=True,
)
def on_chain(selected_row_keys, table_data, chain_direction, data_scope):
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
        cloud_df = search_cloud_data_from_ck(log_ids)
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
    Output("btn-query-log-id", "loading"),
    Input("btn-query-log-id", "nClicks"),
    State("log-id-input", "value"),
    State("global-id-input", "value"),
    prevent_initial_call=True,
)
def on_query_log_id(n_clicks, log_id, global_id):
    """查询串联条件（log_id 和/或 global_id）的时间跨度"""
    if n_clicks is None:
        return go.Figure(), [], False
    
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
        return empty_fig, [], False
    
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
    df_down, df_up = query_chain_condition_data(log_id=log_id_trimmed, global_id=global_id_trimmed)
    t2 = time.time()
    logger.info(f"查询完成，耗时 {round(t2 - t1, 2)} 秒")
    
    # 创建图表和表格数据
    fig, table_data = create_timeline_gantt_chart(df_down, df_up)
    
    return fig, table_data, False


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5560, debug=True)
