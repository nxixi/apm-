import pandas as pd
import json
import time
from datetime import datetime
from typing import List, Dict, Set, Tuple, Optional
from collections import defaultdict

import dash
from dash import html, dcc, Output, Input, State
import feffery_antd_components as fac


import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

try:
    import clickhouse_connect
except ImportError:
    logger.warning("clickhouse_connect 未安装，请安装: pip install clickhouse-connect")
    clickhouse_connect = None

try:
    import dash_ag_grid as dag
    HAS_AG_GRID = True
except ImportError:
    HAS_AG_GRID = False
    logger.warning("dash-ag-grid 未安装，请安装: pip install dash-ag-grid")
# HAS_AG_GRID = False


# ClickHouse 连接配置（请根据实际情况修改）
CK_HOST = '192.168.103.47'
CK_PORT = 8123
CK_USER = 'default'
CK_PASSWORD = 'eoi@&UJM.cn'
CK_DATABASE = 'ccdc'

# 全局 ClickHouse 客户端
ck_client = None


def get_ck_client():
    """获取 ClickHouse 客户端"""
    global ck_client
    if ck_client is None and clickhouse_connect:
        try:
            ck_client = clickhouse_connect.get_client(
                host=CK_HOST,
                port=CK_PORT,
                username=CK_USER,
                password=CK_PASSWORD,
                database=CK_DATABASE
            )
            logger.info("ClickHouse 连接成功")
        except Exception as e:
            logger.error(f"ClickHouse 连接失败: {e}")
            ck_client = None
    return ck_client


def query_ck(sql: str) -> pd.DataFrame:
    """
    执行 ClickHouse 查询并返回 DataFrame

    参数:
        sql: SQL 查询语句

    返回:
        DataFrame
    """
    client = get_ck_client()
    if client is None:
        logger.error("ClickHouse 客户端未初始化")
        return pd.DataFrame()

    try:
        df = client.query_df(sql)
        logger.info(f"查询成功，返回 {len(df)} 行数据")
        return df
    except Exception as e:
        logger.error(f"ClickHouse 查询失败: {e}")
        logger.error(f"SQL: {sql}")
        return pd.DataFrame()


def search_apm_data_from_ck(start: str, end: str, msg_type: str = None) -> pd.DataFrame:
    """
    从 ClickHouse 查询云下数据（本地测试改为读取 parquet）
    """
    if not start or not end:
        logger.warning("【云下数据查询】未配置开始和结束时间！")
        return pd.DataFrame()

    try:
        start_ts = int(datetime.strptime(start, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
        end_ts = int(datetime.strptime(end, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
        where_conditions = [f"start_at_ms >= {start_ts}", f"start_at_ms <= {end_ts}"]
        if msg_type:
            where_conditions.append(f"msgType like '{msg_type}%'")
        where_clause = " AND ".join(where_conditions)
        sql = f"""
        SELECT *
        FROM {CK_DATABASE}.apm
        WHERE {where_clause}
        ORDER BY start_at_ms
        """
        logger.info(f"执行查询: {sql[:200]}...")
        df_ck = query_ck(sql)
        return df_ck
    except Exception as e:
        logger.error(f"查询云下数据失败: {e}")
        return pd.DataFrame()


def find_children_from_ck(parent_start_ms: int, parent_end_ms: int, chain_dst_key: str) -> pd.DataFrame:
    """
    查找子节点（本地测试改为从 parquet 过滤）

    """
    if not chain_dst_key or str(chain_dst_key) == 'nan':
        return pd.DataFrame()

    try:
        chain_dst_key_escaped = str(chain_dst_key).replace("'", "''")
        sql = f"""
        SELECT *
        FROM {CK_DATABASE}.apm
        WHERE start_at_ms >= {int(parent_start_ms)}
          AND start_at_ms + latency_msec <= {int(parent_end_ms)}
          AND chain_src_key = '{chain_dst_key_escaped}'
        ORDER BY start_at_ms
        """
        df_ck = query_ck(sql)
        return df_ck
    except Exception as e:
        logger.error(f"查找子节点失败: {e}")
        return pd.DataFrame()


def find_parents_from_ck(child_start_ms: int, child_end_ms: int, chain_src_key: str) -> pd.DataFrame:
    """
    查找父节点（本地测试改为从 parquet 过滤）

    CK 版本保留在注释里，方便切换。
    """
    if not chain_src_key or str(chain_src_key) == 'nan':
        return pd.DataFrame()

    # ===== CK 查询版本（保留注释，勿删） =====
    try:
        chain_src_key_escaped = str(chain_src_key).replace("'", "''")
        sql = f"""
        SELECT *
        FROM {CK_DATABASE}.apm
        WHERE start_at_ms <= {int(child_start_ms)}
          AND start_at_ms + latency_msec >= {int(child_end_ms)}
          AND chain_dst_key = '{chain_src_key_escaped}'
        ORDER BY start_at_ms
        """
        df_ck = query_ck(sql)
        return df_ck
    except Exception as e:
        logger.error(f"查找父节点失败: {e}")
        return pd.DataFrame()

def chain_trace_from_selected_row(selected_row: dict, direction: str = "down") -> pd.DataFrame:
    """
    从选中的行开始串联链路

    参数:
        selected_row: 选中的行数据（字典）

    返回:
        DataFrame，包含串联后的链路数据，列包括：
        trace_id, log_id, global_id, msgType, start_at_ms, latency_msec,
        parents, children, deployUnitObjectName（物理组件单元）, location（云下）
    """
    if not selected_row:
        return pd.DataFrame()

    # 入口节点
    entry_start_ms = int(selected_row.get('start_at_ms', 0))
    entry_latency = int(selected_row.get('latency_msec', 0))
    entry_end_ms = entry_start_ms + entry_latency
    entry_chain_dst_key = str(selected_row.get('chain_dst_key', ''))

    if not entry_chain_dst_key or entry_chain_dst_key == 'nan':
        logger.warning("入口节点缺少 chain_dst_key")
        return pd.DataFrame()

    # 存储所有链路节点
    chain_nodes = []
    visited_keys = set()  # 防止重复处理

    # 使用队列进行 BFS 遍历
    from collections import deque
    queue = deque(
        [(entry_start_ms, entry_end_ms, entry_chain_dst_key, selected_row, 0)])  # (start, end, key, row_data, depth)

    while queue:
        current_start_ms, current_end_ms, current_chain_dst_key, current_row, depth = queue.popleft()

        # 防止重复处理
        node_key = f"{current_row.get('trace_id', '')}"
        if node_key in visited_keys:
            continue
        visited_keys.add(node_key)

        # 查找当前节点的父节点（用于填充 parents 字段）
        parents_list = []
        parents_df = pd.DataFrame()
        current_chain_src_key = str(current_row.get('chain_src_key', ''))
        if current_chain_src_key and current_chain_src_key != 'nan':
            parents_df = find_parents_from_ck(
                current_start_ms,
                current_end_ms,
                current_chain_src_key
            )

            # # 验证父节点数量（入口节点除外）
            # if depth > 0:  # 非入口节点
            #     if len(parents_df) > 1:
            #         # 父节点超过1个，删除整条链路
            #         logger.warning(f"节点 {node_key} 的父节点数量为 {len(parents_df)}，删除整条链路")
            #         return pd.DataFrame()

            # 提取父节点的 trace_id
            for _, parent_row in parents_df.iterrows():
                parent_trace_id = parent_row.get('trace_id')
                if parent_trace_id is not None and str(parent_trace_id) != 'nan':
                    parents_list.append(parent_trace_id)

        # 处理 start_at_time：如果不存在，从 start_at_ms 转换
        start_at_time = current_row.get('start_at_time')
        if not start_at_time and current_start_ms:
            try:
                start_at_time = datetime.fromtimestamp(current_start_ms / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            except:
                start_at_time = None

        # 构建当前节点数据
        node_data = {
            'trace_id': current_row.get('trace_id'),
            'log_id': current_row.get('log_id'),
            'global_id': current_row.get('global_id'),
            'msgType': current_row.get('msgType'),
            'start_at_time': start_at_time,
            'start_at_ms': current_start_ms,
            'latency_msec': current_row.get('latency_msec'),
            'parents': parents_list,  # 父节点的 trace_id 列表
            'children': [],  # 子节点的 trace_id 列表（稍后填充）
            'deployUnitObjectName': current_row.get('deployUnitObjectName', ''),
            'location': '云下'
        }

        # 查找子节点（向下串联）
        if current_chain_dst_key and current_chain_dst_key != 'nan':
            children_df = find_children_from_ck(current_start_ms, current_end_ms, current_chain_dst_key)

            # 处理子节点
            children_list = []
            for _, child_row in children_df.iterrows():
                child_start_ms = int(child_row.get('start_at_ms', 0))
                child_latency = int(child_row.get('latency_msec', 0))
                child_end_ms = child_start_ms + child_latency
                child_chain_src_key = str(child_row.get('chain_src_key', ''))
                child_chain_dst_key = str(child_row.get('chain_dst_key', ''))

                # # 验证子节点的父节点数量
                # if child_chain_src_key and child_chain_src_key != 'nan':
                #     child_parents_df = find_parents_from_ck(
                #         child_start_ms,
                #         child_end_ms,
                #         child_chain_src_key
                #     )
                    # if len(child_parents_df) > 1:
                    #     # 子节点的父节点超过1个，删除整条链路
                    #     logger.warning(f"子节点 {child_row.get('log_id')} 的父节点数量为 {len(child_parents_df)}，删除整条链路")
                    #     return pd.DataFrame()

                # 获取子节点的 trace_id
                child_trace_id = child_row.get('trace_id')
                if child_trace_id is not None and str(child_trace_id) != 'nan':
                    children_list.append(child_trace_id)

                # 添加到队列继续处理
                child_key = f"{child_row.get('trace_id', '')}"
                if child_key not in visited_keys:
                    queue.append((child_start_ms, child_end_ms, child_chain_dst_key, child_row.to_dict(), depth + 1))

            node_data['children'] = children_list

        # 如果串联方向为双向，则将父节点加入队列，继续向上追溯
        if direction == "both" and not parents_df.empty:
            for _, parent_row in parents_df.iterrows():
                parent_start_ms = int(parent_row.get('start_at_ms', 0))
                parent_latency = int(parent_row.get('latency_msec', 0))
                parent_end_ms = parent_start_ms + parent_latency
                parent_chain_dst_key = str(parent_row.get('chain_dst_key', ''))

                parent_key = f"{parent_row.get('trace_id', '')}"
                if parent_key not in visited_keys:
                    queue.append(
                        (parent_start_ms, parent_end_ms, parent_chain_dst_key, parent_row.to_dict(), depth + 1)
                    )

        chain_nodes.append(node_data)

    if not chain_nodes:
        return pd.DataFrame()

    result_df = pd.DataFrame(chain_nodes)
    return result_df


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
                'node_type': 'down'
            }
            trace_id_to_node[trace_id] = node
            all_nodes[node['id']] = node

    # 处理云上数据
    for _, row in cloud_df.iterrows():
        span_id = row.get('span_id')
        parent_span_id = row.get('parent_span_id')
        if span_id is not None and str(span_id) != 'nan':
            node = {
                'id': f"up_{span_id}",
                'span_id': span_id,
                'parent_span_id': parent_span_id,
                'log_id': row.get('log_id'),
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


def search_cloud_data_from_ck(log_ids: List) -> pd.DataFrame:
    """
    从 ClickHouse 查询云上数据（本地测试改为读取 parquet）

    """
    if not log_ids:
        return pd.DataFrame()

    # ===== CK 查询版本（保留注释，勿删） =====
    try:
        valid_log_ids = [str(log_id).replace("'", "''") for log_id in log_ids if
                         log_id is not None and str(log_id) != 'nan']
        if not valid_log_ids:
            return pd.DataFrame()
        log_ids_str = "', '".join(valid_log_ids)
        sql = f"""
        SELECT 
            *
        FROM {CK_DATABASE}.link
        WHERE source = 'transaction_action'
          AND log_id IN ('{log_ids_str}')
        ORDER BY start_time
        """
        df_ck = query_ck(sql)

        # 重命名列
        df_ck = df_ck.rename(
            columns={
                "msg_type": "msgType",
                "start_time": "start_at_ms",
                "response_duration": "latency_msec",
                "physics_component_unit_name_desc": "deployUnitObjectName",
            }
        )
        # 添加 location 列
        df_ck["location"] = "云上"
        # 添加空列以匹配云下数据结构
        df_ck["parents"] = None
        df_ck["children"] = None
        df_ck["trace_id"] = None
        logger.info(f"查询到 {len(df_ck)} 条云上数据")

        return df_ck
    except Exception as e:
        logger.error(f"查询云上数据失败: {e}")
        return pd.DataFrame()


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
            fac.AntdInput(id="target-msgType", placeholder="请输入交易码", style={"width": 160}),
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
    prevent_initial_call=True,
)
def on_query(n_clicks, date_range, msg_type):
    """查询按钮回调"""
    if n_clicks is None:
        return dash.no_update, [], False

    if not date_range or len(date_range) != 2:
        logger.warning("未选择时间范围")
        return dash.no_update, [], False

    start = date_range[0]
    end = date_range[1]

    # 查询云下数据
    t1 = time.time()
    df = search_apm_data_from_ck(start, end, msg_type)
    t2 = time.time()
    logger.info(f"查询完成，耗时 {round(t2 - t1, 2)} 秒")

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
        for col in ["parents", "children", "span_id", "parent_span_id"]:
            if col not in result_df.columns:
                result_df[col] = None
        
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


if __name__ == "__main__":
    # df = query_ck("select * from ccdc.link where source='transaction_action' and msg_type like 'cpms%'")
    # 初始化 ClickHouse 连接
    get_ck_client()
    app.run(host="0.0.0.0", port=5560, debug=True)
