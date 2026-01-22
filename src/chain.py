import pandas as pd
from datetime import datetime
from typing import List, Tuple
from .ck_client import query_ck, build_apm_cte, logger, CK_DATABASE


def search_apm_data_from_ck(start: str, end: str, msg_type: str = None, limit: int = None) -> pd.DataFrame:
    """
    从 ClickHouse 查询云下数据
    """
    if not start or not end:
        logger.warning("【云下数据查询】未配置开始和结束时间！")
        return pd.DataFrame()

    try:
        start_ts = int(datetime.strptime(start, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
        end_ts = int(datetime.strptime(end, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
        where_conditions = [
            f"start_at_time >= fromUnixTimestamp64Milli({start_ts})", f"start_at_time <= fromUnixTimestamp64Milli({end_ts+1000})",
                            f"start_at_ms >= {start_ts}", f"start_at_ms <= {end_ts}"]
        if msg_type:
            where_conditions.append(f"msgType like '{msg_type}%'")
        where_clause = " AND ".join(where_conditions)
        sql = f"""
        {build_apm_cte(where_clause)}
        SELECT *
        FROM apm_view
        """
        # sql = f"""
        # SELECT *
        # FROM {CK_DATABASE}.apm
        # WHERE {where_clause}
        # ORDER BY start_at_ms
        # """
        # 按需限制返回条数，避免全量扫描
        if limit is not None:
            try:
                limit_int = int(limit)
                if limit_int > 0:
                    sql = sql + f"\n        LIMIT {limit_int}"
            except Exception:
                # 如果 limit 转换失败，则忽略限制
                logger.warning(f"无效的 limit 参数: {limit}")
        logger.info(f"执行查询: {sql}...")
        df_ck = query_ck(sql)
        return df_ck
    except Exception as e:
        logger.error(f"查询云下数据失败: {e}")
        return pd.DataFrame()


def find_children_from_ck(parent_start_ms: int, parent_end_ms: int, chain_dst_key: str) -> pd.DataFrame:
    """
    查找子节点

    """
    if not chain_dst_key or str(chain_dst_key) == 'nan':
        return pd.DataFrame()

    try:
        chain_dst_key_escaped = str(chain_dst_key).replace("'", "''")
        where_clause = f"""
        --start_at_time >= fromUnixTimestamp64Milli({int(parent_start_ms)})
        --AND start_at_time <= fromUnixTimestamp64Milli({int(parent_end_ms)}-latency_msec+1000)
        start_at_ms >= {int(parent_start_ms)}
        AND start_at_ms + latency_msec <= {int(parent_end_ms)}
        AND chain_src_key = '{chain_dst_key_escaped}'
        """
        sql = f"""
        {build_apm_cte(where_clause)}
        SELECT *
        FROM apm_view
        """
        # sql = f"""
        # SELECT *
        # FROM {CK_DATABASE}.apm
        # WHERE {where_clause}
        # ORDER BY start_at_ms
        # """
        logger.info(f"执行查询: {sql}...")
        df_ck = query_ck(sql)
        return df_ck
    except Exception as e:
        logger.error(f"查找子节点失败: {e}")
        return pd.DataFrame()


def find_parents_from_ck(child_start_ms: int, child_end_ms: int, chain_src_key: str) -> pd.DataFrame:
    """
    查找父节点
    """
    if not chain_src_key or str(chain_src_key) == 'nan':
        return pd.DataFrame()

    try:
        chain_src_key_escaped = str(chain_src_key).replace("'", "''")
        where_clause = f"""
        --start_at_time <= fromUnixTimestamp64Milli({int(child_start_ms)}+1000)
        --AND start_at_time >= fromUnixTimestamp64Milli({int(child_end_ms)}-latency_msec)
        start_at_ms <= {int(child_start_ms)}
        AND start_at_ms + latency_msec >= {int(child_end_ms)}
        AND chain_dst_key = '{chain_src_key_escaped}'
        """
        sql = f"""
        {build_apm_cte(where_clause)}
        SELECT *
        FROM apm_view
        """
        # sql = f"""
        # SELECT *
        # FROM {CK_DATABASE}.apm
        # WHERE {where_clause}
        # ORDER BY start_at_ms
        # """
        logger.info(f"执行查询: {sql}...")
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
            # 记录源端信息，便于前端核查
            'src_ip': current_row.get('src_ip'),
            'srcSysName': current_row.get('srcSysName'),
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


# 以一条数据为入口串联（云下）
def chain_trace_from_selected_data(trace_id: str, direction: str = "down") -> pd.DataFrame:
    # 查询数据
    where_clause = f"where trace_id = {trace_id}"
    sql = f"""
    {build_apm_cte(where_clause)}
    select * from apm_view
    """
    # sql = f"""
    # SELECT *
    # FROM {CK_DATABASE}.apm
    # WHERE {where_clause}
    # ORDER BY start_at_ms
    # """
    logger.info(f"执行查询: {sql}...")
    query_res = query_ck(sql)
    if query_res.empty:
        selected_row = None
    else:
        if len(query_res) > 1:
            logger.warning("该trace_id有多条数据，自动选择第一条进行串联")
        selected_row = query_ck(sql).iloc[0].to_dict()

    result_df = chain_trace_from_selected_row(selected_row, direction)

    return result_df


########################################################################################################################

# 查询log_id符合条件的云上数据
def search_cloud_data_from_ck(log_ids: List, start_time: str = '', end_time: str = '') -> pd.DataFrame:
    """
    从 ClickHouse 查询云上数据
    """
    if not log_ids:
        return pd.DataFrame()

    try:
        valid_log_ids = [str(log_id).replace("'", "''") for log_id in log_ids if
                         log_id is not None and str(log_id) != 'nan']
        if not valid_log_ids:
            return pd.DataFrame()
        log_ids_str = "', '".join(valid_log_ids)
        sql = f"""
        SELECT *
        FROM {CK_DATABASE}.link
        WHERE source = 'transaction_action'
          AND log_id IN ('{log_ids_str}')
          AND start_at_time >= fromUnixTimestamp64Milli({int(datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)}) AND start_at_time < fromUnixTimestamp64Milli({int(datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)})
        ORDER BY start_time
        """
        logger.info(f"执行查询: {sql}...")
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
        # 云上数据通常没有 src_ip / srcSysName，这里补空列便于前端统一展示
        df_ck["src_ip"] = None
        df_ck["srcSysName"] = None
        logger.info(f"查询到 {len(df_ck)} 条云上数据")

        return df_ck
    except Exception as e:
        logger.error(f"查询云上数据失败: {e}")
        return pd.DataFrame()



# 验证云上调云下
def cloud_apm():
    full_link_data = pd.read_parquet("/Users/fangwang/apple/PycharmProject/云上云下串联/1219/fulllink-zstd-22.parquet")
    sql = f"""
    SELECT *
    FROM {CK_DATABASE}.apm
    WHERE chain_src_ip like "22.67%" and dst_ip="22.1.200.8"
    ORDER BY start_at_ms
    """
    logger.info(f"执行查询: {sql}...")
    apm_data = query_ck(sql)

    # 对full_link_data的每一行，找其子节点


########################################################################################################################

# 查询log_id符合条件的云上全链路日志数据
def query_full_link_log_data(log_id: str = None, global_id: str = None, start_time: str = '', end_time: str = '', limit: int = 5000) -> pd.DataFrame:
    """
    查询全链路日志数据（apm_full_link_log）
    - 以 log_id/global_id 作为过滤条件
    - 返回原始字段为主，具体时间字段在甘特图函数中再做毫秒转换
    """
    if not log_id and not global_id:
        return pd.DataFrame()

    start_ts = int(datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
    end_ts = int(datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
    where_conditions = [f"log_timestamp >= fromUnixTimestamp64Milli({start_ts})", f"log_timestamp < fromUnixTimestamp64Milli({end_ts})"]

    if log_id and str(log_id).strip():
        log_id_escaped = str(log_id).strip().replace("'", "''")
        where_conditions.append(f"log_id = '{log_id_escaped}'")
    if global_id and str(global_id).strip():
        global_id_escaped = str(global_id).strip().replace("'", "''")
        where_conditions.append(f"global_id = '{global_id_escaped}'")

    if not where_conditions:
        return pd.DataFrame()

    where_clause = " AND ".join(where_conditions)

    try:
        sql = f"""
        SELECT *
        FROM {CK_DATABASE}.ods_full_link_log
        WHERE {where_clause}
        ORDER BY log_timestamp
        LIMIT {int(limit) if limit else 5000}
        """
        df = query_ck(sql)
        logger.info(f"查询到 {len(df)} 条全链路日志数据")
        return df
    except Exception as e:
        logger.error(f"查询全链路日志数据失败: {e}")
        return pd.DataFrame()


def query_chain_condition_data(log_id: str = None, global_id: str = None, start_time: str = '', end_time: str = '') -> Tuple[pd.DataFrame, pd.DataFrame]:
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

    start_ts = int(datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
    end_ts = int(datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)

    # 构建查询条件
    # where_conditions_down = [f"start_at_ms >= {start_ts}", f"start_at_ms < {end_ts}"]
    where_conditions_down = [f"start_at_time >= fromUnixTimestamp64Milli({start_ts})", f"start_at_time < fromUnixTimestamp64Milli({end_ts})"]
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
        {build_apm_cte(where_clause_down)}
        select * from apm_view
        """
        # sql_down = f"""
        # SELECT *
        # FROM {CK_DATABASE}.apm
        # WHERE {where_clause_down}
        # --AND `sync-flag`='同步'
        # ORDER BY start_at_ms
        # """
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

            deploy_unit = df_down.get('deployUnitObjectName',
                                      pd.Series([''] * len(df_down), index=df_down.index)).astype(str)

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