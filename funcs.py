import pandas as pd
import numpy as np
from typing import List, Dict, Tuple
import time
from collections import defaultdict


class APMDataLinker:
    def __init__(self, left_offset: int = 10, right_offset: int = 10):
        self.left_offset = left_offset
        self.right_offset = right_offset

    def link_apm_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        串联APM数据，添加父节点和子节点信息

        Parameters:
        -----------
        df : pd.DataFrame
            包含以下列的数据框：
            index, r_src_ip, r_dst_ip, start_at_ms, latency_msec,
            end_at_ms, msgType, global_id

        Returns:
        --------
        pd.DataFrame
            添加了六列的数据框：
            candidate_parents, same_msgtype_parents, same_globalid_parents
            candidate_children, same_msgtype_children, same_globalid_children
        """
        print("开始处理APM数据串联 (双向链路)...")
        start_time = time.time()

        # 确保数据按时间排序
        df = df.sort_values('start_at_ms').reset_index(drop=True)
        # 重新生成索引以确保连续
        df['index'] = range(len(df))

        # 1. 初始化结果列 (父节点)
        df['candidate_parents'] = -1
        df['same_msgtype_parents'] = -1
        df['same_globalid_parents'] = -1

        # 2. 初始化子节点临时存储 (使用字典列表，key为父节点index，value为子节点index列表)
        # 相比直接操作DataFrame，操作字典速度更快
        children_map = defaultdict(list)
        msgtype_children_map = defaultdict(list)
        globalid_children_map = defaultdict(list)

        # 提取numpy数组以提高访问速度
        indices = df['index'].values
        src_ips = df['r_src_ip'].values
        dst_ips = df['r_dst_ip'].values
        start_times = df['start_at_ms'].values
        end_times = df['end_at_ms'].values
        msg_types = df['msgType'].values
        global_ids = df['global_id'].values

        # 构建IP到索引的映射
        ip_to_indices = self._build_ip_index_mapping(df)

        print("开始遍历节点构建关系...")

        # 为每个节点寻找父节点 (同时反向构建子节点关系)
        for i in range(len(df)):
            if i % 10000 == 0 and i > 0:
                print(f"已处理 {i}/{len(df)} 条记录...")

            # --- 寻找父节点 ---
            candidate_parents = self._find_candidate_parents(
                i, src_ips, dst_ips, start_times, end_times, ip_to_indices
            )

            if candidate_parents:
                # 记录父节点到 DataFrame
                df.at[i, 'candidate_parents'] = ','.join(map(str, candidate_parents))

                # 【关键逻辑】反向记录：这些父节点的子节点就是当前节点 i
                for p_idx in candidate_parents:
                    children_map[p_idx].append(i)

                # --- 筛选同 msgType ---
                same_msgtype_parents = self._filter_same_msgtype(
                    candidate_parents, msg_types[i], msg_types
                )
                if same_msgtype_parents:
                    df.at[i, 'same_msgtype_parents'] = ','.join(map(str, same_msgtype_parents))
                    # 反向记录同 msgType 子节点
                    for p_idx in same_msgtype_parents:
                        msgtype_children_map[p_idx].append(i)

                # --- 筛选同 global_id ---
                same_globalid_parents = self._filter_same_globalid(
                    candidate_parents, global_ids[i], global_ids, indices
                )
                if same_globalid_parents:
                    df.at[i, 'same_globalid_parents'] = ','.join(map(str, same_globalid_parents))
                    # 反向记录同 global_id 子节点
                    for p_idx in same_globalid_parents:
                        globalid_children_map[p_idx].append(i)

        # 3. 将子节点字典转换为DataFrame列
        print("正在写入子节点信息...")

        # 预分配列表以加速列创建
        cand_children_col = [-1] * len(df)
        msg_children_col = [-1] * len(df)
        gid_children_col = [-1] * len(df)

        # 只需要遍历有子节点的记录
        for idx, children in children_map.items():
            cand_children_col[idx] = ','.join(map(str, children))

        for idx, children in msgtype_children_map.items():
            msg_children_col[idx] = ','.join(map(str, children))

        for idx, children in globalid_children_map.items():
            gid_children_col[idx] = ','.join(map(str, children))

        # 赋值回 DataFrame
        df['candidate_children'] = cand_children_col
        df['same_msgtype_children'] = msg_children_col
        df['same_globalid_children'] = gid_children_col

        end_time = time.time()
        print(f"处理完成！耗时: {end_time - start_time:.2f} 秒")

        return df

    def _build_ip_index_mapping(self, df: pd.DataFrame) -> Dict:
        """构建IP地址到索引的映射，分批处理大数据集"""
        # 注意：这里我们只映射 dst_ip，因为我们在寻找 current.src_ip == parent.dst_ip
        ip_to_indices = {}
        # 如果数据量巨大，可以调整 batch_size
        batch_size = 100000

        for start_idx in range(0, len(df), batch_size):
            end_idx = min(start_idx + batch_size, len(df))
            batch_df = df.iloc[start_idx:end_idx]

            for idx, row in batch_df.iterrows():
                dst_ip = row['r_dst_ip']
                if pd.notna(dst_ip):  # 简单的空值检查
                    if dst_ip not in ip_to_indices:
                        ip_to_indices[dst_ip] = []
                    ip_to_indices[dst_ip].append(idx)

        return ip_to_indices

    def _find_candidate_parents(self, current_idx: int, src_ips: np.ndarray,
                                dst_ips: np.ndarray, start_times: np.ndarray,
                                end_times: np.ndarray, ip_to_indices: Dict) -> List[int]:
        """寻找候选父节点"""
        candidate_parents = []
        current_src_ip = src_ips[current_idx]
        current_start_time = start_times[current_idx]
        current_end_time = end_times[current_idx]

        # 查找可能的父节点（当前节点的src_ip等于父节点的dst_ip）
        # 父节点是流量的接收方(dst)，当前节点是流量的发送方(src)，或者反之取决于具体的APM模型
        # 通常：Child(src) -> Parent(dst) 这种调用关系意味着 Parent 是被调用方(Server)，Child 是调用方(Client)
        # 代码逻辑：寻找 index j (Parent), 使得 Parent.dst_ip == Child.src_ip
        # 这意味着我们在寻找"谁接收了我的请求"或者"我作为Client连接了谁"

        if current_src_ip in ip_to_indices:
            potential_parent_indices = ip_to_indices[current_src_ip]

            for parent_idx in potential_parent_indices:
                # 确保父节点在当前节点之前 (基于 sort_values('start_at_ms'))
                # 注意：如果存在时钟偏差，父节点可能略晚于子节点开始，如果需要处理这种情况，
                # 可以移除这个 if check，仅依赖下面的时间窗口判断。
                # if parent_idx >= current_idx:
                #     continue

                parent_start_time = start_times[parent_idx]
                parent_end_time = end_times[parent_idx]

                # 检查时间窗口条件：父节点包裹子节点
                time_condition = (
                        (parent_start_time - self.left_offset <= current_start_time) and
                        (parent_end_time + self.right_offset >= current_end_time)
                )

                if time_condition:
                    candidate_parents.append(parent_idx)

        return candidate_parents

    def _filter_same_msgtype(self, candidate_parents: List[int],
                             current_msgtype: str, msg_types: np.ndarray) -> List[int]:
        """筛选相同msgType的父节点"""
        return [
            parent_idx for parent_idx in candidate_parents
            if msg_types[parent_idx] == current_msgtype
        ]

    def _filter_same_globalid(self, candidate_parents: List[int],
                              current_globalid: str, global_ids: np.ndarray,
                              indices: np.ndarray) -> List[int]:
        """筛选相同global_id的父节点（仅在都有值时判断）"""
        same_globalid_parents = []

        for parent_idx in candidate_parents:
            parent_globalid = global_ids[parent_idx]

            # 只有当两个节点的global_id都有值时才需要比较
            if (pd.notna(current_globalid) and pd.notna(parent_globalid)):
                if current_globalid == parent_globalid:
                    same_globalid_parents.append(parent_idx)
            # 这里原有逻辑是：如果有空值则保留。如果你的业务逻辑是必须相等才保留，可以去掉else
            else:
                same_globalid_parents.append(parent_idx)

        return same_globalid_parents


def optimize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """优化数据框内存使用"""
    # 转换IP地址为category类型节省内存
    if 'r_src_ip' in df.columns:
        df['r_src_ip'] = df['r_src_ip'].astype('category')
    if 'r_dst_ip' in df.columns:
        df['r_dst_ip'] = df['r_dst_ip'].astype('category')
    if 'msgType' in df.columns:
        df['msgType'] = df['msgType'].astype('category')

    # 转换数值列为适当类型
    numeric_cols = ['start_at_ms', 'latency_msec', 'end_at_ms']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    return df


# ==============================================================================

import pandas as pd
import numpy as np
from typing import List, Dict, Set, Tuple, Literal
from collections import deque
import time


class APMTraceFinder:
    """
    根据APM数据中的预计算关系（父节点/子节点列表），
    使用广度优先搜索 (BFS) 算法查找完整链路。
    """

    def __init__(self, df: pd.DataFrame):
        # 优化数据访问：确保'index'列存在并将其设为索引
        if 'index' not in df.columns:
            df['index'] = range(len(df))
        self.df = df.set_index('index')
        self.max_index = len(df)

        # 定义可选的关系列，方便后续调用
        self.parent_cols = ['candidate_parents', 'same_msgtype_parents', 'same_globalid_parents']
        self.child_cols = ['candidate_children', 'same_msgtype_children', 'same_globalid_children']

    def _get_indices(self, data) -> List[int]:
        """将逗号分隔的字符串（或 -1）转换为整数列表"""
        if pd.isna(data) or data == -1:
            return []

        if isinstance(data, (str, int, float)):
            # 确保 -1 被正确处理
            if data == '-1' or data == -1:
                return []
            try:
                # 假设存储时已经是以逗号分隔的字符串
                return [int(x) for x in str(data).split(',') if x.strip().isdigit()]
            except ValueError:
                return []  # 遇到非数字字符，返回空列表
        return []

    def _traverse_bfs(self,
                      start_index: int,
                      parent_col: str,
                      child_col: str,
                      mode: Literal[1, 2]
                      ) -> Tuple[Set[int], Set[Tuple[int, int]]]:
        """
        核心BFS遍历函数，区分两种模式。

        模式1 (Strict Path): 仅向上追踪父节点，向下追踪子节点。
        模式2 (Full Subgraph): 追踪所有邻居（父母和子女），构建完整子图。
        """
        if start_index not in self.df.index:
            raise ValueError(f"起始索引 {start_index} 不在数据集中。")

        # 使用队列进行 BFS
        queue = deque([start_index])
        visited_nodes: Set[int] = {start_index}
        found_edges: Set[Tuple[int, int]] = set()

        # 仅模式1需要，用于限制遍历方向
        # 存储节点的来源方向，只有模式1需要
        # Key: 节点ID, Value: 'up' (来自父节点链) 或 'down' (来自子节点链)
        source_direction: Dict[int, Literal['up', 'down']] = {start_index: 'center'}

        while queue:
            u = queue.popleft()  # 当前处理的节点

            # 1. 查找和处理父节点 (Edge: p -> u)
            parents = self._get_indices(self.df.at[u, parent_col])

            # 2. 查找和处理子节点 (Edge: u -> c)
            children = self._get_indices(self.df.at[u, child_col])

            # 遍历父节点
            for p in parents:
                if p not in self.df.index: continue

                # 检查是否允许向上追溯
                is_up_allowed = (mode == 2) or (source_direction.get(u) in ['center', 'up'])

                # 关键修正：只有允许追溯的方向，才添加边和继续追溯
                if is_up_allowed:
                    found_edges.add((p, u))  # 关系：父节点指向当前节点 (只在允许时添加边)

                    if p not in visited_nodes:
                        visited_nodes.add(p)
                        queue.append(p)
                        source_direction[p] = 'up'
                # else: 如果 mode=1 且 source_direction='down'，则忽略这条侧向边 (24->42, 24->26)

            # 遍历子节点
            for c in children:
                if c not in self.df.index: continue

                # 检查是否允许向下追溯
                is_down_allowed = (mode == 2) or (source_direction.get(u) in ['center', 'down'])

                # 关键修正：只有允许追溯的方向，才添加边和继续追溯
                if is_down_allowed:
                    found_edges.add((u, c))  # 关系：当前节点指向子节点

                    if c not in visited_nodes:
                        visited_nodes.add(c)
                        queue.append(c)
                        source_direction[c] = 'down'

        return visited_nodes, found_edges

    def find_trace(self,
                   start_index: int,
                   trace_type: Literal[1, 2] = 2,
                   parent_rel: str = 'candidate_parents',
                   child_rel: str = 'candidate_children'
                   ) -> Dict:
        """
        主函数：根据指定类型和关系查找链路并格式化为图表数据。

        Parameters:
        -----------
        start_index : int
            起始节点的索引。
        trace_type : Literal[1, 2]
            1: 严格路径 (Ancestors-Descendants chain)。
            2: 完整子图 (Full Neighborhood/Subgraph)。
        parent_rel : str
            用于向上追踪的关系列名 (e.g., 'candidate_parents')。
        child_rel : str
            用于向下追踪的关系列名 (e.g., 'candidate_children')。

        Returns:
        --------
        Dict
            包含 'nodes' 和 'edges' 列表，可用于 Dash Cytoscape 或 FefferyGraph。
        """

        if parent_rel not in self.df.columns or child_rel not in self.df.columns:
            raise ValueError("指定的父/子关系列不存在于DataFrame中。")

        start_time = time.time()

        # 执行遍历
        nodes_set, edges_set = self._traverse_bfs(
            start_index, parent_rel, child_rel, trace_type
        )

        end_time = time.time()
        print(f"链路查找完成 (模式{trace_type})，耗时: {end_time - start_time:.4f} 秒。")
        print(f"找到 {len(nodes_set)} 个节点， {len(edges_set)} 条边。")

        # 格式化输出为图表组件所需的JSON格式
        nodes_data = []
        for index in nodes_set:
            row = self.df.loc[index]
            
            # 将pandas Series转换为字典，并添加is_start标记
            row_dict = row.to_dict()
            row_dict['is_start'] = (index == start_index)

            # 为图表节点准备数据
            node_data = {
                'id': str(index),
                'label': f"Idx:{index} | Src:{row.get('r_src_ip', 'N/A')}",
                # 可以在这里添加更多属性用于样式或显示
                'data': row_dict
            }
            nodes_data.append(node_data)

        edges_data = []
        for source, target in edges_set:
            # 尝试获取源节点的msgType作为边标签
            try:
                source_row = self.df.loc[source]
                msg_type = source_row.get('msgType', '')
                # 如果msgType存在且不是None/NaN，使用它；否则留空
                if pd.notna(msg_type) and msg_type != '' and msg_type != 'None':
                    edge_label = str(msg_type)
                else:
                    edge_label = ''  # 留空，让图更简洁
            except:
                edge_label = ''
            
            edges_data.append({
                'id': f"{source}-{target}",
                'source': str(source),
                'target': str(target),
                'label': edge_label
            })

        return {
            'nodes': nodes_data,
            'edges': edges_data
        }


if __name__ == '__main__':
    df = pd.read_csv('result/candidate_result_20251117.csv')

    ip_to_indices = {}
    # 如果数据量巨大，可以调整 batch_size
    batch_size = 100000

    for start_idx in range(0, len(df), batch_size):
        end_idx = min(start_idx + batch_size, len(df))
        batch_df = df.iloc[start_idx:end_idx]

        for idx, row in batch_df.iterrows():
            dst_ip = row['r_dst_ip']
            if pd.notna(dst_ip):  # 简单的空值检查
                if dst_ip not in ip_to_indices:
                    ip_to_indices[dst_ip] = []
                ip_to_indices[dst_ip].append(idx)

    a = 1

    # finder = APMTraceFinder(result_df)
    # trace_data_type1 = finder.find_trace(
    #     start_index=2,  # 假设从索引 2 开始查找
    #     trace_type=1,
    #     parent_rel='candidate_parents',
    #     child_rel='candidate_children'
    # )

    a = 1