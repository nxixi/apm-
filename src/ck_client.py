import pandas as pd
import clickhouse_connect
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


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


def build_apm_cte(where_clause: str = "1=1") -> str:
    """
    构建 APM 视图的 CTE，便于在各查询函数中复用。
    说明：
    - 基于用户提供的 with f5_vip + 主查询拼装
    - where_clause 由调用方传入，将查询条件下推到 CTE 中
    """
    return f"""
WITH
f5_vip AS (
    SELECT
        x.businessIp AS businessIp,
        vip,
        substringIndex(x.businessIp, '.', 1) AS net_zone,
        CASE net_zone
            WHEN 22 THEN 'TZ'
            WHEN 215 THEN 'XG'
            WHEN 25 THEN 'KY'
            WHEN 26 THEN 'PJ'
            ELSE 'error'
        END AS dataCenter,
        z.vip,
        z.port
    FROM {CK_DATABASE}.ads_sys_to_host_all_cmdb_one_to_many x
    JOIN {CK_DATABASE}.dws_biz_system y
        ON x.bizSystemObjectId = y.bizSystemObjectId
    LEFT JOIN {CK_DATABASE}.dws_deploy_unit_vip z
        ON x.deployUnitObjectId = z.deployUnitObjectId
        AND x.dataCenter = z.dataCenter
    WHERE sync_time > today()
),
apm_view AS (
    SELECT
        trace_id,
        log_id,
        msgId,
        dstSysName,
        deployUnitObjectId,
        deployUnitObjectName,
        recvSysname,
        dport,
        rtnCode,
        sendSysname,
        sport,
        srcSysName,
        start_at_time,
        start_at_ms,
        latency_msec,
        is_sys_success,
        is_busi_success,
        sync_time,
        `sync-flag`,
        src_ip,
        dst_ip,
        b.businessIp,
        coalesce(nullif(b.vip, ''), src_ip) AS chain_src_ip,
        msgType,
        global_id,
        w.ip AS w_ip,
        if(
            w.ip IS NULL,
            a.dst_ip,
            a.dst_ip || ':' || coalesce(a.msgType, '-') || ':' || coalesce(a.global_id, '-')
        ) AS chain_dst_key,
        v.ip AS v_ip,
        if(
            v.ip IS NULL,
            chain_src_ip,
            chain_src_ip || ':' || coalesce(a.msgType, '-') || ':' || coalesce(a.global_id, '-')
        ) AS chain_src_key
    FROM {CK_DATABASE}.dws_apm_trace_detail_process_join a
    LEFT JOIN f5_vip b
        ON a.dst_ip = b.businessIp
    LEFT JOIN {CK_DATABASE}.v_dws_esb_f5_ip w
        ON a.dst_ip = w.ip
    LEFT JOIN {CK_DATABASE}.v_dws_esb_f5_ip v
        ON chain_src_ip = v.ip
    WHERE `sync-flag`='同步' AND {where_clause}
    ORDER BY start_at_ms
)
"""


if __name__ == '__main__':
    l = query_ck("select count(*) from ccdc.apm WHERE log_id='cf65c14b4c4045ad9a19249f4fe7a07b' and '`sync-flag`='同步'")
    # df = query_ck("select * from ccdc.link where source='transaction_action' and log_id = '408f5b4e7e7f464bb8a50a39f144407b'")
    # df = query_ck("select * from ccdc.apm where log_id = '408f5b4e7e7f464bb8a50a39f144407b'")
    df = query_ck("select COUNT(*) as group_count from ccdc.link where source='transaction_action' group by log_id, span_id order by group_count desc")

    df = query_ck("select * from ccdc.link where source='transaction_action'")
    for key, group in df.groupby(['log_id', 'span_id']):
        if len(group) > 1:
            a = 1
    # df = query_ck("select DISTINCT msg_type from ccdc.link where source='transaction_action' and server_ip = '22.67.80.41' order by msg_type")
    a = 1