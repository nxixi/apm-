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
with
f5_vip as (
  select x.businessIp as businessIp, vip, splitByChar(':', ifNull(`--x.businessIp`, ''))[1] AS net_zone,
  case net_zone
    when 22 then 'TZ'
    when 215 then 'XG'
    when 25 then 'KY'
    when 26 then 'PJ'
    else 'error'
  end as dataCenter,
  z.vip,z.port
  from {CK_DATABASE}.ads_sys_to_host_all_cmdb_one_to_many x
  global join {CK_DATABASE}.dws_biz_system y
  on x.bizSystemObjectId = y.bizSystemObjectId
  global left join {CK_DATABASE}.dws_deploy_unit_vip z
  on x.deployUnitObjectId = z.deployUnitObjectId and x.dataCenter = z.dataCenter
  where sync_time > today()
),
f5_or_esb_ip as (
  select ip from {CK_DATABASE}.v_dws_esb_f5_ip
  union distinct
  select vip from {CK_DATABASE}.dws_deploy_unit_vip
),
apm_view AS (
select trace_id, log_id, msgId, dstSysName, deployUnitObjectId, deployUnitObjectName,
  recvSysname, dport, rtnCode, sendSysname, sport,
  srcSysName, start_at_time, start_at_ms,
  latency_msec, is_sys_success, is_busi_success,
  sync_time, `sync-flag`,
  src_ip, dst_ip, b.businessIp,
coalesce(nullif(b.vip,''), src_ip) as chain_src_ip,
msgType, global_id,
w.ip as w_ip,
if(w.ip is null, a.dst_ip, a.dst_ip ||':'||coalesce(a.msgType,'-')||':'|| coalesce(a.global_id,'-')) as chain_dst_key,
v.ip as v_ip,
if(v.ip is null, chain_src_ip, chain_src_ip ||':'||coalesce(a.msgType,'-')||':'|| coalesce(a.global_id,'-')) as chain_src_key
from {CK_DATABASE}.dws_apm_trace_detail_process_join a
global left join f5_vip b on a.dst_ip = b.businessIp
--left join {CK_DATABASE}.v_dws_esb_f5_ip w on a.dst_ip = w.ip
--left join {CK_DATABASE}.v_dws_esb_f5_ip v on chain_src_ip = v.ip
global left join f5_or_esb_ip w on a.dst_ip = w.ip
global left join f5_or_esb_ip v on chain_src_ip = v.ip
WHERE
--sync-flag`='同步' AND
{where_clause}
ORDER BY start_at_ms
)
"""


if __name__ == '__main__':
    # df = pd.read_parquet("/Users/fangwang/apple/PycharmProject/apm串联/data/ads_sys_to_host_all_cmdb_one_to_many.parquet")
    # sql = """
    # SELECT
    # location,
    # MAX(time_minute)
    # AS
    # max_time_minute
    # FROM
    # ccdc.dws_storm_event_count_1m
    # GROUP
    # BY
    # location
    # """
    # res = query_ck(sql)

    # 查看ccdc下的所有表
    tb = list(query_ck('show tables')['name'])
    # query_ck("SELECT name, metadata_modification_time  FROM system.tables WHERE database = 'ccdc'  ORDER BY metadata_modification_time DESC;")
    a = 1
    # ads_sys_to_host_all_cmdb_one_to_many_res = query_ck("select * FROM ccdc.ads_sys_to_host_all_cmdb_one_to_many")   # 326513
    # dws_biz_system_res = query_ck("select * FROM ccdc.dws_biz_system")   # 30
    # dws_deploy_unit_vip_res = query_ck("select * FROM ccdc.dws_deploy_unit_vip")   # 41
    # dws_f5_snat_res = query_ck("select * FROM ccdc.dws_f5_snat")   # 16
    # v_dws_esb_f5_ip_res = query_ck("select * FROM ccdc.v_dws_esb_f5_ip")   # 38
    # apm_res = query_ck("select * FROM ccdc.dws_apm_trace_detail_process_join")   # 1991733
    # full_link_res = query_ck("select * FROM ccdc.ods_full_link_log")   # 224135
    #
    # res = query_ck("select * FROM ccdc.dwd_storm_event_baseline")
    # res = query_ck("select * FROM ccdc.dws_storm_event_count_1m")
    #
    #
    # tb = query_ck("select * FROM ccdc.apm_full_link_log_local where log_id =='7d82a168141247a5b279429eb55e51c0'")
    # l = query_ck("select count(*) from ccdc.apm WHERE log_id='cf65c14b4c4045ad9a19249f4fe7a07b' and '`sync-flag`='同步'")
    # # df = query_ck("select * from ccdc.link where source='transaction_action' and log_id = '408f5b4e7e7f464bb8a50a39f144407b'")
    # # df = query_ck("select * from ccdc.apm where log_id = '408f5b4e7e7f464bb8a50a39f144407b'")
    # df = query_ck("select COUNT(*) as group_count from ccdc.link where source='transaction_action' group by log_id, span_id order by group_count desc")
    #
    # df = query_ck("select * from ccdc.link where source='transaction_action'")
    # for key, group in df.groupby(['log_id', 'span_id']):
    #     if len(group) > 1:
    #         a = 1
    # # df = query_ck("select DISTINCT msg_type from ccdc.link where source='transaction_action' and server_ip = '22.67.80.41' order by msg_type")
    # a = 1