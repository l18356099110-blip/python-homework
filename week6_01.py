# ======================================================
# 【配置区：所有可修改的配置全部在这里】
# ======================================================
AUTO_RUN_MINUTE_INTERVAL = 1   # 每 2 分钟执行一次（演示用）
LOG_FILE_PATH = "task.log"     # 日志文件
LOG_MAX_SIZE = 10 * 1024 * 1024
LOG_BACKUP_COUNT = 5

# ======================================================
# 导入依赖库
# ======================================================
import os
import logging
import requests
from collections import Counter
from logging.handlers import RotatingFileHandler
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from dotenv import load_dotenv
from pathlib import Path

# ======================================================
# 加载 .env 配置（绝对路径，不会读不到）
# ======================================================
env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path)

# ======================================================
# 日志初始化（输出到文件 + 控制台）
# ======================================================
def init_logger():
    """初始化日志系统，同时输出到控制台和文件"""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = "%(asctime)s - %(levelname)s - %(message)s"
    date_fmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt, datefmt=date_fmt)

    # 控制台输出
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)

    # 文件输出（自动切分）
    fh = RotatingFileHandler(
        LOG_FILE_PATH,
        maxBytes=LOG_MAX_SIZE,
        backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8"
    )
    fh.setFormatter(formatter)

    logger.addHandler(ch)
    logger.addHandler(fh)

# ======================================================
# 函数1：获取飞书 token
# ======================================================
def get_tenant_token():
    """获取飞书租户访问令牌"""
    logging.info("正在获取飞书 Token...")
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    resp = requests.post(url, json={
        "app_id": os.getenv("CLIENT_ID"),
        "app_secret": os.getenv("CLIENT_SECRET")
    })

    if resp.status_code != 200:
        logging.error(f"获取 Token 失败：{resp.text}")
        return None

    data = resp.json()
    token = data.get("tenant_access_token")
    if not token:
        logging.error("未获取到有效 Token")
        return None

    logging.info("Token 获取成功")
    return token

# ======================================================
# 函数2：拉取多维表格数据
# ======================================================
def fetch_bitable_data(token):
    """从飞书多维表格拉取全部记录数据"""
    logging.info("开始拉取多维表格数据...")
    app_token = os.getenv("APP_TOKEN")
    table_id = os.getenv("TABLE_ID")
    records = []

    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"page_size": 500}

    resp = requests.get(url, headers=headers, params=params)
    if resp.status_code == 200:
        items = resp.json().get("data", {}).get("items", [])
        records.extend(items)
        logging.info(f"拉取完成，共 {len(records)} 条记录")
    else:
        logging.error("拉取数据失败")

    return records

# ======================================================
# 函数3：数据分析
# ======================================================
def analyze_data(records):
    """分析职业分布与技能类型分布"""
    logging.info("开始数据分析...")
    hero_map = {}
    skill_counter = Counter()

    for record in records:
        fields = record.get("fields", {})
        hero_id = fields.get("英雄ID")
        profession = fields.get("英雄职业", "未知")
        skill_type = fields.get("技能类型", "未知")

        if hero_id:
            hero_map[hero_id] = profession
        skill_counter[skill_type] += 1

    prof_counter = Counter(hero_map.values())
    logging.info(f"有效英雄数量：{len(hero_map)}")
    return prof_counter, skill_counter, len(hero_map)

# ======================================================
# 函数4：输出报告
# ======================================================
def show_report(prof_dist, skill_dist, hero_cnt, record_cnt):
    """展示分析结果"""
    logging.info("\n====== 数据分析报告 ======")
    logging.info(f"总记录数：{record_cnt}")
    logging.info(f"有效英雄数：{hero_cnt}")

    logging.info("\n【职业分布】")
    for k, v in prof_dist.most_common():
        logging.info(f"- {k}：{v}人")

    logging.info("\n【技能类型统计】")
    for k, v in skill_dist.most_common():
        logging.info(f"- {k}：{v}次")
    logging.info("==========================\n")

# ======================================================
# 主函数：完整业务流程
# ======================================================
def main():
    """主执行函数：一次完整的任务流程"""
    logging.info("=" * 40)
    logging.info("任务开始执行")
    logging.info("=" * 40)

    token = get_tenant_token()
    if not token:
        return

    records = fetch_bitable_data(token)
    if not records:
        return

    prof_dist, skill_dist, hero_cnt = analyze_data(records)
    show_report(prof_dist, skill_dist, hero_cnt, len(records))

    logging.info("任务执行完成 ✅\n")

# ======================================================
# 定时任务（每 2 分钟执行一次）
# ======================================================
def start_scheduler():
    """启动定时任务：每 2 分钟执行一次 main()"""
    scheduler = BlockingScheduler(timezone="Asia/Shanghai")
    scheduler.add_job(main, "interval", minutes=AUTO_RUN_MINUTE_INTERVAL)

    logging.info(f"\n✅ 定时任务已启动：每 {AUTO_RUN_MINUTE_INTERVAL} 分钟执行一次")
    logging.info("⏰ 等待自动执行...\n")

    try:
        scheduler.start()
    except KeyboardInterrupt:
        logging.info("🛑 任务已手动停止")

# ======================================================
# 程序入口
# ======================================================
if __name__ == "__main__":
    init_logger()
    main()          # 启动立刻执行一次
    start_scheduler() # 然后每 2 分钟自动执行