import os
import requests
import logging
import time
from datetime import datetime, timedelta
from collections import Counter
from logging.handlers import RotatingFileHandler
from threading import Timer
from dotenv import load_dotenv

# ======================== 【配置区】 ========================
# 从 .env 文件加载配置（不会上传到GitHub）
load_dotenv()

# 定时任务配置
AUTO_RUN_HOUR = 15
AUTO_RUN_MINUTE = 53

# 日志配置
LOG_FILE_PATH = "task.log"
LOG_MAX_SIZE = 10 * 1024 * 1024
LOG_BACKUP_COUNT = 5
# ============================================================

def init_logger():
    """初始化日志系统，同时输出到控制台和文件"""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(log_format, datefmt=date_format)

    # 控制台输出
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # 文件输出
    file_handler = RotatingFileHandler(
        LOG_FILE_PATH,
        maxBytes=LOG_MAX_SIZE,
        backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8"
    )
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

def get_tenant_token():
    """获取飞书租户访问令牌"""
    print("CLIENT_ID:", os.getenv("CLIENT_ID"))  # 调试用，看是否读到配置
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    data = {
        "app_id": os.getenv("CLIENT_ID"),
        "app_secret": os.getenv("CLIENT_SECRET")
    }
    resp = requests.post(url, json=data)

    if resp.status_code != 200:
        raise Exception(f"获取令牌失败：{resp.text}")

    token = resp.json().get("tenant_access_token")
    if not token:
        raise Exception("未获取到有效 token")
    return token

def fetch_bitable_data(token):
    """分页获取多维表格全部数据"""
    all_records = []
    page_token = None

    while True:
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{os.getenv('APP_TOKEN')}/tables/{os.getenv('TABLE_ID')}/records"
        headers = {"Authorization": f"Bearer {token}"}
        params = {"page_size": 500}
        if page_token:
            params["page_token"] = page_token

        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code != 200:
            raise Exception(f"拉取数据失败：{resp.text}")

        data = resp.json()
        items = data.get("data", {}).get("items", [])
        all_records.extend(items)

        page_token = data.get("data", {}).get("page_token")
        if not page_token:
            break

    return all_records

def analyze_hero(records):
    """分析英雄职业与技能类型分布"""
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

    profession_counter = Counter(hero_map.values())
    return profession_counter, skill_counter, len(hero_map)

def show_report(prof_dist, skill_dist, hero_cnt, record_cnt):
    """输出数据分析报告"""
    logging.info("\n========== 英雄数据分析报告 ==========")
    logging.info(f"总记录数：{record_cnt}")
    logging.info(f"有效英雄数：{hero_cnt}")

    logging.info("\n【职业分布】")
    for name, cnt in prof_dist.most_common():
        logging.info(f"- {name}：{cnt}人 ({cnt / hero_cnt:.1%})")

    logging.info("\n【技能类型】")
    for name, cnt in skill_dist.most_common():
        logging.info(f"- {name}：{cnt}次")

def run_task():
    """执行一次完整任务流程"""
    try:
        logging.info("🚀 开始执行每日任务")
        token = get_tenant_token()
        records = fetch_bitable_data(token)
        prof_dist, skill_dist, hero_cnt = analyze_hero(records)
        show_report(prof_dist, skill_dist, hero_cnt, len(records))
        logging.info("✅ 任务执行完成\n")
    except Exception as e:
        logging.error("❌ 任务执行异常", exc_info=True)

def schedule_task():
    """每日定时调度：8:00 自动执行"""
    now = datetime.now()
    target = now.replace(hour=AUTO_RUN_HOUR, minute=AUTO_RUN_MINUTE, second=0, microsecond=0)
    if now > target:
        target += timedelta(days=1)

    delay = (target - now).total_seconds()
    logging.info(f"⏰ 下次执行：{target}")
    logging.info(f"等待时间：{delay / 3600:.1f} 小时\n")

    t = Timer(delay, run_task)
    t.start()
    t.join()

def main():
    """主函数：每秒检查一次时间，到点执行"""
    init_logger()
    logging.info("✅ 服务已启动，每天 %02d:%02d 自动执行\n" % (AUTO_RUN_HOUR, AUTO_RUN_MINUTE))

    last_exec_date = None  # 记录今天是否已经跑过

    while True:
        now = datetime.now()
        current_date = now.date()
        current_hour = now.hour
        current_minute = now.minute

        # 到了设定时间，且今天还没执行
        if current_hour == AUTO_RUN_HOUR and current_minute == AUTO_RUN_MINUTE:
            if last_exec_date != current_date:
                run_task()
                last_exec_date = current_date

        # 每秒检查一次（你要的 1s）
        time.sleep(1)

if __name__ == "__main__":
    main()