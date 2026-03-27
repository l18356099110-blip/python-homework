import pandas as pd

# =========================
# 1. 文件路径
# =========================
input_path = r"E:\python\VENV\lesson2\Hero.csv"
output_path = r"E:\python\VENV\lesson2\英雄信息汇总.csv"

# =========================
# 2. 映射字典
# =========================
SORT_MAP = {
    1: "坦克",
    2: "战士",
    3: "刺客",
    4: "法师",
    5: "射手",
    6: "辅助"
}

ATK_TYPE_MAP = {
    1: "近战",
    2: "远程"
}

# =========================
# 3. 通用多值映射函数（核心）
# =========================
def map_multi_value(value, mapping):
    """
    将 1;4;5 → 坦克;法师;射手
    多个结果保留在同一个单元格，用分号隔开
    """
    if pd.isna(value):
        return "其他"

    # 转字符串，去首尾空格和多余分号
    value = str(value).strip().strip(";")
    if not value:
        return "其他"

    result = []
    for item in value.split(";"):
        item = item.strip()
        if not item.isdigit():
            result.append("其他")
            continue

        result.append(mapping.get(int(item), "其他"))

    return ";".join(result)

# =========================
# 4. 读取 & 处理数据
# =========================
try:
    df = pd.read_csv(
        input_path,
        encoding="utf-16",
        sep="\t",
        header=1
    )

    # 清洗列名
    df.columns = df.columns.str.replace("\ufeff", "", regex=False).str.strip()

    # 只保留需要的列
    df = df[["ID", "mName", "Sort", "CommonAtkType"]]

    # 多值字段映射（仍然在同一个单元格）
    df["Sort"] = df["Sort"].apply(lambda x: map_multi_value(x, SORT_MAP))
    df["CommonAtkType"] = df["CommonAtkType"].apply(lambda x: map_multi_value(x, ATK_TYPE_MAP))

    # 重命名列
    df.rename(columns={
        "ID": "英雄ID",
        "mName": "英雄名称",
        "Sort": "职业",
        "CommonAtkType": "普攻类型"
    }, inplace=True)

    # ============= 新增：删除输出 CSV 的第 2 行（即数据框的第一条记录） =============
    if len(df) >= 1:
        df = df.drop(df.index[0]).reset_index(drop=True)
    # ===========================================================================

    # 输出
    df.to_csv(
        output_path,
        index=False,
        encoding="utf-8-sig"
    )

    print(f"✅ 处理完成：{output_path}")
    print(f"📊 共处理 {len(df)} 条英雄数据")

except Exception as e:
    print(f"❌ 处理失败：{e}")
