import csv
import datetime
import urllib.parse
import pathlib

import pandas as pd
import requests

d = {
    # 1:免許情報検索  2: 登録情報検索
    "ST": 1,
    # 詳細情報付加 0:なし 1:あり
    "DA": 1,
    # スタートカウント
    "SC": 1,
    # 取得件数
    "DC": 1,
    # 出力形式 1:CSV 2:JSON 3:XML
    "OF": 1,
    # 無線局の種別
    "OW": "FB_H",
    # 所轄総合通信局
    "IT": "E",
    # 免許人名称/登録人名称
    "NA": "楽天モバイル",
}

parm = urllib.parse.urlencode(d, encoding="shift-jis")

r = requests.get("https://www.tele.soumu.go.jp/musen/list", parm)
r.raise_for_status()

cr = csv.reader(r.text.splitlines(), delimiter=",")
data = list(cr)

# 更新日
update = datetime.datetime.strptime(data[0][0], "%Y-%m-%d").date()

# データラングリング

df0 = pd.DataFrame(data[1:]).dropna(how="all")

df1 = df0[25].str.strip().str.split(r"\\n", 2, expand=True)

se = df1.loc[df1[0].str.contains("携帯電話（その他基地局等"), 2]

df2 = (
    se.str.strip()
    .str.replace(r"\\n", "")
    .str.extractall("(.+?)\(([0-9,]+?)\)")
    .rename(columns={0: "市区町村名", 1: "開設局数"})
    .reset_index(drop=True)
)

df2["市区町村名"] = df2["市区町村名"].str.strip()

df2["開設局数"] = df2["開設局数"].str.strip().str.replace(",", "").astype(int)

flag = df2["市区町村名"].str.endswith(("都", "道", "府", "県"))

df2["都道府県名"] = df2["市区町村名"].where(flag).fillna(method="ffill")

df2["更新日"] = update.isoformat()

df3 = df2.reindex(["都道府県名", "市区町村名", "開設局数", "更新日"], axis=1)

# ディレクトリ作成

pathlib.Path("data").mkdir(parents=True, exist_ok=True)

# 都道府県

df_prefs = df3[flag].reset_index(drop=True)

df_prefs.drop("市区町村名", axis=1, inplace=True)

df_prefs.to_csv(pathlib.Path("data", "prefs.csv"), index=False, encoding="utf_8_sig")

# 市区町村

df_cities = df3[~flag].reset_index(drop=True)

df_cities.to_csv(pathlib.Path("data", "cities.csv"), index=False, encoding="utf_8_sig")
