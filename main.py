import csv
import datetime
import pathlib
import time
import urllib.parse

import pandas as pd
import requests

api = {
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
    "IT": "A",
    # 免許人名称/登録人名称
    "NA": "楽天モバイル",
}

musen = [
    {"auth": "01_hokkaido", "value": "J"},
    {"auth": "02_tohoku", "value": "I"},
    {"auth": "03_kanto", "value": "A"},
    {"auth": "04_shinetsu", "value": "B"},
    {"auth": "05_hokuriku", "value": "D"},
    {"auth": "06_tokai", "value": "C"},
    {"auth": "07_kinki", "value": "E"},
    {"auth": "08_chugoku", "value": "F"},
    {"auth": "09_shikoku", "value": "G"},
    {"auth": "10_kyushu", "value": "H"},
    {"auth": "11_okinawa", "value": "O"},
]

df_code = pd.read_csv(
    "https://docs.google.com/spreadsheets/d/e/2PACX-1vSseDxB5f3nS-YQ1NOkuFKZ7rTNfPLHqTKaSag-qaK25EWLcSL0klbFBZm1b6JDKGtHTk6iMUxsXpxt/pub?gid=284869672&single=true&output=csv",
    dtype={"団体コード": int, "都道府県名": str, "郡名": str, "市区町村名": str},
)

df_code["市区町村名"] = df_code["郡名"].fillna("") + df_code["市区町村名"].fillna("")
df_code.drop("郡名", axis=1, inplace=True)

df_code


def fetch_api(parm, auth):

    r = requests.get("https://www.tele.soumu.go.jp/musen/list", parm)
    r.raise_for_status()

    cr = csv.reader(r.text.splitlines(), delimiter=",")
    data = list(cr)

    # 更新日
    latest = datetime.datetime.strptime(data[0][0], "%Y-%m-%d").date()

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

    # 都道府県を抽出
    flag = df2["市区町村名"].str.endswith(("都", "道", "府", "県"))

    # 都道府県に移動
    df2["都道府県名"] = df2["市区町村名"].where(flag).fillna(method="ffill")

    df2["市区町村名"] = df2["市区町村名"].mask(flag, "")

    # df2["更新日"] = latest.isoformat()

    # 団体コードを付加
    df3 = (
        pd.merge(df2, df_code, on=["都道府県名", "市区町村名"], how="left")
        .reset_index(drop=True)
        .reindex(["団体コード", "都道府県名", "市区町村名", "開設局数"], axis=1)
    )

    df3["団体コード"] = df3["団体コード"].astype("Int64")

    df3.sort_values("団体コード", inplace=True)

    df3.to_csv(f"{auth}.csv", index=False, encoding="utf_8_sig")

    # 都道府県

    df_prefs = df3[flag].reset_index(drop=True)

    df_prefs.drop("市区町村名", axis=1, inplace=True)

    df_prefs.to_csv(
        pathlib.Path("data", f"{auth}_prefs.csv"), index=False, encoding="utf_8_sig"
    )

    # 市区町村

    df_cities = df3[~flag].reset_index(drop=True)

    df_cities.to_csv(
        pathlib.Path("data", f"{auth}_cities.csv"), index=False, encoding="utf_8_sig"
    )

    # 更新チェック
    df = pd.read_csv(f"https://imabari.github.io/musen/{auth}_cities.csv")
    update = (df_cities == df).all(axis=None)
    
    return latest, not update


if __name__ == "__main__":

    # ディレクトリ作成

    pathlib.Path("data").mkdir(parents=True, exist_ok=True)

    updated = []

    for m in musen:

        api["IT"] = m["value"]

        parm = urllib.parse.urlencode(api, encoding="shift-jis")

        latest, update = fetch_api(parm, m["auth"])

        updated.append([m["auth"], latest, int(update)])

        time.sleep(3)

    df = pd.DataFrame(updated, columns=["area", "latest", "update"])

    df.to_csv(
        pathlib.Path("data", "updated.csv"),
        index=False,
        encoding="utf_8_sig",
    )
