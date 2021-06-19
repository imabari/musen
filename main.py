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
    {"auth": "01_hokkaido", "value": "J", "prefs": [10006]},
    {
        "auth": "02_tohoku",
        "value": "I",
        "prefs": [20001, 30007, 40002, 50008, 60003, 70009],
    },
    {
        "auth": "03_kanto",
        "value": "A",
        "prefs": [
            80004,
            90000,
            100005,
            110001,
            120006,
            130001,
            140007,
            190004,
        ],
    },
    {"auth": "04_shinetsu", "value": "B", "prefs": [150002, 200000]},
    {"auth": "05_hokuriku", "value": "D", "prefs": [160008, 170003, 180009]},
    {
        "auth": "06_tokai",
        "value": "C",
        "prefs": [210005, 220001, 230006, 240001],
    },
    {
        "auth": "07_kinki",
        "value": "E",
        "prefs": [250007, 260002, 270008, 280003, 290009, 300004],
    },
    {
        "auth": "08_chugoku",
        "value": "F",
        "prefs": [310000, 320005, 330001, 340006, 350001],
    },
    {
        "auth": "09_shikoku",
        "value": "G",
        "prefs": [360007, 370002, 380008, 390003],
    },
    {
        "auth": "10_kyushu",
        "value": "H",
        "prefs": [400009, 410004, 420000, 430005, 440001, 450006, 460001],
    },
    {"auth": "11_okinawa", "value": "O", "prefs": [470007]},
]

df_code = pd.read_csv(
    "https://docs.google.com/spreadsheets/d/e/2PACX-1vSseDxB5f3nS-YQ1NOkuFKZ7rTNfPLHqTKaSag-qaK25EWLcSL0klbFBZm1b6JDKGtHTk6iMUxsXpxt/pub?gid=284869672&single=true&output=csv",
    dtype={"団体コード": int, "都道府県名": str, "郡名": str, "市区町村名": str},
)

df_code["市区町村名"] = df_code["郡名"].fillna("") + df_code["市区町村名"].fillna("")
df_code.drop("郡名", axis=1, inplace=True)

df_code


def fetch_file(url, dir="."):

    p = pathlib.Path(dir, pathlib.PurePath(url).name)
    p.parent.mkdir(parents=True, exist_ok=True)

    r = requests.get(url)
    r.raise_for_status()

    with p.open(mode="wb") as fw:
        fw.write(r.content)
    return p


def fetch_api(parm, auth, prefs):

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

    df3.to_csv(pathlib.Path("data", f"{auth}.csv"), index=False, encoding="utf_8_sig")

    # 更新チェック
    p_csv = fetch_file(f"https://imabari.github.io/musen/{auth}.csv", "data/before")
    df = pd.read_csv(p_csv).fillna("")

    df = df.astype(df3.dtypes)
    update = df3.equals(df)

    # 更新がない場合はbeforeに戻す

    if update:

        p_csv = fetch_file(
            f"https://imabari.github.io/musen/before/{auth}.csv", "data/before"
        )
        df = pd.read_csv(p_csv).fillna("")

    df4 = pd.merge(
        df, df3, on=["団体コード", "都道府県名", "市区町村名"], suffixes=["_前回", "_今回"], how="right"
    )

    df4["開設局数_今回"] = df4["開設局数_今回"].fillna(0).astype(int)
    df4["開設局数_前回"] = df4["開設局数_前回"].fillna(0).astype(int)

    df4["開設局数_差分"] = df4["開設局数_今回"] - df4["開設局数_前回"]

    df4.to_csv(
        pathlib.Path("data", f"{auth}_diff.csv"), index=False, encoding="utf_8_sig"
    )

    df5 = df4[df4["団体コード"].isin(prefs)]

    return latest, not update, df5

if __name__ == "__main__":

    # ディレクトリ作成

    pathlib.Path("data").mkdir(parents=True, exist_ok=True)

    dfs = []
    updated = []

    for m in musen:

        api["IT"] = m["value"]

        parm = urllib.parse.urlencode(api, encoding="shift-jis")

        latest, update, df = fetch_api(parm, m["auth"], m["prefs"])

        dfs.append(df)

        updated.append([m["auth"], latest, int(update)])

        time.sleep(3)

    df1 = pd.concat(dfs).sort_values("団体コード").reset_index(drop=True)

    df1.to_csv(
        pathlib.Path("data", "prefs.csv"),
        index=False,
        encoding="utf_8_sig",
    )

    df2 = pd.DataFrame(updated, columns=["area", "latest", "update"])

    df2.to_csv(
        pathlib.Path("data", "updated.csv"),
        index=False,
        encoding="utf_8_sig",
    )
