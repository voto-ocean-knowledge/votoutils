import pandas as pd
import glob

infiles = glob.glob("bad_pld/sea061.48.pld1.raw.1???.gz")
for a in infiles:
    dfa = pd.read_csv(a, sep=";", index_col=0, parse_dates=True, dayfirst=True)
    fn = a.split("/")[-1]
    b = f"{a[:-3]}48{a[-3:]}"
    try:
        dfb = pd.read_csv(b, sep=";", index_col=0, parse_dates=True, dayfirst=True)
    except FileNotFoundError:
        continue
    dfc = pd.concat((dfa, dfb), axis=0)
    df_merge = dfc.sort_index()
    df_merge.to_csv(
        f"fix_pld/{fn}",
        sep=";",
        date_format="%d/%m/%Y %H:%M:%S.%f",
        compression="gzip",
    )
