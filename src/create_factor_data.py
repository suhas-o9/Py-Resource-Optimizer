import pandas as pd
import numpy as np
import os
from os.path import dirname
import logging
import gc

def factorize(val):
    comb = [(i, val / i, val) for i in range(1, int(val ** 0.5) + 1) if val % i == 0]
    perm = comb
    for i in comb:
        perm = perm + [(i[1], i[0], i[2])]
    return perm


def gen_factors(VM, name, factor_list):

    node_range = pd.DataFrame(range(1, 201), columns=["node_count"])
    # df_temp=pd.DataFrame(row, columns=["Cores", "Memory", "name"])
    # df_temp=pd.DataFrame.from_dict(dict(VM), orient='columns')
    # print(df_temp)
    df1 = VM.merge(node_range, how="cross")
    df2 = df1.merge(factor_list, how="cross")
    del df1
    gc.collect()
    # print(df2.head())
    df2["TotalAvailableCores"] = df2.Cores_x * df2.node_count
    df2["TotalCoresUsed"] = df2.Exec * df2.Cores_y
    mask = df2.TotalCoresUsed <= df2.TotalAvailableCores
    df2 = df2[mask]
    # print(len(df2))
    df2 = df2.reset_index()[["Cores_x", "Memory", "node_count", "Exec", "Cores_y"]]
    df2 = df2.astype(
        {
            "Cores_x": "int8",
            "Memory": "int16",
            "node_count": "int16",
            "Exec": "int16",
            "Cores_y": "int16",
        }
    )
    path = os.path.join(dirname(dirname(__file__)), "data", f"factors_{name}.parquet")
    df2.to_parquet(path)
    del df2
    gc.collect()

def main():

    logging.info("Start: Create Factor Data")
    TotalCoresAvailable = 21000
    logging.info(f"Creating Factors upto {TotalCoresAvailable} Cores")
    factors = []
    for i in range(0, TotalCoresAvailable + 1):
        factors = factors + factorize(i)
    df = pd.DataFrame(factors, columns=["Exec", "Cores", "TotalCoresUsed"])
    df = df.drop_duplicates(["Exec", "Cores"])
    # df.to_csv("factors.csv")
    logging.info(f"Finish: Creating Factors")

    factor_list = df[["Exec", "Cores"]]
    del df
    path = os.path.join(dirname(dirname(__file__)), "data", "VM_Azure.csv")
    VM_list = pd.read_csv(path)[["Cores", "Memory", "name"]]

    for i in range(0, len(VM_list)):
        df = VM_list[VM_list.index == i].reset_index()
        name = df["name"][0]
        logging.info(f"Start: Creating file for {name}")
        gen_factors(df, name, factor_list)
        logging.info(f"Finish: Creating file for {name}")
    logging.info("Finish: Create Factor Data. Exiting.")


main()
