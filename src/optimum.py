import streamlit as st
import time
import numpy as np
import math
import pandas as pd
from streamlit_echarts import st_echarts
from shared import *
from datetime import datetime as dt
import warnings
warnings.filterwarnings("ignore")
from os.path import dirname


def gen_max_slices_curve_data(df, VMem, VCores):
   logger.info("Generating Max Slices curve for given machine")
   df = df[df.Cores_x==VCores][df.Memory==VMem]
   df = df.loc[df.groupby(["MaxSerialSlices"])["node_count"].idxmin()]
   df = df.loc[df.groupby(["node_count"])["MaxSerialSlices"].idxmin()]
   return df

def get_runtime_vs_cost_line_chart_data(full_df):
   logger.info("Generating Runtime vs Cost data")
   start = dt.now()
   
   #TODO Get correct Prices
   full_df["total_cost"] = full_df.node_count * full_df.Memory

   full_df.dropna(inplace=True)
   full_df_optimum = full_df.loc[full_df.groupby(["MaxSerialSlices"])["total_cost"].idxmin()]
   full_df_optimum = full_df_optimum.loc[full_df_optimum.groupby(["total_cost"])["MaxSerialSlices"].idxmin()]

   #TODO Change this to get VM name correectly
   full_df_optimum["name"] = "E" + full_df_optimum.Cores_x.astype("str")

   full_df_optimum["tooltip"] = full_df_optimum["node_count"].astype("str") + " * " +  full_df_optimum["name"] + " --> " +full_df_optimum["MaxSerialSlices"].astype("str") +"x Runtime" 
   end = dt.now()
   runtime = (end-start).total_seconds()
   logger.info(f"Generating Runtime vs Cost data took {runtime} s")
   return full_df_optimum


def optimize(cluster_perc, MemRequired, NormalizedDataLoad, slices, presliced_flag, VMem, VCores, Nodes , CurrentInfraCalc):
    
    start = dt.now()
    path = os.path.join(dirname(dirname(__file__)), "data", "factors.parquet") 
    df=pd.read_parquet(path)
    df = df.astype({"Cores_x":"int64",	"Memory":"int64",	"node_count":"int64",	"Exec":"int64",	"Cores_y":"int64"})
    if CurrentInfraCalc:
        df = df[df.Cores_x==VCores][df.Memory==VMem][df.node_count==Nodes]
    
    df["TotalCoresUsed"] = (df.Exec * df.Cores_y)

    #Limit possibilities -??????
    df=df[df.TotalCoresUsed <= 3 * slices]

    df["available_memory_per_node"] = df.Memory * cluster_perc
    df["available_memory"] = df.node_count * df.available_memory_per_node
    df["available_cores_per_node"] = np.floor(df.Cores_x * cluster_perc).astype("int64")
    df["available_cores"] = (df.node_count * df.available_cores_per_node)

    #Constraints 1 and 2
    mask1 = df["TotalCoresUsed"] <= df["available_cores"]
    mask2 = df["Cores_y"] <= df["available_cores_per_node"]
    df = df[mask1][mask2]

    df["TotalMemUsed"] = df.TotalCoresUsed * (MemRequired + NormalizedDataLoad) + df.Exec
    df["MaxExecPerNode"] = np.ceil(df.Exec/df.node_count).astype("int64")
    df["MaxSerialSlices"]=np.ceil(slices/df.TotalCoresUsed).astype("int64")
    df["WorkerMemory"] = round(df.Cores_y * MemRequired, 0)
    df["MemoryOverhead"] = df.WorkerMemory + 1
    df["ExecutorMemory"] = 2 if presliced_flag else round(df.Cores_y * NormalizedDataLoad, 0) 
    df["TotalCoresUsed%"] = (df["TotalCoresUsed"] / df.available_cores) * 100
    df["TotalMemUsed"] = (df["MemoryOverhead"] + df["ExecutorMemory"]) * df.Exec

    #CONSTRAINT 3 and 4
    mask3 = df.TotalMemUsed < df.available_memory
    mask4 = df.MaxExecPerNode * (df.ExecutorMemory + df.MemoryOverhead) < df.available_memory_per_node
    df = df[mask3][mask4]

    df["TotalMemUsed%"] = (df["TotalMemUsed"] / df.available_memory) * 100

    end = dt.now()
    runtime = (end-start).total_seconds()
    print(f"{runtime} seconds")

    if (not CurrentInfraCalc):
        chart_data1 = gen_max_slices_curve_data(df, VMem, VCores)
        chart_data2 = get_runtime_vs_cost_line_chart_data(df)
        return chart_data1, chart_data2
    
    else: 
        df = df.sort_values(["MaxSerialSlices", "Exec", "TotalMemUsed" ], \
                              ascending=[True, True, True])\
                                    .reset_index(drop=True)
        return df


