import numpy as np
import pandas as pd
from datetime import datetime as dt
import warnings
warnings.filterwarnings("ignore")
from os.path import dirname
# from PyResourceOptimizer import *
# from .shared import inputs
import os
from PyResourceOptimizer import shared


logger=shared.logger

def optimize(CalcMode):
    """
    CalcMode:   
    1: Current VM, Current Node Count
    2: Current VM, Any Node Count
    3. Any VM, Any Node Count
    """
    
    Nodes, VMem, VCores, slices, Input_Rows, Input_Cols, DataLoadMultiplier, cluster_perc, override_mem, override_mem_flag, VPrice, presliced_flag, VName, Run,  RunInfra, Cloud = shared.inputs.values()
    
    NormalizedDataLoad = Input_Rows * (Input_Cols/20)
    calc_mem = NormalizedDataLoad * DataLoadMultiplier
    MemRequired = override_mem if override_mem_flag else calc_mem

    start = dt.now()
    
    if (CalcMode==3):
        path = os.path.join(dirname(dirname(dirname(__file__))), "data", f"factors_{Cloud}.parquet") 
    if (CalcMode==1 or CalcMode==2):
        path = os.path.join(dirname(dirname(dirname(__file__))), "data", f"factors_{VName}.parquet")

    
    df=pd.read_parquet(path)
    df = df.astype({"Cores_x":"int64",	"Memory":"int64",	"node_count":"int64",	"Exec":"int64",	"Cores_y":"int64"})
    if CalcMode==1:
        df = df[df.node_count==Nodes]
    
    df["TotalCoresUsed"] = (df.Exec * df.Cores_y)

    #Limit possibilities -??????
    df=df[df.TotalCoresUsed <= 3 * slices]
    #20% is reserved for other hadoop services
    df["available_memory_per_node"] = np.max((df.Memory*0.8) * cluster_perc, 0)
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
    df["WorkerMemory"] = df.WorkerMemory.replace(0,1)
    df["ExecutorMemory"] = 2 if presliced_flag else np.round(df.Cores_y * NormalizedDataLoad, 0)
    df["ExecutorMemory"] = df.ExecutorMemory.replace(0,1)
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
    return df


def gen_max_slices_curve_data(df, VCores, VMem):
    logger.info("Generating Max Slices curve for given machine")
    # df_nodes = pd.DataFrame(range(1,200), columns=["node_count"])
    # df_nodes["max_slices"] = df_nodes.node_count.apply(lambda x : get_optimum_settings(factors, x, VMem, VCores, slices, Input_Rows, Input_Cols, DataLoadMultiplier, cluster_perc, override_mem, override_mem_flag, ideal_mode=ideal_mode, presliced_flag=presliced_flag))
    # df = df[df.Cores_x==VCores][df.Memory==VMem]
    df = df.loc[df.groupby(["node_count"])["MaxSerialSlices"].idxmin()]
    df = df.loc[df.groupby(["MaxSerialSlices"])["node_count"].idxmin()]
    return df

def get_runtime_vs_cost_line_chart_data(full_df):
   logger.info("Generating Runtime vs Cost data")
   start = dt.now()
   
   full_df["total_cost"] = full_df.node_count * full_df.Memory
   full_df.dropna(inplace=True)
   full_df_optimum = full_df.loc[full_df.groupby(["MaxSerialSlices"])["total_cost"].idxmin()]
   full_df_optimum = full_df_optimum.loc[full_df_optimum.groupby(["total_cost"])["MaxSerialSlices"].idxmin()]

   #TODO Change this to get VM name correectly
   full_df_optimum["name"] = "E" + full_df_optimum.Cores_x.astype("str")

   full_df_optimum["tooltip"] = full_df_optimum["node_count"].astype("str") + " * " +  full_df_optimum["name"] + " --> " +full_df_optimum["MaxSerialSlices"].astype("str") +"x Runtime" 
   # full_df_optimum["parallelism"]=1/full_df_optimum["max_slices"]
   # full_df_optimum=full_df_optimum.sort_values(["parallelism"], ascending=True)
   end = dt.now()
   runtime = (end-start).total_seconds()
   logger.info(f"Generating Runtime vs Cost data took {runtime} s")
   return full_df_optimum