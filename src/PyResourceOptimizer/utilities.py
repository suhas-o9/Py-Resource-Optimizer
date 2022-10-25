import streamlit as st
import numpy as np
import pandas as pd
import json
from os.path import dirname

import os
import threading
from streamlit.runtime.scriptrunner import add_script_run_ctx

from PyResourceOptimizer import shared
from PyResourceOptimizer import display
from PyResourceOptimizer import optimum

logger=shared.logger

def read_json(json_file_path):
    """
    read JSON file in path and return python dictionary
    """
    with open(json_file_path, 'r') as j:
      text = json.loads(j.read())
    return text

def set_frame():
   st.set_page_config(layout="wide", page_title="Ex-stream-ly Cool App",page_icon="fire")

   
def get_inputs():
   # logger = shared.logger
   
   Cloud=st.radio("Cloud Provider", ["AWS", "Azure", "GCP"], horizontal=True, index=1, help="Find out from Ops which Cloud your Tenant/Hadoop Cluster is hosted on")

   nodes=float(st.slider("Enter the number of Nodes", 1, 200, 3))
   path = os.path.join(dirname(dirname(dirname(__file__))), "data", f"VM_{Cloud}.csv")
   VM_List = pd.read_csv(path)
   VM_List["DisplayName"]=VM_List["name"].astype(str) + " - " + VM_List["Memory"].astype(str) + "G - " + VM_List["Cores"].astype(str) + "Cores"
   def format_func1(key):
      name = f'''{VM_List[VM_List["key"]==key]["name"].iloc[0]} - {VM_List[VM_List["key"]==key]["Memory"].iloc[0]}G - {VM_List[VM_List["key"]==key]["Cores"].iloc[0]} Cores'''
      # st.write(name)
      return name
   VM_key=st.selectbox("Choose the Machine Class", list(VM_List.key), format_func=format_func1)
   VMem= VM_List[VM_List["key"]==VM_key]["Memory"].iloc[0]
   VCores= VM_List[VM_List["key"]==VM_key]["Cores"].iloc[0]
   VPrice = VM_List[VM_List["key"]==VM_key]["Price"].iloc[0]
   VName = VM_List[VM_List["key"]==VM_key]["name"].iloc[0]
   cluster_perc=float(st.slider("Enter the percentage of cluster available", 0, 80, 80))/100
   slices=st.number_input("Enter the number of Slices", 1, 1000000)
   Input_Rows=st.number_input("Biggest input size(rows in million)", 0,3000)
   Input_Cols=float(st.slider("Biggest input size(columns)", 1, 50, 20))
   presliced_flag=st.checkbox("Data Pre-Sliced?", help="Big Data tables are already partitioned, for example on HDFS")
   override_mem_flag=st.checkbox("Override Memory Requirements?")
   override_mem=np.nan
   if override_mem_flag:
      override_mem = st.number_input("Memory Requirements Override (GB)", 0,3000)
   DataLoadMultiplierdict={
      1:{"name": "Data/Other", "multiplier":2}, 
      2:{"name": "ML", "multiplier":3}}
   def format_func2(type):
      return DataLoadMultiplierdict[type]["name"]  
   Purpose_key=st.radio("Purpose of Plugin", list(DataLoadMultiplierdict.keys()), horizontal=True, index=1, format_func=format_func2, help="The type of plugin impacts how much compute is needed")
   DataLoadMultiplier=DataLoadMultiplierdict[Purpose_key]["multiplier"]
   
   col1, col2 = st.columns(2)
   with col1: 
      Run = st.button("Calculate!", help="Help Text to be Added")
   with col2: 
      RunInfra = st.button("Calculate Ideal Infra!" , help="Help Text to be Added") 

   # shared.debug_mode = st.checkbox("Debug Mode")
       
   keys_list = ["nodes", "VMem", "VCores", "slices", "Input_Rows", "Input_Cols", "DataLoadMultiplier", "cluster_perc", "override_mem", "override_mem_flag", "VPrice", "presliced_flag", "VName", "Run", "RunInfra", "Cloud"]
   values_list = [nodes, VMem, VCores, slices, Input_Rows, Input_Cols, DataLoadMultiplier, cluster_perc, override_mem, override_mem_flag, VPrice, presliced_flag, VName, Run, RunInfra, Cloud]
   
   shared.inputs = dict(zip(keys_list, values_list))

   logger.info(f"Getting Inputs: {shared.inputs}")

   return


def ideal_infra():
   if shared.inputs["RunInfra"]:
      cont1 = st.container()
      cont2 = st.container()                
      def ideal_infra_VM():
                     
         CalcMode = 2
         df1 = optimum.optimize(CalcMode)
         df = df1.loc[df1.groupby(["node_count"])["MaxSerialSlices"].idxmin()]
         chart_data1 = df.loc[df.groupby(["MaxSerialSlices"])["node_count"].idxmin()]
         # =gen_max_slices_curve_data(df1, VCores, VMem)
         display.display_runtime_vs_node_line_chart(chart_data1)
   
         
      def ideal_infra_All():
               
         CalcMode = 3
         df1 = optimum.optimize(CalcMode)
         chart_data2 = optimum.get_runtime_vs_cost_line_chart_data(df1)
         display.display_runtime_vs_cost_line_chart(chart_data2)
                     
      t1 = threading.Thread(target=ideal_infra_VM, name='t1')
      t2 = threading.Thread(target=ideal_infra_All, name='t2') 
      add_script_run_ctx(t1)
      add_script_run_ctx(t2)
      # starting threads
      
      t1.start()
      t2.start()

      # wait until all threads finish
      with st.spinner("Optimizing for Your VM"): 
         t1.join()
      with st.spinner("Optimizing for All VMs"):
         t2.join()

def current_infra():
   # print(shared.inputs)
   if (shared.inputs["Run"] | shared.inputs["RunInfra"]): 
         
      try:
         CalcMode = 1
         df1 = optimum.optimize(CalcMode)
         # st.info(df1.dtypes)
         df2=df1.sort_values(["MaxSerialSlices", "TotalCoresUsed", "Exec"], \
                        ascending=[True, True, False])\
                              .reset_index(drop=True)
         # st.write(df2)                     
         # df3 = df2.loc[df2.groupby(["MaxSerialSlices", "Exec"])["Cores_y"].idxmin()].reset_index(drop=True)                  
         display.display_results_current_infra(df2)

      except Exception as e: 
         st.error("The Infra selected is insufficient for the given Data size. Please change the settings and try again!")
         st.write(e)





