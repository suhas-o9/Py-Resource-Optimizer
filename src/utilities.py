import streamlit as st
import time
import numpy as np
import math
import pandas as pd
from streamlit_echarts import st_echarts
import json
from os.path import dirname
import shared
import os



def read_json(json_file_path):
    """
    read JSON file in path and return python dictionary
    """
    with open(json_file_path, 'r') as j:
      text = json.loads(j.read())
    return text

def set_frame():
   st.set_page_config(layout="wide", page_title="Saving Money Max",page_icon="fire")

   # container1 = st.container()
   # return container1
   
def get_inputs(credits):
   logger = shared.logger
   
   colA, colB, colC, colD, colE = st.columns([3, 1,3,1,3]) 

   with colA:
      nodes=float(st.slider("Enter the number of Nodes", 1, 200, 3))
      path = os.path.join(dirname(dirname(__file__)), "data", "VM_list.csv") 
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
   with colC:
      Input_Cols=float(st.slider("Biggest input size(columns)", 1, 50, 20))
      Input_Rows=st.number_input("Biggest input size(rows in million)", 0,3000)
      slices=st.number_input("Enter the number of Slices", 1, 1000000)
      
   with colE:
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
   
   
   
   st.markdown("")
   st.markdown("")
   st.markdown("")
   st.markdown("")
   st.markdown("")
   st.markdown("")

   Run = st.button("Calculate!", help="Help Text to be Added")
   RunInfra = st.button("Calculate Ideal Infra!" , help="Help Text to be Added") 

   
       


      

   keys_list = ["nodes", "VMem", "VCores", "slices", "Input_Rows", "Input_Cols", "DataLoadMultiplier", "cluster_perc", "override_mem", "override_mem_flag", "VPrice", "presliced_flag", "VName", "Run", "RunInfra"]
   values_list = [nodes, VMem, VCores, slices, Input_Rows, Input_Cols, DataLoadMultiplier, cluster_perc, override_mem, override_mem_flag, VPrice, presliced_flag, VName, Run, RunInfra]
   
   shared.inputs = dict(zip(keys_list, values_list))

   logger.info(f"Getting Inputs: {shared.inputs}")

   return nodes, VMem, VCores, slices, Input_Rows, Input_Cols, DataLoadMultiplier, cluster_perc, override_mem, override_mem_flag, VM_List, VPrice, presliced_flag, VName, Run, RunInfra





