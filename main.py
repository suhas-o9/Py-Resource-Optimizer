
import streamlit as st
from streamlit_echarts import st_echarts
import time
import numpy as np
import math
import pandas as pd
from optimum import *
from utilities import *
from display import *
import json
from shared import logger
from datetime import datetime as dt


def main():
   
   set_frame()
   
   text = read_json("./config/text.json")
    
   
   
   # with container1:
       
   st.title(text["title"]["text"])
   with st.sidebar:
      Nodes, VMem, VCores, slices, Input_Rows, Input_Cols, DataLoadMultiplier, cluster_perc, override_mem, override_mem_flag, VM_List, VPrice, presliced_flag, VName, Run,  RunInfra = get_inputs(credits)

   NormalizedDataLoad = Input_Rows * (Input_Cols/20)
   calc_mem = NormalizedDataLoad * DataLoadMultiplier
   MemRequired = override_mem if override_mem_flag else calc_mem

   tab1, tab2 = st.tabs([text["tabs"]["tab1"]["title"], text["tabs"]["tab2"]["title"]])
   
   with tab1:
      
      def current_infra():
         if (Run | RunInfra): 
             
            try:
               CurrentInfraCalc = True
               df1 = optimize(cluster_perc, MemRequired, NormalizedDataLoad, slices, presliced_flag, VMem, VCores, Nodes, CurrentInfraCalc)\
                        .sort_values(["MaxSerialSlices", "Exec", "TotalMemUsed" ], \
                              ascending=[True, True, True])\
                                    .reset_index(drop=True)

         
               display_results_current_infra(df1)

            except Exception as e: 
               st.error("The Infra selected is insufficient for the given Data size. Please change the settings and try again!")
               st.write(e)

      current_infra()


   with tab2: 
      # Ideal Infra Results Here
      
      def ideal_infra():
         if RunInfra:
            
            with st.spinner("Crunching Some Numbers"):
               CurrentInfraCalc = False
               df1 = optimize(cluster_perc, MemRequired, NormalizedDataLoad, slices, presliced_flag, VMem, VCores, Nodes, CurrentInfraCalc)

            with st.spinner("Optimizing for your VM"):
               chart_data1=gen_max_slices_curve_data(df1)
               display_runtime_vs_node_line_chart(chart_data1, VName)
               
            with st.spinner("Optimizing Across All VMs"):
               chart_data2 = get_runtime_vs_cost_line_chart_data(df1)
               display_runtime_vs_cost_line_chart(chart_data2)

      ideal_infra()

main()