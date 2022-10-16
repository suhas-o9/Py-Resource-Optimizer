
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
from os.path import dirname

def main():
   
   with st.sidebar:
      shared.debug_mode = st.checkbox("Debug Mode")
   st.title("Welcome")
   st.write("check out this [link](./Inputs_and_Results_Summary)")

main()