import streamlit as st
from os.path import dirname
from src.PyResourceOptimizer.utilities import (
    read_json,
    set_frame,
    get_inputs,
    ideal_infra,
    current_infra,
)
from src.PyResourceOptimizer import shared
import os
import gc
logger=shared.logger

def main():
    shared.MaxMemory=0
    set_frame()
    path = os.path.join(dirname(__file__), "config", "text.json")
    text = read_json(path)

    st.title(text["title"]["text"])
    with st.sidebar:
        get_inputs()

    tab1, tab2 = st.tabs([text["tabs"]["tab1"]["title"], text["tabs"]["tab2"]["title"]])
    with tab1:
        current_infra()
    with tab2:
        ideal_infra()
        # st.info("Coming Soon...")

    logger.info(f"Max Memory is {shared.MaxMemory} GB")

main()
