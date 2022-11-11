import streamlit as st
from streamlit_echarts import st_echarts
import os
from os.path import dirname
from src.PyResourceOptimizer import shared
from src.PyResourceOptimizer import utilities


def display_results_current_infra(df):
    # st.write(df)
    def format_current_infra_results(df, i):
        ExecutorCores = int(df["Cores_y"])
        NumExecutors = int(df["Exec"])
        WorkerMemory = int(df["WorkerMemory"])
        MemoryOverhead = int(df["MemoryOverhead"])
        ExecutorMemory = int(df["ExecutorMemory"])
        SliceBucket = int(shared.inputs["slices"])+1

        def format_arguments():
            cores = f"""(ExecutorCores, {ExecutorCores})"""
            exec = f"""(NumExecutors, {NumExecutors})"""
            exec_mem = f"""(ExecutorMemory, "{ExecutorMemory}G")"""
            driver_mem = f"""(DriverMemory, "5G")"""
            driver_cores = f"""(DriverCores, 1)"""
            spark_profile = f"""("SparkProfileConfig", "spark_profile_{i}")"""
            slice_bucket = f"""([Param.use_slice_bucket_count], True)"""
            args = f"""{exec}, {cores}, {exec_mem},\n{driver_mem},  {driver_cores}, \n{spark_profile}, {slice_bucket}"""
            return args

        def format_SparkConfig():
            worker_mem = f'''"spark.python.worker.memory": "{WorkerMemory}g"'''
            mem_overhead = f""""spark.executor.memoryOverhead": "{MemoryOverhead}g" """
            profile = (
                f""""spark_profile_{i}" :"""
                + "{"
                + f""" {worker_mem},
                     {mem_overhead},
                     "spark.python.profile": true,
                     "spark.python.worker.reuse": false,
                     "spark.executorEnv.MKL_NUM_THREADS": 2, "spark.executorEnv.NUMEXPR_NUM_THREADS": 2, "spark.executorEnv.OMP_NUM_THREADS": 2  """
                + "}"
            )
            return profile

        return format_arguments(), format_SparkConfig()

    def display_formatted_expander(i):
        colA, colB, colC, colD, colE = st.columns([1.5, 2, 2, 2, 2])
        preselect_value = True if i == 0 else False
        # Best = "Best" if i==0 else ""
        Best = ""
        with colA:
            st.metric(
                "Most Slices in Series",
                df.MaxSerialSlices.iloc[i],
                delta=Best,
                help="""Lower this number, Higher the Parallelism and Lower the Runtime.  
         Ideal number is 1.""",
            )
        with colB:
            st.metric(
                "Usage of Total Available Memory",
                str(round(df["TotalMemUsed"].iloc[i], 1)) + " GB",
                delta=Best,
                help=f"""This number represents how much memory the plugin will use up  
         out of the Total Available Memory of {round(df.available_memory.iloc[i], 1)} GB""",
            )
        with colC:
            st.metric(
                "Usage % of Total Available Memory",
                str(round(df["TotalMemUsed%"].iloc[i], 1)) + " %",
                delta=Best,
                help="""The Lower - The Better.  
         It is fine if this number hits 100% since there are margins taken into account.""",
            )
        with colD:
            st.metric(
                "Usage of Total Available Cores",
                df["TotalCoresUsed"].iloc[i],
                help=f"""This number represents how many cores the plugin will use up  
         out of the Total Available Cores = {df.available_cores.iloc[i]}""",
                delta=Best,
            )
        with colE:
            st.metric(
                "Usage % of Total Available Cores",
                str(round(df["TotalCoresUsed%"].iloc[i], 1)) + " %",
                delta=Best,
                help="""The Lower - The Better.  
         It is fine if this number hits 100% since there are margins taken into account.""",
            )

    st.success("""VERY VERY BEST🚀🚀🚀🚀""")
    i = 0
    display_formatted_expander(i)

    with st.expander("Arguments and Profile for Optimal Solution", expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            args, profile = format_current_infra_results(df.iloc[i], i)
            st.markdown(
                """Add to the *using arguments*  
         part in plugin command:"""
            )
            st.code(args)
        with col2:
            st.markdown(
                """Add to *SparkProfileConfig*  
         in *TenantSystemSettings* and *DefaultSystemSettings*:"""
            )
            st.code(profile)
    rows = 3
    st.warning("Almost Best🚀🚀")
    try:
        for i in range(1, rows + 1):
            display_formatted_expander(i)

            with st.expander(f"Sub Optimal Option {i}🚀🚀", expanded=False):
                col1, col2 = st.columns(2)
                with col1:
                    args, profile = format_current_infra_results(df.iloc[i], i)
                    st.markdown(
                        """Add to the *using arguments*  
               part in plugin command:"""
                    )
                    st.code(args)
                with col2:
                    st.markdown(
                        """Add to *SparkProfileConfig*  
               in *TenantSystemSettings* and *DefaultSystemSettings*:"""
                    )
                    st.code(profile)
    except Exception as e:
        st.warn("Not Enough Options to Display")
        # st.error(f"Error due to {e}")


def display_runtime_vs_node_line_chart(chart_data):
    path = os.path.join(
        dirname(dirname(dirname(__file__))),
        "config",
        "runtime_vs_node_line_chart_options.json",
    )
    options = utilities.read_json(path)
    options["series"][0]["data"] = chart_data[
        ["node_count", "MaxSerialSlices"]
    ].values.tolist()
    with st.expander(
        options["title"]["expander_text"].format(VName=shared.inputs["VName"])
    ):

        st.metric(
            label="How Do I Read this Chart?",
            value="Runtime vs Number of Nodes",
            help="Viverra justo nec ultrices dui sapien eget. Ultricies mi quis hendrerit dolor magna eget. Fermentum odio eu feugiat pretium nibh. Bibendum arcu vitae elementum curabitur vitae. Vulputate sapien nec sagittis aliquam malesuada bibendum. Lectus quam id leo in vitae turpis. Proin sed libero enim sed faucibus turpis in. Vitae ultricies leo integer malesuada nunc vel risus. Eget arcu dictum varius duis at consectetur lorem donec massa. Facilisis sed odio morbi quis commodo odio aenean. Fringilla ut morbi tincidunt augue interdum velit euismod in pellentesque. Quis eleifend quam adipiscing vitae proin. Risus pretium quam vulputate dignissim suspendisse in est ante in. Dignissim diam quis enim lobortis scelerisque fermentum dui. Consectetur lorem donec massa sapien faucibus et molestie. Eget arcu dictum varius duis at consectetur lorem donec massa. Sapien pellentesque habitant morbi tristique senectus et. www.google.com ",
        )
        st_echarts(options=options, height="610px")


def display_runtime_vs_cost_line_chart(chart_data):
    full_df_optimum = chart_data.copy()
    path = os.path.join(
        dirname(dirname(dirname(__file__))),
        "config",
        "Runtime_vs_Cost_line_chart_options.json",
    )
    options = utilities.read_json(path)
    options["series"][0]["data"] = full_df_optimum[
        ["total_cost", "MaxSerialSlices"]
    ].values.tolist()
    options["series"][1]["data"] = full_df_optimum[
        ["total_cost", "tooltip"]
    ].values.tolist()
    with st.expander("Show Me All Machines, Life's Too Short for Commitments 🏔️"):
        st.metric(
            label="How Do I Read this Chart?",
            value="Runtime vs Cost",
            help="Viverra justo nec ultrices dui sapien eget. Ultricies mi quis hendrerit dolor magna eget. Fermentum odio eu feugiat pretium nibh. Bibendum arcu vitae elementum curabitur vitae. Vulputate sapien nec sagittis aliquam malesuada bibendum. Lectus quam id leo in vitae turpis. Proin sed libero enim sed faucibus turpis in. Vitae ultricies leo integer malesuada nunc vel risus. Eget arcu dictum varius duis at consectetur lorem donec massa. Facilisis sed odio morbi quis commodo odio aenean. Fringilla ut morbi tincidunt augue interdum velit euismod in pellentesque. Quis eleifend quam adipiscing vitae proin. Risus pretium quam vulputate dignissim suspendisse in est ante in. Dignissim diam quis enim lobortis scelerisque fermentum dui. Consectetur lorem donec massa sapien faucibus et molestie. Eget arcu dictum varius duis at consectetur lorem donec massa. Sapien pellentesque habitant morbi tristique senectus et. www.google.com ",
        )
        st_echarts(options=options, height="610px")

