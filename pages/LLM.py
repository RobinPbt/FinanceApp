import os
import asyncio
import streamlit as st
import streamlit.components.v1 as components
import time
from functools import wraps
from PIL import Image


from langchain.chat_models import ChatOpenAI
from langchain.tools import DuckDuckGoSearchRun, WikipediaQueryRun
from langchain_community.utilities import WikipediaAPIWrapper
from langchain.prompts import PromptTemplate
from langchain.memory import ConversationBufferMemory
from langchain.chains import LLMChain

from app_functions import *

st.set_page_config(
    page_title="LLMs",
    layout = "wide"
)

# Create db connection
con = st.connection(
    "app_finance",
    type="sql",
    url="postgresql+psycopg2://airflow:airflow@localhost/airflow"
)

# -----------------------------Define caching functions ---------------------------------

@st.cache_data
def get_names():
    query = """
        SELECT DISTINCT("shortName") 
        FROM general_information;
    """

    comapny_names = con.query(query)
    return comapny_names

# -----------------------------Define sidebar -------------------------------------------

# # Button to shutdown app (in development stage)
# exit_app = st.sidebar.button("Shut Down")
# if exit_app:
#     # Give a bit of delay for user experience
#     time.sleep(5)
#     # Close streamlit browser tab
#     keyboard.press_and_release('ctrl+w')
#     # Terminate streamlit python process
#     pid = os.getpid()
#     p = psutil.Process(pid)
#     p.terminate()

# Names selection    
comapny_names = get_names()
company_selection = st.sidebar.selectbox('Company selection', comapny_names)

# -----------------------------LLM definition -------------------------------------------

# # Create a model
# chat_model = ChatOpenAI(
#     model_name="gpt-3.5-turbo", 
#     temperature=0.01, 
#     openai_api_key=os.getenv("OPENAI_API_KEY")
# )

# # setting up the script prompt templates
# script_template = PromptTemplate(
#     input_variables = ['company_name', 'wikipedia_research', 'web_search'], 
#     template='''Give me a 10 lines description of the activity of the company {company_name} 
#     including the location of its headquarter, its number of employees over the world, its underlying markets and its main competitors.
#     You will make use of the information and knowledge obtained from the Wikipedia research:{wikipedia_research}
#     and make use of the additional information from the web search:{web_search} ''',
# )

# # memory buffer
# memory = ConversationBufferMemory(
#     input_key='company_name', 
#     memory_key='chat_history')

# # LLM chain
# chain = LLMChain(
#     llm=chat_model, 
#     prompt=script_template, 
#     verbose=True, 
#     output_key='script', 
#     memory=memory)

# async def generate_script(company_name):
#     wikipedia_research = fetch_wikipedia_data(company_name)
#     web_search = fetch_web_search_results(company_name)
#     script = chain.run(
#         company_name=company_name, 
#         wikipedia_research=wikipedia_research, 
#         web_search=web_search
#     )
    
#     return script, wikipedia_research, web_search

# # This function is a wrapper around the async function 'generate_script'
# # It allows us to call the async function in a synchronous way
# # using 'asyncio.run'
# def run_generate_script(company_name):
#     """
#     Wrapper function to run the async function 'generate_script'
#     in a synchronous way
    
#     Args:
#         input_text (str): The input text passed to the language model

#     Returns:
#         tuple: A tuple containing the script, web search and wikipedia research
#     """
#     return asyncio.run(generate_script(company_name))

# def stream_data(script):
#     for word in script.split(" "):
#         yield word + " "
#         time.sleep(0.02)

# -----------------------------Dashboard ------------------------------------------------

# LLM function call
# script, wikipedia_research, web_search = run_generate_script(company_selection)

# writing the title and script
st.write("# Activity description of {}".format(company_selection))
col_1, col_2 = st.columns([1, 8])

with col_1:
    st.write("Powered by ChatGPT")
with col_2:
    image = Image.open("ChatGPT_logo.png")
    st.image(image, width=30)

# # st.write_stream(stream_data(script))
# st.write(script) 

# with st.expander('Wikipedia-based exploration: '): 
#     st.info(wikipedia_research)

# with st.expander('Web-based exploration: '):
#     st.info(web_search)