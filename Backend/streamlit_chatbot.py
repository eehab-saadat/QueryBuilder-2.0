import streamlit as st
from chatbot import (
    load_dotenv, set_tracing_export_api_key, 
    normal_sql_agent, Runner, trace,
)
from openai.types.responses import ResponseTextDeltaEvent
import asyncio
import os


load_dotenv()
set_tracing_export_api_key(os.getenv("OPENAI_API_KEY"))


async def process_query(conversation):
    with trace(workflow_name="Chatbot 2.0", group_id='3'):
        # Use streaming instead of regular run
        result = Runner.run_streamed(normal_sql_agent, conversation)
        
        # Create a placeholder for streaming output
        message_placeholder = st.empty()
        full_response = ""


        async for event in result.stream_events():
            if event.type == "raw_response_event" and isinstance(event.data, ResponseTextDeltaEvent):
                full_response += event.data.delta
                message_placeholder.markdown(full_response + "▌")
        
        # Display final response without cursor
        message_placeholder.markdown(full_response)
        
        # Get final result for conversation history
        return result.to_input_list()


st.set_page_config(page_title="Medical SQL Assistant 2.0 🏥", page_icon="🏥")
st.title("Medical SQL Assistant 2.0 🏥")


if "conversation_history" not in st.session_state:
    st.session_state.conversation_history = []
if "messages" not in st.session_state:
    st.session_state.messages = []


for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

user_query = st.chat_input("Ask a question about patient data...")

if user_query:
    st.session_state.messages.append({"role": "user", "content": user_query})
    with st.chat_message("user"):
        st.markdown(user_query)
    with st.chat_message("assistant"):
        with st.spinner("Processing..."):
            if not st.session_state.conversation_history:
                response = asyncio.run(process_query(user_query))
            else:
                new_input = st.session_state.conversation_history + [{"role": "user", "content": user_query}]
                response = asyncio.run(process_query(new_input))
            
            st.session_state.conversation_history = response
            final_response = response[-1]["content"][0]['text'] if response else "Failed to process query"
    
    st.session_state.messages.append({"role": "assistant", "content": final_response})

with st.sidebar:
    st.header("About")
    st.info("Medical SQL Assistant 2.0 - Ask questions about patient data")
    if st.button("Clear Conversation"):
        st.session_state.conversation_history = []
        st.session_state.messages = []
        st.rerun()

