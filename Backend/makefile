ui:
	uv run streamlit run streamlit_app.py
main:
	uv run main.py
chat:
	uv run chatbot.py
ui2:
	uv run streamlit run streamlit_chatbot.py
install:
	uv sync

help:
	@echo "Available commands:"
	@echo "  make ui        - Run the multiagent Streamlit app (streamlit_app.py)"
	@echo "  make ui2       - Run the singleagent Streamlit app (streamlit_chatbot.py)"
	@echo "  make main      - Run the multiagent script"
	@echo "  make chat      - Run the singleagent script"
	@echo "  make install   - Create virtual environment and install dependencies using uv"
	@echo "  make help      - Display this help message"
