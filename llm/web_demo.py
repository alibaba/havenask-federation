import gradio as gr
import requests

def chat_with_java_backend(question):
    url = "http://localhost:8080/ask"
    response = requests.post(url, json={"question": question})
    if response.status_code == 200:
        return response.text
    else:
        raise Exception(f"Request failed with status code: {response.status_code}")

def reset_user_input():
    return gr.update(value='')

def reset_state():
    return [], []

with gr.Blocks() as demo:
    """copy from https://github.com/alibaba/havenask/blob/main/llm/web_demo.py"""
    gr.HTML("""<h1 align="center">Havenask-federation LLM Web Demo!</h1>""")
    chatbot = gr.Chatbot()
    with gr.Row():
        with gr.Column(scale=4):
            with gr.Column(scale=12):
                user_input = gr.Textbox(show_label=False, placeholder="Input...", lines=10)#.style(container=False)
            with gr.Column(min_width=32, scale=1):
                submit_btn = gr.Button("Submit", variant="primary")
        with gr.Column(scale=1):
            empty_btn = gr.Button("Clear History")

    history = gr.State([])

    def add_message(question, history):
        answer = chat_with_java_backend(question)
        history.append((question, answer))
        return history, '', history

    submit_btn.click(add_message, inputs=[user_input, history], outputs=[chatbot, user_input, history])

    empty_btn.click(reset_state, outputs=[chatbot, history], show_progress=True)

demo.launch(share=False, server_name='0.0.0.0', server_port=7860, inbrowser=True)