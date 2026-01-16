from langchain_openai import ChatOpenAI
from langchain_google_genai import ChatGoogleGenerativeAI

def get_openai_lm(json=False) -> ChatOpenAI:
    if json:
        return ChatOpenAI(
            base_url="https://api.holdai.top/v1",
            api_key="sk-YgRVqOzP21Pqj6Y2AIifEdy05Q4OWAZDEG01bGxboSw5XHsu",
            model = "gpt-5-chat-latest",
            
            # base_url="https://api.siliconflow.cn/v1",
            # api_key="sk-dedrgjziscpyozelgtpxqmzjexaivzznqbktvpucqnqnkueg",
            # model="deepseek-ai/DeepSeek-V3.1",
            
            seed=42,
            temperature=0, 
            model_kwargs={"response_format": {"type": "json_object"}}
        )
    else:
        return ChatOpenAI(
            base_url="https://api.holdai.top/v1",
            api_key="sk-YgRVqOzP21Pqj6Y2AIifEdy05Q4OWAZDEG01bGxboSw5XHsu",
            model = "gpt-5-chat-latest",
            
            # base_url="https://api.siliconflow.cn/v1",
            # api_key="sk-dedrgjziscpyozelgtpxqmzjexaivzznqbktvpucqnqnkueg",
            # model="deepseek-ai/DeepSeek-V3.1",
            
            seed=42,
            temperature=0
        )

def get_gemini_lm(json=False) -> ChatGoogleGenerativeAI:
    return ChatGoogleGenerativeAI(
        model="gemini-2.0-flash",
        google_api_key="AIzaSyCSV7JvuKrWmdZkzEkfXErDESsdEzY3w0g",
        
        seed = 42,
        temperature=0
    )
