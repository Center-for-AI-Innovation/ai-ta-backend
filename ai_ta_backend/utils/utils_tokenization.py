import os
from typing import Any

import tiktoken


def count_tokens_and_cost(
    prompt: str,
    completion: str = '',
    openai_model_name: str = "gpt-3.5-turbo"):  # -> tuple[int, float] | tuple[int, float, int, float]:
  """
  # TODO: improve w/ extra tokens used by model: https://github.com/openai/openai-cookbook/blob/d00e9a48a63739f5b038797594c81c8bb494fc09/examples/How_to_count_tokens_with_tiktoken.ipynb
  Returns the number of tokens in a text string.

  Only the first parameter is required, a string of text to measure. The completion and model name are optional.

  num_tokens, prompt_cost = count_tokens_and_cost(prompt="hello there")
  num_tokens_prompt, prompt_cost, num_tokens_completion, completion_cost  = count_tokens_and_cost(prompt="hello there", completion="how are you?")  
  
  Args:
      prompt (str): _description_
      completion (str, optional): _description_. Defaults to ''.
      openai_model_name (str, optional): _description_. Defaults to "gpt-3.5-turbo".

  Returns:
      tuple[int, float] | tuple[int, float, int, float]: Returns the number of tokens consumed and the cost. The total cost you'll be billed is the sum of each individual cost (prompt_cost + completion_cost)
  """
  # encoding = tiktoken.encoding_for_model(openai_model_name)
  openai_model_name = openai_model_name.lower()
  encoding = tiktoken.encoding_for_model("gpt-3.5-turbo")  # I think they all use the same encoding
  prompt_cost = 0
  completion_cost = 0

  prompt_token_cost = 0
  completion_token_cost = 0

  if openai_model_name.startswith("gpt-3.5-turbo"):
    if "16k" in openai_model_name:
      prompt_token_cost: float = 0.003 / 1_000
      completion_token_cost: float = 0.004 / 1_000
    else:
      # 3.5-turbo regular (4k context)
      prompt_token_cost: float = 0.0015 / 1_000
      completion_token_cost: float = 0.002 / 1_000

  elif openai_model_name.startswith("gpt-4"):
    if "32k" in openai_model_name:
      prompt_token_cost = 0.06 / 1_000
      completion_token_cost = 0.12 / 1_000
    else:
      # gpt-4 regular (8k context)
      prompt_token_cost = 0.03 / 1_000
      completion_token_cost = 0.06 / 1_000
  elif openai_model_name.startswith("text-embedding-ada-002"):
    prompt_token_cost = 0.0001 / 1_000
    completion_token_cost = 0.0001 / 1_000
  else:
    # no idea of cost
    print(f"NO IDEA OF COST, pricing not supported for model model: `{openai_model_name}`")
    prompt_token_cost = 0
    completion_token_cost = 0

  if completion == '':
    num_tokens_prompt: int = len(encoding.encode(prompt))
    prompt_cost = float(prompt_token_cost * num_tokens_prompt)
    return num_tokens_prompt, prompt_cost
  elif prompt == '':
    num_tokens_completion: int = len(encoding.encode(completion))
    completion_cost = float(completion_token_cost * num_tokens_completion)
    return num_tokens_completion, completion_cost
  else:
    num_tokens_prompt: int = len(encoding.encode(prompt))
    num_tokens_completion: int = len(encoding.encode(completion))
    prompt_cost = float(prompt_token_cost * num_tokens_prompt)
    completion_cost = float(completion_token_cost * num_tokens_completion)
    return num_tokens_prompt, prompt_cost, num_tokens_completion, completion_cost


# from dotenv import load_dotenv

# load_dotenv()


def analyze_conversations(supabase_client: Any = None):
  import supabase

  if supabase_client is None:
    supabase_client = supabase.create_client(  # type: ignore
        supabase_url=os.getenv('SUPABASE_URL'),  # type: ignore
        supabase_key=os.getenv('SUPABASE_API_KEY'))  # type: ignore
  # Get all conversations
  response = supabase_client.table('llm-convo-monitor').select('convo').execute()
  # print("total entries", response.data.count)

  total_convos = 0
  total_messages = 0
  total_prompt_cost = 0
  total_completion_cost = 0

  # Iterate through all conversations
  # for convo in response['data']:
  for convo in response.data:
    total_convos += 1
    # print(convo)
    # prase json from convo
    # parse json into dict
    # print(type(convo))
    # convo = json.loads(convo)
    convo = convo['convo']
    messages = convo['messages']
    model_name = convo['model']['name']

    # Iterate through all messages in each conversation
    for message in messages:
      total_messages += 1
      role = message['role']
      content = message['content']

      # If the message is from the user, it's a prompt
      # TODO: Fix these
      # WARNING: Fix these error messages... they are the sign of a logic bug.
      if role == 'user':
        num_tokens, cost = count_tokens_and_cost(prompt=content, openai_model_name=model_name)
        total_prompt_cost += cost
        print(f'User Prompt: {content}, Tokens: {num_tokens}, cost: {cost}')

      # If the message is from the assistant, it's a completion
      elif role == 'assistant':
        num_tokens_completion, cost_completion = count_tokens_and_cost(prompt='',
                                                                       completion=content,
                                                                       openai_model_name=model_name)
        total_completion_cost += cost_completion
        print(f'Assistant Completion: {content}\nTokens: {num_tokens_completion}, cost: {cost_completion}')
  return total_convos, total_messages, total_prompt_cost, total_completion_cost


if __name__ == '__main__':
  pass

# if __name__ == '__main__':
#   print('starting main')
#   total_convos, total_messages, total_prompt_cost, total_completion_cost = analyze_conversations()
#   print(f'total_convos: {total_convos}, total_messages: {total_messages}')
#   print(f'total_prompt_cost: {total_prompt_cost}, total_completion_cost: {total_completion_cost}')
