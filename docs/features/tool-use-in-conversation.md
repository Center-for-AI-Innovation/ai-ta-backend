---
description: >-
  To best answer your question, the LLM system uses tools as needed. Create your
  own tools with N8N.
---

# Tool use in conversation

Now in UIUC.chat, you can create your own tools for the LLM to use seamlessly during a conversation. See our demo below.

<figure><img src="../.gitbook/assets/image (1).png" alt=""><figcaption><p>The concept of "tool use." The LLM parses a user input and determines if any of the available tools are relevant. If so, the AI generates the input params and then our code manually invokes the tool requested by the LLM. Finally, the output is sent back to the LLM to generate a final answer consider the tool output. <a href="https://python.langchain.com/v0.1/docs/use_cases/tool_use/">Image source</a>.</p></figcaption></figure>

## Tools demo

{% embed url="https://www.youtube.com/watch?v=sSai_F1cbEI" %}

## Get access

Tools are invite-only during beta. Just shoot me an email and I'll send you an onboarding invite no problem. &#x20;

`Email me: rohan13@illinois.edu`

I'm happy to onboard anyone. No need to justify anything, no calls required.&#x20;

## N8n to easily define tools!

To make it as easy as possible for you to create your own tools, we self-host n8n a visual workflow builder. We chose n8n after an intense study of the market because of:&#x20;

* n8n's [massive library of integrations](https://n8n.io/integrations/) (Slack, Jira, Google drive, Gmail, etc)
* Library of [creative & helpful templates](https://n8n.io/workflows/)
  * I like [Talk to your SQLite database with a LangChain AI Agent](https://n8n.io/workflows/2292-talk-to-your-sqlite-database-with-a-langchain-ai-agent/)
* [Cool features](https://n8n.io/features/) (drag & drop, code running, LLM abilities)

## Usage - Write your own tool

After receiving an invite, login on [tools.UIUC.chat](https://tools.uiuc.chat/).

Tools can take text or images as input and text or images as output. These modalities are supported by current OpenAI models, maybe audio is coming soon.

### Inputs

{% hint style="warning" %}
All tools _**MUST**_ start with a `n8n Form Trigger`. Use this to define the inputs to your tool.&#x20;
{% endhint %}

The AI will use the `Form Title`, `Form Description` and the `Form Fields` to decide when to use your tool, so make those as descriptive as possible so the AI will know how to best use your tool.



<figure><img src="../.gitbook/assets/CleanShot 2024-06-26 at 17.26.27.png" alt=""><figcaption><p>ALL tools must start with a <code>n8n Form Trigger</code></p></figcaption></figure>

<figure><img src="../.gitbook/assets/CleanShot 2024-06-26 at 17.28.22.png" alt=""><figcaption><p>You can have both required and optional input parameters. You can have as many parameters as you like.</p></figcaption></figure>

### Text Outputs

No explicit return is necessary because we use the output of your last node as the return value of the tool. This works seamlessly across all the nodes offered by n8n.

## Using Images in tools

Images are passed via an array of `image_urls` in a json object. They must be URLs to images, no raw/binary data.

```json
{
   "image_urls": ["url","url","https://bucket.r2.cloudflare.com/img-path"],
   "other-useful-text": "These images depict the circle of life in the savanna."
}
```

### Image Inputs

To take an image as input, put `image_urls` as a field in your `n8n Form Trigger` (shown below).

<figure><img src="../.gitbook/assets/CleanShot 2024-06-26 at 17.39.18.png" alt=""><figcaption><p><code>image_urls</code> as an input in a <code>n8n Form Trigger</code>.</p></figcaption></figure>

If you're using code, you'll have to parse this `image_urls` text into a JSON array. This is required because n8n doesn't allow JSON inputs, so we use a text input and have to parse the JSON data manually.

```python
image_urls: List[str] = post_body.get('image_urls', []) # grab data from POST body

if image_urls and isinstance(image_urls, str):
  image_urls = json.loads(image_urls)
print(f"Parsed image URLs: {image_urls}")
```

### Image Outputs

Your final node must return a JSON object that contains a top-level key `"image_urls"`. You may return as many images as you'd like. They must be URLs to images, no raw/binary data.

You can return images + other text. That's fine and encouraged! Your tool can output arbitrary JSON data. Just the `image_urls` field is specially handled.&#x20;

```json
{
   "image_urls": ["url","url"],
   "other-useful-text": "These images depict the circle of life in the savanna.",
   "animals_detected": [
      "tigers": 5,
      "antelope": 1
   ]
}
```

### Example tool using images

There's just two nodes: first capture input params, then call a POST endpoint hosted on Beam's serverless infra.

<figure><img src="../.gitbook/assets/CleanShot 2024-06-26 at 17.53.49.png" alt=""><figcaption></figcaption></figure>

Step 1: Capture the input params. We just need the `image_urls` field.

<figure><img src="../.gitbook/assets/CleanShot 2024-06-26 at 17.50.21.png" alt=""><figcaption><p>n8n form node with only <code>image_urls</code> as input. </p></figcaption></figure>

Step 2: call a POST endpoint hosted on Beam. Auth is handled by an Authorization Header. The `body` uses the values from the last node as an input. No explicit return is necessary because we use the output of your last node as the return value of the tool. This works seamlessly across all the nodes offered by n8n.

<div><figure><img src="../.gitbook/assets/CleanShot 2024-06-26 at 17.51.52.png" alt=""><figcaption><p>HTTP</p></figcaption></figure> <figure><img src="../.gitbook/assets/CleanShot 2024-06-26 at 17.52.26.png" alt=""><figcaption></figcaption></figure></div>

## Recommended patterns

In our experience, we like defining arbitrary python functions and run those as tools. Many of our tools look like a single `n8n Form Trigger -> HTTP request` to our python code.

<figure><img src="../.gitbook/assets/CleanShot 2024-06-26 at 17.47.41.png" alt=""><figcaption><p>Call any HTTP endpoint you'd like! </p></figcaption></figure>

I host these endpoints on [Beam.cloud](https://www.beam.cloud/), which is a [phenomenal "serverless" hosting service](https://x.com/KastanDay/status/1790066477372158196). They're super low cost, with a truly next-level development experience. I highly recommend them.

### Development

During beta, try using the feature branch here: [https://uiuc-chat-git-n8n-ui-kastandays-projects.vercel.app/](https://uiuc-chat-git-n8n-ui-kastandays-projects.vercel.app/)

1. Define tools in `uiuc.chat/<YOUR-PROJECT>/tools`
2. Enable the tools you want active in your project
3. Start chatting, tools will be invoked as needed.

