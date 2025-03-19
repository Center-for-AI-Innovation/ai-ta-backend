---
description: Quickstart on how to get the most out of Cropwizard.
---

# CropWizard

CropWizard is a cutting-edge AI agronomy assistant that answers agricultural questions with expert precision. It consults over 450,000 selected online publications, including Extension resources from US land-grant universities and a growing set of open-access research publications. It can be used as:

* **a "virtual agronomist":** to obtain farming advice;
* **a research assistant:** to find the most relevant online publications for a topic of interest;
* **a search engine:** to look up basic information about agriculture.

### **Getting Started**&#x20;

Enter a text question in the chat bar at the bottom of the Chat page and you should receive a text response in a few seconds.  See "[_Text Questions and Responses_](./#text-questions-and-references)_"_, below, for more information.

Alternatively, you can upload one or more images and type in a question about those. See "[Image Questions](./#image-questions)" below for more information about multimodal questions answering.

You can also get answers to questions that require predefined computational tools. These tools have been registered with CropWizard and are invoked automatically when relevant. See "_Tool Questions_" below for more information about tools.

The tools functionality can be used to analyze your data, including spreadsheets (CSV) and databases (SQL). This functionality is preliminary — contact us if you are interested in that.

### **Customizing User Settings**

&#x20;**1. Account:** (Optional) Click on “Login” at the top right to log in to your account (or to create one).

&#x20;**2. New Chat:**  Click on “New Chat” at the top right to launch a fresh conversation (aka, session). Chat history consisting of previous questions, images, and answers for the current conversation is included along with every question posed to the LLM.

&#x20;**3. Settings:** Click on “Settings” at the top right to customize user-facing settings. These include:

* **Model:**  Control which LLM is used and how
* **LLM:**  Choose which LLM should be used to answer your questions.
* **Temperature:** Choose a value between 0 and 1.  Values close to 0 give more precise and less creative answers while high values close to 1 do the opposite. We recommend the default setting of 0.1 since the goal is technical question-answering and reliability is important.
* **Fancy Retrieval**:  More powerful LLM-based retrieval technique (disabled for now).
* **Document Groups**: Enable and disable various categories of documents in the Knowledge Base (KB). The documents in the KB are grouped in multiple ways: By University for extension documents; by publisher for research publications. “All Documents” are enabled by default. Disable that setting if you wish to select one or more other specific document groups
* **Tools:** Shows specific computational (i.e., data-driven) tools that will be invoked automatically when relevant to answering a question. Individual tools listed can be enabled or disabled.

&#x20;**4. Conversation History:**  All questions in the current history are listed in the left pane. Click on a question to reenter it automatically in the text bar.

* **Clear conversations**:  Click this to clear the conversation history. This does NOT start a new chat, so the page does not revert to the initial welcome page.
* **Export history**: Click this to export a JSON file with all questions and answers in the current history.

### **Text Questions and References**

* Enter a text question in the chat bar at the bottom of the Chat page and you should receive a text response in a few seconds.
* Cloud-hosted LLMs are usually faster than locally hosted ones because they run on more powerful servers.
* The displayed response cites relevant reference numbers. Click on the icon named "Sources" at the bottom of the response to see a numbered list of these References. Each list entry is a link to the original online source document.
*   **Steps when responding:**

    Depending on the question, CropWizard displays one or more of the following steps before printing the response. Click on the arrow to reveal the information in each step.

    * _**Optimized search query**_**:** If the question is the second or later in the chat session, the combined search query including the past questions and answers and the latest question are "optimized" by an LLM to reduce token count without losing content.
    * _**Retrieved documents**_**:** CropWizard prints out how many relevant document chunks in the KB were included with the user prompt. This step is always displayed
    * _**Routing to tools**_**:** The chat engine used in CropWizard searches for  one or more computational tools that may be relevant to the user prompt and lists them, if any (and invokes them in parallel). See the section on **Tools Questions** below for details.
    * _**Tool outputs**_**:** If any tools are invoked, their output is displayed. These outputs are combined with the retrieved knowledge chunks, input prompt, and any input images, in prompting the LLM for the final response.
    * _**Final Response:**_ Final response is generated encapsulating information from user prompt, image description and retrieved documents

### **CropWizard as a Research Assistant**

* Enter a research topic as your text prompt, i.e., your "question."
* The references to source documents in the response will give you relevant material to explore the topic in great detail.
* Use follow-up questions to explore any facets of the topic that are of interest. CropWizard is a conversation tool, and previous questions and even previous answers are retained as "context" for answering subsequent questions.

{% hint style="success" %}
Example: _Give me a detailed explanation of hairy vetch as a cover crop. Explain when it must be planted, what size range it grows to, how fast it grows to maturity, and the best ways to clear the field for planting in the next season._
{% endhint %}

### Image questions

* Upload one or more images by clicking on the little photo icon at the left of the chat bar.
* Type in a text question about the image(s) and receive a response, just like the text questions.
* **Steps when responding:**\
  For questions with images, CropWizard displays additional steps before printing the response:
  * _**Image description**_: An LLM is prompted to generate a textual description of the contents of the images, organized into a formatted list. This description is included when searching for relevant document chunks in the KB.
  * _**Final Response:**_ Final response is generated encapsulating information from user prompt, image description and retrieved documents

### **Tools questions**

* Optionally, upload one or more images by clicking on the little photo icon at the left of the chat bar related to pests
* Type in your question and receive a response, the AI will automatically invoke the necessary tools.
* **Steps when responding:**\
  For questions with images of pests, CropWizard displays additional steps before printing the response:
  * _**Image description**_: An LLM is prompted to generate a textual description of the contents of the images, organized into a formatted list. This description is included when searching for relevant document chunks in the KB.
  * **Retrieved documents:** CropWizard prints out how many relevant document chunks in the KB were included with the user prompt and image description
  * _**Tool inputs**_: If the user supplys an image, the Pest Detection tool is invoked, and the input image(s) to the tool are displayed. If other pre-configured tools are relevant, the AI will also invoke those tools in parallel. Many tools can run in parallel, pulling in real time data, or making API requests, or sending results to an email address.
  * _**Tool outputs**_: Tools can output both text and images. If the user supplys an image and it contains possible pests, the Pest Detection tool is invoked, and the output image(s) from this tool are displayed. The final LLM will use those images as input, so it can see the pest labels. If a different tool returns some text, like a crop harvesting report, that text will be passed along to the final LLM call.
  * _**Final Response:**_ Final response is generated encapsulating information from user prompt, image description, retrieved documents and all tool outputs.

For detailed information about the Pest Detection tool, check out the [Pest Detection Tool page](https://app.gitbook.com/o/SfApyd80yHo8lLe0r7PA/s/vdrzNTxffjmyrhd2NKsD/~/changes/87/cropwizard/pest-detection-tool).

### Example Questions

> What are the best practices for crop rotation in corn production?
>
> How can I manage pests in my soybean field?
>
> What are the nutrient requirements for wheat during the growing season?
>
> How do I improve soil fertility with cover crops?
>
> What irrigation techniques work best in drought-prone areas?

### Funding and acknowledgements

CropWizard is a research project launched as part of the **AIFARMS** national AI Institute for agriculture. AIFARMS is funded by USDA NIFA under award number 2020-67021-32799.

The CropWizard project has received additional funding from **Intel** Corporation, from Amazon AWS through the **Amazon-Illinois Center on AI for Conversational Experiences (AICE)**, and from the University of Illinois system through the **Discovery Partners Institute** Science program.

CropWizard is built on the remarkable **Illinois Chat** platform, which greatly simplifies and largely automates the creation of interactive question-answering services using documents and other data sources. Illinois Chat is funded by the **NCSA** [**Center for AI Innovation**](https://ai.ncsa.illinois.edu/) **(CAII), the** [**Office of the CIO**](https://cio.illinois.edu/) **of The University of Illinos at Urbana Champaign**, **The** [**Healthcare Innovation Office**](https://www.ncsa.illinois.edu/research/health-sciences/healthcare-innovation-program-office/) **at NCSA,** [**Gies College of Business**](https://giesbusiness.illinois.edu/)**, among anothers**. Illinois Chat is [fully open source and available in Github](https://github.com/CAII-NCSA/uiuc-chat-frontend) through a permissive MIT open-source license.
