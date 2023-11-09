





class LLM:

    def __init__(self) -> None:

        self._llm = 'mistral'

    def create_prompt(self, question: str):
        """
        This function creates a prompt to be sent to the LLM.
        """

        context = self.create_context(question)

        # use context docs in the prompt

        prompt_template = """
                Follow these steps exactly:
                1. Read this question as an experienced graph data scientist at Neo4j: {question} 
                2. Read and summarize the following context documents, ignoring any that do not relate to the user question: {context}
                3. Use this context and your knowledge to answer the user question.
                4. Return your answer with sources.
                            """
        
        prompt = PromptTemplate(input_variables=["question", "context"], template=prompt_template)
    
        return [
            prompt.format(question=question, context=context[['url', 'text']].to_dict('records')),
            list(context['index'])
        ]
        
    def create_conversation(self, llm_type:str):
        """
        This function intializes a conversation with the llm.
        The resulting conversation can be prompted successively and will
        remember previous interactions.
        """
        create_conversation_timer_start = time.perf_counter()
        print("llm type: ", llm_type)
        llm = self._init_llm(llm_type, st.session_state['temperature'])

        st.session_state['llm_memory'] = ConversationSummaryBufferMemory(llm=llm, max_token_limit=1000)

        res = ConversationChain(
            llm=llm,
            memory=st.session_state['llm_memory']
            ) 
        print("Create conversation time: "+str(round(time.perf_counter()-create_conversation_timer_start, 4))+" seconds.")

        return res