import requests
from facts_mapper import FactsMapper
from utils import reformat_facts_to_prompt

class LLMAgent:
    def __init__(self, url, facts_list):
        self.url = url
        self.facts_list = facts_list

    def format_prompt(self, facts_mapper):
        """
        Format the system prompt dynamically based on facts list and current facts.

        Args:
            facts_mapper (FactsMapper): An instance of the FactsMapper class.

        Returns:
            str: The formatted system prompt.
        """
        formatted_facts_list, formatted_current_facts = reformat_facts_to_prompt(facts_list=self.facts_list ,current_facts=facts_mapper)
        prompt = f"""
        You are an expert system designed to manage dynamic vehicle states and provide actionable recommendations based on predefined rules. If the system encounters a situation where no rules apply or a decision is unclear, you must modify existing facts from the provided list to resolve the issue. You are not allowed to introduce new facts.

        Facts  
        This is the list of facts that the system can add or modify:
            {formatted_facts_list}

        Current Facts  
            {formatted_current_facts}

        Rules and Suggestions  
        Each rule checks conditions and, if satisfied, determines an actionable outcome:

            Rule: Drive Forward  
                IF: gear = drive & accelerator_pressed = T  
                THEN: can_drive_forward = T  
                SUGGESTION: [The car is moving forward]  

            Rule: Turn Left  
                IF: indicator_left_on = T & clear_to_turn = T  
                THEN: can_turn_left = T  
                SUGGESTION: [The car can turn left]  

            Rule: Turn Right  
                IF: indicator_right_on = T & clear_to_turn = T  
                THEN: can_turn_right = T  
                SUGGESTION: [The car can turn right]  

            Rule: Stop at Red Light  
                IF: traffic_light = red  
                THEN: must_stop = T  
                SUGGESTION: [The car must stop at the red light]  

            Rule: Yield  
                IF: intersection = T & other_vehicle_approaching = T  
                THEN: must_yield = T  
                SUGGESTION: [The car must yield at the intersection]  

            Rule: Pedestrian Crossing  
                IF: pedestrian_crossing = T & pedestrian_present = T  
                THEN: must_stop = T  
                SUGGESTION: [The car must stop for pedestrians at the crossing]  

            Rule: Reverse  
                IF: gear = reverse & accelerator_pressed = T  
                THEN: can_reverse = T  
                SUGGESTION: [The car is moving backward]  

            Rule: Speed Limit  
                IF: current_speed <= speed_limit  
                THEN: is_within_speed_limit = T  
                SUGGESTION: [The car is within the speed limit]  

            Rule: Park  
                IF: gear = park & engine_off = T  
                THEN: is_parked = T  
                SUGGESTION: [The car is parked]  

            Rule: Merge  
                IF: merge_lane_clear = T & indicator_on = T  
                THEN: can_merge = T  
                SUGGESTION: [The car can merge into traffic]  

        Current System Evaluation  
        The system was unable to find results derived from the current facts and rules.  

        Task  

        Based on the facts and current evaluation:
            Suggest additional actionable recommendations (if any) by modifying the current facts.

        Response Format  
            Your response must strictly follow this JSON structure in a single compact line with no line breaks or indentation:
            {{"facts":[{{"fact_name":"<fact_name_1>","value":"<value_1>"}},{{"fact_name":"<fact_name_2>","value":"<value_2>"}}]}}  

        Constraints:  
            - Respond strictly in the specified format.  
            - You can only add or modify facts from the list given above.  
            - Do not add any new fact that doesn't exist in the list given above.  
            - Do not include explanations, commentary, or additional text.  
            - Ensure the JSON is valid and contains only the required fields.  
            - **Respond only in JSON format.**  
            - **Do not include any additional characters or strings.**  
            - **Ensure the JSON output is compact with no line breaks or spaces.**  
        """
        return prompt

    def call_llm(self, facts_mapper):
        """
        Send a request to the LLM and process its response.

        Args:
            facts_mapper (FactsMapper): An instance of the FactsMapper class.

        Returns:
            FactsMapper: The updated FactsMapper instance.
        """
        prompt = self.format_prompt(facts_mapper)
        response = requests.post(self.url, json={
            "model": "llama3.2:latest",
            "messages": [
                {"role": "system", "content": prompt},
                {"role": "user", "content": "{'facts': [{'fact_name': 'fact_example', 'value': example}]}"}
            ],
            "stream": False
        })

        try:
            data = response.json()
            facts_mapper.update_facts(data)
            return facts_mapper
        except ValueError:
            print("Response is not valid JSON:", response.text)
            return None
