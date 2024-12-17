import json

class FactsMapper:
    def __init__(self):
        self.facts = {
            "car_id": None,
            "timestamp": None,
            "gear": None,
            "accelerator_pressed": False,
            "indicator_left_on": False,
            "indicator_right_on": False,
            "traffic_light": None,
            "intersection": False,
            "other_vehicle_approaching": False,
            "pedestrian_crossing": False,
            "pedestrian_present": False,
            "current_speed": 0,
            "speed_limit": None,
            "merge_lane_clear": True,
            "engine_off": False,
        }

    def set_facts(self, query_result):
        """
        Set the facts based on the query result.

        Args:
            query_result (dict): A dictionary containing column-value pairs.
        """
        for key, value in query_result.items():
            if key in self.facts:
                if value == 'true':
                    self.facts[key] = True
                elif value == 'false':
                    self.facts[key] = False
                else:
                    self.facts[key] = value

    def update_facts(self, data):
        """
        Update the current facts based on the provided data.

        Args:
            data (dict): Data from the LLM response.

        Returns:
            self (FactsMapper): The updated instance of FactsMapper.
        """
        try:
            message_content = json.loads(data['message']['content'])
            facts = message_content.get('facts', [])
            for fact in facts:
                fact_name = fact.get('fact_name')
                value = fact.get('value')
                if fact_name and value is not None:
                    self.facts[fact_name] = value
        except (KeyError, json.JSONDecodeError) as e:
            print(f"Error processing data: {e}")

        return self  # Return the instance for chaining or further use

    def reformat_facts_to_prompt(self, facts_list):
        """
        Reformat the facts into a string suitable for a prompt.

        Args:
            facts_list (list): The list of possible facts.

        Returns:
            tuple: A tuple containing formatted facts list and current facts.
        """
        facts_str = "\n    ".join(facts_list)
        current_facts_str = "\n    ".join(
            f"{key} = {value if value is not None else 'N/A'}" for key, value in self.facts.items()
        )

        return facts_str, current_facts_str
