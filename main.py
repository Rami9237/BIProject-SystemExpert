from facts_mapper import FactsMapper
import clickhouse_driver
from llm import LLMAgent
from etl import upload_csv_to_clickhouse
url = "https://065b-197-15-239-141.ngrok-free.app/api/chat"
facts_list = [
    "gear",
    "accelerator_pressed",
    "indicator_left_on",
    "indicator_right_on",
    "traffic_light",
    "intersection",
    "other_vehicle_approaching",
    "pedestrian_crossing",
    "pedestrian_present",
    "current_speed",
    "speed_limit",
    "merge_lane_clear",
    "engine_off"
]
agent = LLMAgent(url, facts_list)

class ExpertSystem:
    def __init__(self, client):
        self.client = client
        self.facts = FactsMapper()
        self.inferred_facts = {}
        self.suggestions = []
        self.rules = []

    def fetch_rules_and_conditions(self):
        query = """
        SELECT r.rule_id, r.rule_name, r.action, r.description, rc.condition_key, rc.operator, rc.condition_value, rc.logical_operator
        FROM rules r
        JOIN rule_conditions rc ON r.rule_id = rc.rule_id
        ORDER BY r.rule_id, rc.condition_id;
        """
        result = self.client.execute(query)

        rules_dict = {}
        for row in result:
            rule_id = row[0]
            rule_name = row[1]
            condition_key = row[4]
            operator = row[5]
            condition_value = row[6]
            logical_operator = row[7]

            if rule_id not in rules_dict:
                rules_dict[rule_id] = {
                    'rule_name': rule_name,
                    'conditions': []
                }

            rules_dict[rule_id]['conditions'].append({
                'condition_key': condition_key,
                'operator': operator,
                'condition_value': condition_value,
                'logical_operator': logical_operator
            })

        self.rules = rules_dict

    def get_facts(self, car_id):
        query = f"""
        SELECT 
            car_id,
            timestamp,
            gear,
            accelerator_pressed,
            indicator_left_on,
            indicator_right_on,
            traffic_light,
            intersection,
            other_vehicle_approaching,
            pedestrian_crossing,
            pedestrian_present,
            current_speed,
            speed_limit,
            merge_lane_clear,
            engine_off
        FROM cars
        WHERE car_id = {car_id};
        """

        result = self.client.execute(query)

        column_names = [
            "car_id",
            "timestamp",
            "gear",
            "accelerator_pressed",
            "indicator_left_on",
            "indicator_right_on",
            "traffic_light",
            "intersection",
            "other_vehicle_approaching",
            "pedestrian_crossing",
            "pedestrian_present",
            "current_speed",
            "speed_limit",
            "merge_lane_clear",
            "engine_off"
        ]

        for row in result:
            query_result = {column_names[i]: row[i] for i in range(len(column_names))}
            self.facts.set_facts(query_result)

    def evaluate_conditions(self, conditions):
        conditions_met = True
        for condition in conditions:
            condition_key = condition['condition_key']
            operator = condition['operator']
            condition_value = condition['condition_value']
            if condition_value == 'true':
                condition_value = 1
            elif condition_value == 'false':
                condition_value = 0

            fact_value = self.facts.facts.get(condition_key)

            if condition_value in self.facts.facts:
                condition_value = self.facts.facts.get(condition_value)

            try:
                fact_value = float(fact_value)
                condition_value = float(condition_value)
            except ValueError:
                pass 

            if operator == '=':
                condition_met = str(fact_value) == str(condition_value)
            elif operator == '<=':
                condition_met = fact_value <= condition_value
            elif operator == '>=':
                condition_met = fact_value >= condition_value
            elif operator == '<':
                condition_met = fact_value < condition_value
            elif operator == '>':
                condition_met = fact_value > condition_value
            else:
                raise ValueError(f"Unsupported operator: {operator}")

            if not condition_met:
                conditions_met = False
                break

        return conditions_met

    def run_inference(self):
        self.fetch_rules_and_conditions()

        for rule_id, rule_data in self.rules.items():
            conditions = rule_data['conditions']

            if self.evaluate_conditions(conditions):
                self.infer(rule_id, True, f"The rule '{rule_data['rule_name']}' is triggered.")

        if self.inferred_facts:
            print("New facts inferred:", self.inferred_facts)
            print("Suggestions:")
            for suggestion in self.suggestions:
                print(f"- {suggestion}")
        else:
            print("No new inferences made. Calling LLM...")
            newfacts_mapper = FactsMapper()
            newfacts_mapper.set_facts(self.facts.facts)
            self.facts = agent.call_llm(facts_mapper=newfacts_mapper)
            self.run_inference()



    def infer(self, rule_id, value, suggestion):
        if rule_id not in self.inferred_facts:
            self.inferred_facts[rule_id] = value
            self.suggestions.append(suggestion)

if __name__ == "__main__":

    client = clickhouse_driver.Client(
        host='localhost',
        port=9000,
        user='default',    
        password='',       
        database='default'
    )
    system = ExpertSystem(client)

    print("Fetching facts...")
    system.get_facts("1")

    system.run_inference()
