import json

def update_facts(data, current_facts):
    try:
        message_content = json.loads(data['message']['content'])
        facts = message_content.get('facts', [])
        for fact in facts:
            fact_name = fact.get('fact_name')
            value = fact.get('value')
            if fact_name and value is not None:
                current_facts[fact_name] = value

    except (KeyError, json.JSONDecodeError) as e:
        print(f"Error processing data: {e}")

    return current_facts

def reformat_facts_to_prompt(facts_list, current_facts):
    facts_str = "\n    ".join(facts_list)
    current_facts_str = "\n    ".join(
        f"{key} = {value if value is not None else 'N/A'}" for key, value in current_facts.facts.items()
    )

    return facts_str,current_facts_str

# Example usage
data = {
    'message': {
        'role': 'assistant',
        'content': '{"facts":[{"fact_name":"gear","value":"neutral"}]}'
    }
}

current_facts = {
    "gear": "drive",
    "accelerator_pressed": "True",
    "indicator_left_on": "True",
    "clear_to_turn": "True",
    "traffic_light": "red",
    "other_vehicle_approaching": "True",
    "pedestrian_crossing": "True",
    "pedestrian_present": "True",
    "current_speed": "40",
    "speed_limit": "50",
    "engine_off": "True",
}
if __name__=="__main__":
    updated_facts = update_facts(data, current_facts)
    print(updated_facts)
