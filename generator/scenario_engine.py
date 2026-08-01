import random

class ScenarioEngine:
    def __init__(self, config):
        self.config = config
        self.current_mode = self.config.get("simulation", {}).get("mode", "normal_day")
        self.scenarios = self.config.get("scenarios", {})
        self.base_rate = self.config.get("simulation", {}).get("target_events_per_second", 10)
        
    def set_scenario(self, mode_name):
        if mode_name in self.scenarios:
            self.current_mode = mode_name
            print(f"\n[SCENARIO ENGINE] SWITCHED TO: [{self.current_mode.upper()}]")
            
    def get_effective_rate(self):
        scenario_params = self.scenarios.get(self.current_mode, {})
        multiplier = scenario_params.get("rate_multiplier", 1.0)
        return int(self.base_rate * multiplier)
        
    def should_inject_fraud(self):
        scenario_params = self.scenarios.get(self.current_mode, {})
        fraud_prob = scenario_params.get("fraud_probability", 0.02)
        return random.random() < fraud_prob

    def get_abandonment_multiplier(self):
        scenario_params = self.scenarios.get(self.current_mode, {})
        return scenario_params.get("abandonment_multiplier", 1.0)
