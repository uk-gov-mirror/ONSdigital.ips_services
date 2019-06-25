from ips.util.Configuration import Configuration


class ServicesConfiguration(Configuration):

    def __init__(self):
        super().__init__(__name__, 'config.yaml')

    def get_database(self):
        return self.cfg['database']

    def get_shift_weight(self):
        return self.cfg['shift_weight']

    def get_non_response(self):
        return self.cfg['non_response']

    def get_minimums_weight(self):
        return self.cfg['minimums_weight']

    def get_traffic_weight(self):
        return self.cfg['traffic_weight']

    def get_unsampled_weight(self):
        return self.cfg['unsampled_weight']

    def get_imbalance_weight(self):
        return self.cfg['imbalance_weight']

    def get_final_weight(self):
        return self.cfg['final_weight']

    def get_stay_imputation(self):
        return self.cfg['stay_imputation']

    def get_fares_imputation(self):
        return self.cfg['fares_imputation']

    def get_spend_imputation(self):
        return self.cfg['spend_imputation']

    def get_rail_imputation(self):
        return self.cfg['rail_imputation']

    def get_regional_weights(self):
        return self.cfg['regional_weights']

    def get_town_and_stay_expenditure(self):
        return self.cfg['town_and_stay_expenditure']

    def get_air_miles(self):
        return self.cfg['air_miles']

    def sas_rounding(self):
        return self.cfg['rounding']['sas_rounding']
