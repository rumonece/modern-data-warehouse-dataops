from cddp_solution.common.utils.custom_function import AbstractCustomFunction
from cddp_solution.common.utils.merge_to_scd2 import merge_to_scd2
import datetime


class Function(AbstractCustomFunction):
    def __init__(self, app_config, func_config):
        super().__init__(app_config, func_config)

    def execute(self):
        transform_target = self.func_config["target"]
        partition_keys = "ID"
        key_cols = "ID"
        data = self.spark.sql(
            "SELECT ID, shuiguo as Fruit, yanse as Colour, jiage as Price FROM raw_fruits_2"
        )
        current_time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        data = merge_to_scd2(
            self.spark,
            data,
            self.master_data_storage_path + "/" + transform_target,
            self.pz_dbname,
            partition_keys,
            key_cols,
            current_time,
        )
