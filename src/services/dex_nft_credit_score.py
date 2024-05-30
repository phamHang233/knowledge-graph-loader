# import csv
# import json
# import time
# from typing import List
#
# from src.utils.logger_utils import get_logger
#
# logger = get_logger('Dex NFT Flagged')
#
#
# def round_timestamp_to_date(timestamp):
#     timestamp_a_day = 86400
#     timestamp_unit_day = timestamp / timestamp_a_day
#     recover_to_unit_second = int(timestamp_unit_day) * timestamp_a_day
#     return recover_to_unit_second
#
#
# def check_logs(timestamp, log_timestamps):
#     """
#     Check if timestamp and latest log timestamps is in a day
#     """
#     if not log_timestamps:
#         return False
#     return round_timestamp_to_date(timestamp) == round_timestamp_to_date(log_timestamps[-1])
#
#
# def export_csv(data: List[dict], write_header=False, csv_file='artifacts/liquidate_user_credit_score.csv'):
#     with open(csv_file, 'w') as f:
#         writer = csv.DictWriter(f, data[0].keys())
#         if write_header:
#             writer.writeheader()
#         writer.writerows(data)
#
#
# if __name__ == '__main__':
#     trava_pool_address = '0x75de5f7c91a89c16714017c7443eca20c7a8c295'
#     chain = '0x38'
#
#     calculator = WalletsCreditScore('arangodb@read_only:read_only_123@http://139.59.226.71:1012',
#                                     statistic_path='../data')
#     # calculator.trava_scores(trava_pool_address, batch_size=200)
#     calculator.calculate_multichain_wallets_history(chain, trava_pool_address, batch_size=200)