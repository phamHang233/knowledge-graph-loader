# import time
#
# import numpy as np
# import random
#
# from job.crawlers.uni_pool_data import get_pool_day_datas
# from job.services.fee_from_strategy import uniswap_strategy_backtest
#
#
# class SimulatedAnneal:
#
#     def __init__(self, pool, protocol, min_price_bounds, max_price_bounds,
#                  iterations, cooling_rate):
#         self.pool = pool
#         self.protocol = protocol
#         self.iterations = iterations
#         self.cooling_rate = cooling_rate
#         self.T0 = 100.0
#         self.min_price_bounds = min_price_bounds
#         self.max_price_bounds = max_price_bounds
#         self.end_timestamp = int(time.time())
#         self.start_timestamp = self.end_timestamp - 31 * 24 * 3600
#
#     def simulated_annealing(self):
#         while True:
#             min_price = random.uniform(*self.min_price_bounds)
#             max_price = random.uniform(*self.max_price_bounds)
#             data = uniswap_strategy_backtest(
#                 pool=self.pool, investment_amount=1000, min_range=min_price, max_range=max_price, protocol=self.protocol,
#                 start_timestamp=self.start_timestamp, end_timestamp=self.end_timestamp)
#             if data:
#                 break
#
#         best_min_price, best_max_price, best_apr = min_price, max_price, data['apr']
#
#         for _ in range(self.iterations):
#             # Rối loạn
#             delta_min_price = random.uniform(-0.1, 0.1) * min_price
#             delta_max_price = random.uniform(-0.1, 0.1) * max_price
#             new_min_price = min_price + random.uniform(-delta_min_price, delta_min_price)
#             new_max_price = max_price + random.uniform(-delta_max_price, delta_max_price)
#
#             # Giữ trong phạm vi
#             new_min_price = max(self.min_price_bounds[0], min(self.min_price_bounds[1], new_min_price))
#             new_max_price = max(self.max_price_bounds[0], min(self.max_price_bounds[1], new_max_price))
#
#             # Tính lợi nhuận mới
#             data = uniswap_strategy_backtest(
#                 pool=self.pool, investment_amount=1000, min_range=new_min_price, max_range=new_max_price,
#                 protocol=self.protocol, start_timestamp=self.start_timestamp, end_timestamp=self.end_timestamp)
#             if not data:
#                 continue
#             new_apr = data['apr']
#             # Chấp nhận giải pháp mới?
#             delta_apr = new_apr - best_apr
#             if delta_apr >= 0:
#                 min_price, max_price, best_apr = new_min_price, new_max_price, new_apr
#             elif random.random() < np.exp(-delta_apr / self.T0):
#                 min_price, max_price = new_min_price, new_max_price
#
#             # Giảm nhiệt độ
#             self.T0 *= self.cooling_rate
#
#         return best_min_price, best_max_price, best_apr
#
#     def process(self):
#         self.simulated_annealing()
#
#
