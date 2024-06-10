# # def calculate_gradient(pool, investment_amount, min_range, max_range, protocol, start_timestamp, end_timestamp):
# #     # Tính toán đạo hàm của res theo min_range
# #     delta_min_range = (uniswap_strategy_backtest(pool, investment_amount, min_range + 0.01, max_range, protocol, start_timestamp, end_timestamp) -
# #                        uniswap_strategy_backtest(pool, investment_amount, min_range, max_range, protocol, start_timestamp, end_timestamp)) / 0.01
# #
# #     # Tính toán đạo hàm của res theo max_range
# #     delta_max_range = (uniswap_strategy_backtest(pool, investment_amount, min_range, max_range + 0.01, protocol, start_timestamp, end_timestamp) -
# #                        uniswap_strategy_backtest(pool, investment_amount, min_range, max_range, protocol, start_timestamp, end_timestamp)) / 0.01
# #
# #     return delta_min_range, delta_max_range
#
# import sympy as sp
#
# x, y = sp.symbols('x y')
# f = x**2 + y**3
#
# df_dx = sp.diff(f, x)
# df_dy = sp.diff(f, y)
#
# print("Derivative wrt x:", df_dx)
# print("Derivative wrt y:", df_dy)
