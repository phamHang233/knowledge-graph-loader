import math
import time

from job.services.calculate_from_strategy import tokens_for_strategy, liquidity_for_strategy, calc_fees, pivot_fee_data
from job.crawlers.uni_pool_data import pool_by_id, get_pool_hour_data
from job.utils.sqrt_price_math import get_token_amount_of_user, convert_price_to_tick


def uniswap_strategy_backtest(pool, investment_amount, min_range, max_range, protocol, start_timestamp,
                              end_timestamp):
    if min_range >= max_range:
        return None
    pool_data = pool_by_id(pool, protocol)
    hourly_price_data = get_pool_hour_data(pool, start_timestamp, end_timestamp, protocol)
    if pool_data and hourly_price_data and len(hourly_price_data) > 0:
        backtest_data = hourly_price_data[::-1]  ## thời gian từ quá khứ đến hiện tại
        # print(min_range, max_range)
        entry_price = float(backtest_data[0]['close'])
        # entry_price = 1 / 1.0001 ** 193955 * 10 ** 12
        decimals0 = int(pool_data['token0']['decimals'])
        decimals1 = int(pool_data['token1']['decimals'])

        # tick_lower = convert_price_to_tick(min_range, decimals0, decimals1)
        # tick_upper = convert_price_to_tick(max_range, decimals0, decimals1)
        # tick = convert_price_to_tick(entry_price, decimals0, decimals1)
        # amount0, amount1 = get_token_amount_of_user(liquidity=investment_amount,
        #                                             sqrt_price_x96=math.sqrt(1.0001 ** tick) * 2 ** 96, tick=tick,
        #                                             tick_upper=tick_upper, tick_lower=tick_lower)
        amount0, amount1 = tokens_for_strategy(min_range=min_range, max_range=max_range,
                                               investment_amount=investment_amount, price=entry_price,
                                               decimals=decimals1 - decimals0)
        liquidity = liquidity_for_strategy(float(entry_price), min_range, max_range, amount0, amount1,
                                           decimals0, decimals1)
        # unbound_liquidity = liquidity_for_strategy(float(entry_price), 1.0001 ** -887220, 1.0001 ** 887220, amount0,
        #                                            amount1, decimals0, decimals1)
        hourly_backtest = calc_fees(backtest_data, pool_data, liquidity,
                                    min_range, max_range)
        return pivot_fee_data(amount0, amount1, hourly_backtest)


def uniswap_strategy_algorithm(backtest_data, pool_data, investment_amount, min_tick, max_tick):
    entry_price = float(backtest_data[0]['close'])
    decimals0 = int(pool_data['token0']['decimals'])
    decimals1 = int(pool_data['token1']['decimals'])
    max_range = 1/(1.0001 ** min_tick * 10 ** (decimals0 - decimals1))
    min_range = 1/(1.0001 ** max_tick * 10 ** (decimals0 - decimals1))
    amount0, amount1 = tokens_for_strategy(
        min_range=min_range, max_range=max_range, investment_amount=investment_amount, price=entry_price,
        decimals=decimals1 - decimals0)
    liquidity = liquidity_for_strategy(entry_price, min_range, max_range, amount0, amount1,
                                       decimals0, decimals1)
    # unbound_liquidity = liquidity_for_strategy(float(entry_price), 1.0001 ** -887220, 1.0001 ** 887220, amount0,
    #                                            amount1, decimals0, decimals1)
    hourly_backtest = calc_fees(backtest_data, pool_data, liquidity,
                                min_range, max_range)
    return pivot_fee_data(amount0, amount1, hourly_backtest)

#
# end_timestamp = int(time.time())
# start_timestamp = int(end_timestamp / 24 /3600) * 24 * 3600 - 3 * 24 * 3600 -1
# start_time = time.time()
# #
# print(uniswap_strategy_backtest(pool="0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640", investment_amount=1000,
#                                        min_range=3729.3582,
#                                        max_range=3850.6215,
#                                        protocol='ethereum', start_timestamp=start_timestamp,end_timestamp=end_timestamp))
# # print(res, time)
# print(time.time()-start_time)

# amount0, amount1 = tokens_for_strategy(1 / 1.0001 ** 196920 * 10 ** 12, 1 / 1.0001 ** 193260 * 10 ** 12, 103959.43,
#                                        3770.48,
#                                        12)
# liquidity = liquidity_for_strategy(float(3770.48), 1 / 1.0001 ** 196920 * 10 ** 12, 1 / 1.0001 ** 193260 * 10 ** 12, amount0, amount1,
#                                            6, 18)
# amount0, amount1 = get_token_amount_of_user(liquidity=9842733575012294,
#                                             sqrt_price_x96=math.sqrt(1.0001 ** 195688) * 2 ** 96, tick=195688,
#                                             tick_upper=196920, tick_lower=193260, decimals0=6, decimals1=18)
# print(amount0, amount1)
# print(liquidity)
# 33133.651899 19.967213229540583
