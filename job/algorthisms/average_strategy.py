from job.crawlers.uni_pool_data import pool_by_id, get_pool_day_datas, get_pool_hour_data
from job.services.fee_from_strategy import uniswap_strategy_algorithm
from job.utils.sqrt_price_math import convert_price_to_tick, convert_tick_to_price


class AverageStrategy:
    def __init__(self, pool, start_timestamp, end_timestamp, protocol='ethereum'):
        self.pool_data = pool_by_id(pool, protocol)
        self.pool_day_data = get_pool_day_datas(pool, protocol, start_timestamp, end_timestamp)
        self.pool = pool
        self.protocol = protocol
        self.end_timestamp = end_timestamp
        self.start_timestamp = start_timestamp

    def find_range(self, current_price):
        above_price = 0
        above_volumes = 0
        below_price, below_volumes = 0, 0
        below_prices = []
        for d in self.pool_day_data:
            price = float(d['close'])
            volume = float(d['volumeUSD'])
            if price > current_price:
                above_price = ((above_volumes * above_price + price * volume) / (above_volumes + volume))
                above_volumes += volume
            else:
                below_price = (below_volumes * below_price + price * volume) / (below_volumes + volume)
                below_volumes = below_volumes + volume
                # below_prices.append(price)

        # print(sum(below_prices) / len(below_price))
        return [below_price, above_price]

    def process(self):
        current_price =float( self.pool_day_data[-1]['close'])
        decimals0 = int(self.pool_data['token0']['decimals'])
        decimals1 = int(self.pool_data['token1']['decimals'])
        # current_tick = convert_price_to_tick(current_price, decimals0, decimals1)
        price_range = self.find_range(current_price)
        tick_range = [convert_price_to_tick(price, decimals0, decimals1) for price in price_range]
        pool_hour_data = get_pool_hour_data(self.pool, self.start_timestamp, self.end_timestamp, self.protocol)[::-1]

        data = uniswap_strategy_algorithm(
            pool_data=self.pool_data, backtest_data=pool_hour_data, investment_amount=1000,
            min_tick=tick_range[1], max_tick=tick_range[0])
        best_range = [convert_tick_to_price(tick, decimals0, decimals1) for tick in tick_range]

        return data, best_range

