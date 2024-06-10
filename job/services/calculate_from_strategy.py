import datetime
import math
import time

from job.utils.number import log_with_base


def tokens_for_strategy(min_range: float, max_range: float, investment_amount: float, price: float, decimals: int):
    """
    token 1 được gọi là base token , token 0: quote token
    :param min_range:
    :param max_range:
    :param investment_amount: số lượng token deposit tính theo base token
    :param price : gía của token 0/ token 1
    :param decimals:
    :return: lượng token0, token1 thực sự sẽ được deposit vào pool theo tỉ lệ
    """
    sqrt_price = math.sqrt(price) * math.sqrt(10 ** decimals)
    sqrt_low = math.sqrt(min_range * 10 ** decimals)
    sqrt_high = math.sqrt(max_range * 10 ** decimals)
    if sqrt_low < sqrt_price < sqrt_high:
        delta = investment_amount / (
                (sqrt_price - sqrt_low) + (((1 / sqrt_price) - (1 / sqrt_high)) * sqrt_price ** 2))
        amount1 = delta * (sqrt_price - sqrt_low)
        amount0 = delta * ((1 / sqrt_price) - (1 / sqrt_high)) * 10 ** decimals
    elif sqrt_price < sqrt_low:
        delta = investment_amount / (((1 / sqrt_low) - (1 / sqrt_high)) * price)
        amount1 = 0
        amount0 = delta * ((1 / sqrt_low) - (1 / sqrt_high))
    else:
        delta = investment_amount / (sqrt_high - sqrt_low)
        amount1 = delta * (sqrt_high - sqrt_low)
        amount0 = 0

    return amount0, amount1


def liquidity_for_strategy(price, min_range, max_range, amount0, amount1, decimals0, decimals1):
    """
    tính ra lượng liquidity sẽ deposit tương ứng với amount0, amount1
    """
    decimals = decimals1 - decimals0
    low_high = [math.sqrt(min_range * 10 ** decimals) * 2 ** 96, math.sqrt(max_range * 10 ** decimals) * 2 ** 96]
    s_price = math.sqrt(price * 10 ** decimals) * 2 ** 96
    s_low = min(low_high)
    s_high = max(low_high)
    if s_low < s_price < s_high:
        liq0 = amount0 / (2 ** 96 * (s_high - s_price) / s_high / s_price) * 10 ** decimals0
        liq1 = amount1 / ((s_price - s_low) / 2 ** 96 / 10 ** decimals1)
        return min(liq0, liq1)
    elif s_price < s_low:
        return amount0 / (2 ** 96 * (s_high - s_low) / s_high / s_low) * 10 ** decimals0
    else:
        return amount1 / ((s_high - s_low) / 2 ** 96 / 10 ** decimals1)


def calc_fees(data, pool, liquidity, min_range, max_range):
    result = []

    for i, d in enumerate(data):
        fg = calc_unbounded_fees_per_unit(int(d['feeGrowthGlobal0X128']), int(data[i - 1]['feeGrowthGlobal0X128']),
                                          int(d['feeGrowthGlobal1X128']), int(data[i - 1]['feeGrowthGlobal1X128']),
                                          pool) if i else [0, 0]
        low = float(d['low'])
        high = float(d['high'])

        low_tick = get_tick_from_price(low, pool)
        high_tick = get_tick_from_price(high, pool)
        min_tick = get_tick_from_price(min_range, pool)
        max_tick = get_tick_from_price(max_range, pool)

        active_liquidity = active_liquidity_for_candle(min_tick, max_tick, low_tick,
                                                       high_tick)  # tính thời gian liquiditity được active
        tokens = tokens_from_liquidity(float(d['close']), min_range, max_range, liquidity,
                                       int(pool['token0']['decimals']),
                                       int(pool['token1']['decimals']))
        fee_token0 = fg[0] * liquidity * active_liquidity / 100 if i else 0
        fee_token1 = fg[1] * liquidity * active_liquidity / 100 if i else 0

        # fee_unb0 = fg[0] * unbounded_liquidity
        # fee_unb1 = fg[1] * unbounded_liquidity

        lastest_record = data[-1]
        # first_close = data[0]['close']

        # token_ratio_first_close = tokens_from_liquidity(float(first_close), min_range, max_range, liquidity,
        #                                                 int(pool['token0']['decimals']),
        #                                                 int(pool['token1']['decimals']))  # lượng token deposit ban đầu
        # x0 = token_ratio_first_close[1]
        # y0 = token_ratio_first_close[0]

        # fg_v = fg[0] + (fg[1] * float(d['close'])) if i else 0
        fee_v = fee_token0 + fee_token1 * float(d['close']) if i else 0
        # fee_unbound = fee_unb0 + fee_unb1 * float(d['close']) if i else 0
        amount_v = tokens[0] + tokens[1] * float(d['close'])  ## lượng token investment tại thơi điểm  hiện tại
        fee_usd = fee_v * float(lastest_record['pool']['totalValueLockedUSD']) / (
                float(lastest_record['pool']['totalValueLockedToken1']) * float(lastest_record['close'])
                + float(lastest_record['pool']['totalValueLockedToken0']))
        # amount_tr = investment + (amount_v - x0 * float(d['close']) - y0)
        date = datetime.datetime.fromtimestamp(d['periodStartUnix'])
        daily_data_result = {
            "date": date,
            # 'fg0': fg[0],
            # 'fg1': fg[1],
            'activeLiquidity': active_liquidity,
            # "feeToken0": fee_token0,
            # 'feeToken1': fee_token1,
            # 'tokens': tokens,
            # 'fgV': fg_v,
            'feeV': fee_v,
            # 'feeUnb': fee_unbound,
            'amountV': amount_v,
            # 'amountTR': amount_tr,
            'feeUSD': fee_usd,
            'close': float(d['close']),
        }
        daily_data_result.update(d)
        result.append(daily_data_result)
    return result


def pivot_fee_data(amount0, amount1, data):
    def create_pivot_record(date, data):
        return {
            "date": f"{date.month}/{date.day}/{date.year}",
            "day": date.day,
            "month": date.month,
            "year": date.year,
            # "feeToken0": data["feeToken0"],
            # "feeToken1": data["feeToken1"],
            "feeV": data["feeV"],
            # "feeUnb": data["feeUnb"],
            # "fgV": float(data["fgV"]),
            "feeUSD": data["feeUSD"],
            "activeLiquidity": data.get("activeLiquidity", 0),
            "amountV": data["amountV"],
            # "amountTR": data["amountTR"],
            "amountVLast": data["amountV"],
            # "percFee": data["feeV"] / data["amountV"] if data["amountV"] > 0 else 0,
            # "close": data["close"],
            "count": 1
        }

    total_fee = 0
    ave_active = 0
    first_date = datetime.datetime.fromtimestamp(data[0]["periodStartUnix"])
    pivot = [create_pivot_record(first_date, data[0])]
    for i, d in enumerate(data):
        ave_active = (ave_active * i + d["activeLiquidity"]) / (i + 1)
        total_fee += float(d["feeUSD"])

        if i > 0:
            current_date = datetime.datetime.fromtimestamp(d["periodStartUnix"], tz=datetime.timezone.utc)
            current_price_tick = pivot[-1]

            if (current_date.day == current_price_tick["day"] and
                    current_date.month == current_price_tick["month"] and
                    current_date.year == current_price_tick["year"]):

                # current_price_tick["feeToken0"] += d["feeToken0"]
                # current_price_tick["feeToken1"] += d["feeToken1"]
                current_price_tick["feeV"] += d["feeV"]
                # current_price_tick["feeUnb"] += d["feeUnb"]
                # current_price_tick["fgV"] += float(d["fgV"])
                current_price_tick["feeUSD"] += d["feeUSD"]
                current_price_tick["activeLiquidity"] += d["activeLiquidity"]
                current_price_tick["amountVLast"] = d["amountV"]
                current_price_tick["count"] += 1

                if i == (len(data) - 1):
                    current_price_tick["activeLiquidity"] /= current_price_tick["count"]

                    # current_price_tick["percFee"] = (current_price_tick["feeV"] / current_price_tick[
                    #     "amountV"]) * 100 if current_price_tick["amountV"] > 0 else 0
            else:
                current_price_tick["activeLiquidity"] /= current_price_tick["count"]

                # current_price_tick["percFee"] = (current_price_tick["feeV"] / current_price_tick["amountV"]) * 100 if \
                # current_price_tick["amountV"] > 0 else 0
                pivot.append(create_pivot_record(current_date, d))

    token_price = (float(data[-1]['pool']['totalValueLockedUSD'])
                   / (float(data[-1]['pool']['totalValueLockedToken1']) * float(data[-1]['close'])
                      + float(data[-1]['pool']['totalValueLockedToken0'])))
    x = data[::-1]
    price_rate = float(data[-1]['close'])
    ref_invest_amount = (amount0 * price_rate + amount1)
    ref_invest = ref_invest_amount * token_price
    invest_change = (pivot[-1]['amountV'] - ref_invest_amount) * token_price
    apr = (total_fee + invest_change) / ref_invest / 30 * 365
    return {
        'apr': apr,
        'timeInRange': ave_active,
        'PnL': invest_change,
        'investedAsset': ref_invest,
        'currentInvest': pivot[-1]['amountV'],
        'fee_apr': total_fee /ref_invest /30 * 365
    }


def calc_unbounded_fees_per_unit(global_fee0, pre_global_fee0, global_fee1, pre_global_fee1, pool):
    """
    :return: fee of token 0, token1 earned in 1 period by 1 unit of unbounded liquidity
    """
    decimals0 = int(pool['token0']['decimals'])
    decimals1 = int(pool['token1']['decimals'])
    fg0_0 = global_fee0 / 2 ** 128 / 10 ** decimals0
    fg0_1 = pre_global_fee0 / 2 ** 128 / 10 ** decimals0

    fg1_0 = global_fee1 / 2 ** 128 / 10 ** decimals1
    fg1_1 = pre_global_fee1 / 2 ** 128 / 10 ** decimals1

    return [fg0_0 - fg0_1, fg1_0 - fg1_1]


def get_tick_from_price(price, pool):
    decimals0 = int(pool['token0']['decimals'])
    decimals1 = int(pool['token1']['decimals'])
    val_to_log = price * (10 ** (decimals0 - decimals1))
    return round(log_with_base(val_to_log, 1.0001))


def active_liquidity_for_candle(min_tick, max_tick, low_tick, high_tick):
    """
    estimate the percentage of active liquidity for 1 period for a strategy based on min max bounds
    low and high are the period's candle low / high values
    :param min_tick: tick được tính từ min_range
    :param max_tick: tick được tính từ max_range
    :param low_tick: tick được tính từ giá nhỏ hơn của pool hiện tại
    :param high_tick: tick được tính từ giá lớn hơn của pool hiện tại
    :return: percentage of active liquidity
    """

    if high_tick - low_tick:
        divider = high_tick - low_tick
        ratio_true = (min(max_tick, high_tick) - max(min_tick, low_tick)) / divider
    else:
        ratio_true = 1
    if high_tick > min_tick and low_tick < max_tick:
        ratio = ratio_true * 100
    else:
        ratio = 0

    return ratio if ratio else 0


def tokens_from_liquidity(price, min_range, max_range, liquidity, decimals0, decimals1):
    """
    Calculate the number of tokens for a Strategy at a specific amount of liquidity & price

    """
    decimals = decimals1 - decimals0
    low_high = [math.sqrt(min_range * 10 ** decimals) * 2 ** 96, math.sqrt(max_range * 10 ** decimals) * 2 ** 96]
    s_price = math.sqrt(price * 10 ** decimals) * 2 ** 96
    s_low = min(low_high)
    s_high = max(low_high)
    if s_low < s_price < s_high:
        amount0 = liquidity * (s_price - s_low) / 2 ** 96 / 10 ** decimals1
        amount1 = ((liquidity * 2 ** 96 * (s_high - s_price) / s_high / s_price) / 10 ** decimals0)
        return [amount0, amount1]
    elif s_price < s_low:
        amount1 = ((liquidity * 2 ** 96 * (s_high - s_low) / s_high / s_low) / 10 ** decimals0)
        return [0, amount1]
    else:
        amount0 = liquidity * (s_high - s_low) / 2 ** 96 / 10 ** decimals1
        return [amount0, 0]
