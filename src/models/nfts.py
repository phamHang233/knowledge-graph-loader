import math

from defi_services.utils.sqrt_price_math import get_token_amount_of_user


class NFT:
    def __init__(self, id="", chain=None) -> None:
        self.token_id: str = id
        self.chain: str = chain
        self.liquidity = 0
        self.pool_address = None
        self.tick_lower = 0
        self.tick_upper = 0
        self.collected_fee = {}
        self.uncollected_fee = {}
        self.last_interact_at = 0
        self.last_called_at = 0
        self.liquidity_change_logs = {}
        self.fee_change_logs = {}
        self.nft_manager_address = None
        self.apr_in_month = 0
        # self.apr = 0
        self.wallet = None
        self.last_updated_fee_at = 0
        self.pnl = 0
        self.tokens = {}
        self.current_invest_in_usd = 0
        self.ref_tokens= {}
        self.fee = 0
        # self.invested_asset_in_usd = 0

    def to_dict(self):
        return {
            "tokenId": self.token_id.lower(),
            "chainId": self.chain,
            "lastInteractAt": self.last_interact_at,
            'liquidity': self.liquidity,
            'poolAddress': self.pool_address,
            'tickLower': self.tick_lower,
            'tickUpper': self.tick_upper,
            'collectedFee': self.collected_fee,
            'lastCalledAt': self.last_called_at,
            'liquidityChangeLogs': self.liquidity_change_logs,
            'feeChangeLogs': self.fee_change_logs,
            'nftManagerAddress': self.nft_manager_address,
            "uncollectedFee": self.uncollected_fee,
            "wallet": self.wallet,
            "aprInMonth": self.apr_in_month,
            # 'apr': self.apr,
            "lastUpdatedFeeAt": self.last_updated_fee_at,
            'PnL': self.pnl,
            'tokens': self.tokens,
            'assetsInUSD': self.current_invest_in_usd,
            # 'investedAssetInUSD': self.invested_asset_in_usd
            'refTokens': self.ref_tokens,
            'feeEarn': self.fee
        }

    def from_dict(self, json_dict):
        self.liquidity = json_dict.get("liquidity", 0)
        self.tick_upper = json_dict.get('tickUpper', 0)
        self.tick_lower = json_dict.get('tickLower', 0)
        self.collected_fee = json_dict.get('collectedFee', {})
        self.uncollected_fee = json_dict.get('uncollectedFee', {})
        self.last_interact_at = json_dict.get('lastInteractAt', 0)
        self.liquidity_change_logs = json_dict.get('liquidityChangeLogs', {})
        self.fee_change_logs = json_dict.get('feeChangeLogs', {})
        self.last_called_at = json_dict.get('lastCalledAt', 0)
        self.nft_manager_address = json_dict.get('nftManagerAddress', "")
        self.pool_address = json_dict.get('poolAddress', "")
        self.wallet = json_dict.get("wallet")
        self.last_updated_fee_at = json_dict.get('lastUpdatedFeeAt', 0)

    def cal_apr_in_month(self, start_block, fee0_before, fee1_before, pool_info, tick_before, tick):
        token0_info = pool_info['tokens'][0]
        token1_info = pool_info['tokens'][1]
        token0_address = token0_info['address']
        token1_address = token1_info['address']
        token0_price = token0_info.get('liquidityValueInUSD', 0) / token0_info['liquidityAmount']
        token1_price = token1_info.get('liquidityValueInUSD', 0) / token1_info['liquidityAmount']
        decimals0 = token0_info['decimals']
        decimals1 = token1_info['decimals']
        collected_amount0 = 0
        collected_amount1 = 0
        if token1_price == 0 or token0_price == 0:
            return {}
        for block_number, amount in self.fee_change_logs.items():
            if int(block_number) >= start_block:
                collected_amount0 += amount[token0_address]
                collected_amount1 += amount[token1_address]

        token0_change = self.uncollected_fee[token0_address] - fee0_before + collected_amount0
        token1_change = self.uncollected_fee[token1_address] - fee1_before + collected_amount1
        fee_change_in_usd = token0_change * token0_price + token1_change * token1_price

        invest0_before, invest1_before = get_token_amount_of_user(
            liquidity=int(self.liquidity), sqrt_price_x96=math.sqrt(1.0001 ** tick_before) * 2 ** 96, tick=tick_before,
            tick_upper=self.tick_upper, tick_lower=self.tick_lower)
        invest0, invest1 = get_token_amount_of_user(
            liquidity=int(self.liquidity), sqrt_price_x96=math.sqrt(1.0001 ** tick) * 2 ** 96, tick=tick,
            tick_upper=self.tick_upper, tick_lower=self.tick_lower)

        current_invest_in_usd = ((invest1 * token1_price / 10 ** decimals1)
                                 + (invest0 * token0_price / 10 ** decimals0))
        ref_invest_in_usd = ((invest1_before * token1_price / 10 ** decimals1)
                             + (invest0_before * token0_price / 10 ** decimals0))
        investment_change_in_usd = current_invest_in_usd - ref_invest_in_usd
        apr = (fee_change_in_usd + investment_change_in_usd) / ref_invest_in_usd / 1 * 365 if ref_invest_in_usd > 1e-03 else 0
        if apr > 10e3:
            print(self.token_id)
        else:
            self.tokens = {
                token0_address: invest0 / 10 ** decimals0,
                token1_address: invest1 / 10 ** decimals1,

            }
            self.ref_tokens = {
                token0_address: invest0_before / 10 ** decimals0,
                token1_address: invest1_before / 10 ** decimals1,
            }
            self.current_invest_in_usd = current_invest_in_usd
            self.apr_in_month = apr
            self.pnl = investment_change_in_usd
            self.fee = fee_change_in_usd
