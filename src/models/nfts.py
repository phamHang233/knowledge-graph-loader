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
        self.first_called_at = 0
        self.liquidity_change_logs = {}
        self.fee_change_logs = {}
        self.nft_manager_address = None
        self.fee_30_days_before = {}
        self.wallet = None
        self.last_updated_fee_at = 0

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
            'firstCalledAt': self.first_called_at,
            'liquidityChangeLogs': self.liquidity_change_logs,
            'feeChangeLogs': self.fee_change_logs,
            'nftManagerAddress': self.nft_manager_address,
            "uncollectedFee": self.uncollected_fee,
            "wallet": self.wallet,
            "fee30DaysBefore": self.fee_30_days_before,
            "lastUpdatedFeeAt": self.last_updated_fee_at

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
        self.first_called_at = json_dict.get('firstCalledAt', 0)
        self.nft_manager_address = json_dict.get('nftManagerAddress', "")
        self.pool_address = json_dict.get('poolAddress', "")
        self.wallet = json_dict.get("wallet")
        self.fee_30_days_before = json_dict.get('fee30DaysBefore', {})
        self.last_updated_fee_at = json_dict.get('lastUpdatedFeeAt', 0)
