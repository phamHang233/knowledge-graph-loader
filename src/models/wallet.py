from src.models.nfts import NFT


class Wallet:
    def __init__(self, address, chain_id):
        self.address = address
        self.chain_id = chain_id
        self.nfts = []
        self.total_liquidity = 0
        self.tokens = {}
        self.apr = 0
        self.total_asset = 0
        self.pnl = 0

    def from_dict(self, doc):
        self.nfts = doc.get('nfts', [])

    def to_dict(self):
        return {
            'address': self.address,
            'chainId': self.chain_id,
            'nfts': self.nfts,
            'apr': self.apr,
            'totalAsset': self.total_asset,
            'PnL': self.pnl
        }
