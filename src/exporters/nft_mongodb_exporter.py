import time
from typing import List

from src.constants.mongo_constants import DexNFTManagerCollections, MongoDBCollections
from src.databases.dex_nft_manager_db import NFTMongoDB
from src.models.loader import Loader
from src.utils.format_utils import filter_string
from src.utils.logger_utils import get_logger

logger = get_logger("MongoDB Exporter")


class NFTMongoDBExporter:
    def __init__(self, db: NFTMongoDB):
        self._db = db

    ######################
    #    Export data     #
    ######################

    # def export_tokens(self, data: List[dict]):
    #     # fiter none fields
    #     exported_data = list()
    #     for coin_data in data:
    #         coin_data['_id'] = f'{coin_data["chainId"]}_{coin_data["address"]}'
    #         coin_data['tags'] = {Tags.token: 1}
    #         exported_data.append(filter_none_keys(coin_data))
    #     self._db.update_docs(MongoDBCollections.smart_contracts, data=exported_data)

    def export_projects(self, data: List[dict]):
        for project in data:
            project['_id'] = filter_string(project['id'])
        self._db.update_docs(MongoDBCollections.projects, data=data)

    def export_wallets(self, data: List[dict], metadata: dict = None):
        """Update newly-appeared wallets"""
        for wallet in data:
            wallet['_id'] = f"{wallet['chainId']}_{wallet['address']}"
            if metadata is not None:
                wallet.update(metadata)

        self._db.update_docs(MongoDBCollections.wallets, data=data, shard_key='address')

    def export_updated_wallets(self, data: List[dict]):
        """Update existed wallets. Mainly for flagging wallets as 'newElite' or 'elite'"""
        for wallet in data:
            wallet['_id'] = f"{wallet['chainId']}_{wallet['address']}"
        self._db.update_docs(collection_name=MongoDBCollections.wallets, data=data, shard_key='address')

    def export_dex_nfts(self, data: List[dict]):
        for nft in data:
            nft['_id'] = f"{nft['chainId']}_{nft['nftManagerAddress']}_{nft['tokenId']}"
        self._db.update_docs(collection_name=DexNFTManagerCollections.dex_nfts, data=data)

    def export_pairs(self, data: List[dict]):
        self._db.update_docs(collection_name=DexNFTManagerCollections.pairs, data=data)

    def export_tokens_holders(self, data: List[dict]):
        """Update top holders for top tokens
        data schema: [{'chain_id': , 'address': , 'top_holders': }]"""
        exported_data = [
            {
                "_id": f"{token_datum['chain_id']}_{token_datum['address']}",
                "topHolders": token_datum['top_holders'],
                "lastUpdatedAt": int(time.time())
            }
            for token_datum in data
        ]
        self._db.update_docs(collection_name=MongoDBCollections.smart_contracts, data=exported_data, merge=False)

    def export_number_calls_top_contracts(self, data: List[dict]):
        """Export number of times that each wallet call to every top contract"""
        for wallet in data:
            wallet['_id'] = f"{wallet['chainId']}_{wallet['address']}"
        self._db.update_docs(collection_name=MongoDBCollections.wallets, data=data)


    ######################
    #      Get data      #
    ######################

    def get_wallets(self, addresses: list = None, chain_id: str = None, keys: list = None, projection: list = None):
        if keys is None:
            keys = [f'{chain_id}_{address}' for address in addresses]
        return self._db.get_wallets_by_keys(keys, projection=projection)

    def get_nfts(self, nfts):
        return self._db.get_nft_info(nfts)

    ######################
    #   Relationships    #
    ######################

    # def export_is_part_ofs(self, addresses, chain_id):
    #     data = []
    #     for address in addresses:
    #         key = f'{chain_id}_{address}'
    #         data.append({
    #             '_id': key,
    #             '_from': f'{ArangoDBCollections.wallets}/{key}',
    #             '_to': f'{ArangoDBCollections.multichain_wallets}/{address}'
    #         })
    #     self._db.update_is_part_ofs(data)

    ######################
    #      Configs       #
    ######################

    def export_config(self, config):
        config['_id'] = config.pop('id')
        self._db.update_config(config)

    def get_loader(self, key: str) -> Loader:
        data = self._db.get_config(key)
        if not data:
            return Loader(_id=key)

        loader = Loader()
        loader.from_dict(data)
        return loader

    def update_loader(self, loader: Loader) -> None:
        data = loader.to_dict()
        data['_id'] = loader.id
        self._db.update_config(data)

    def update_top_tokens_config(self, tokens, chain):
        config = {
            '_id': f'top_tokens_{chain}',
            'tokens': tokens,
            'lastUpdatedAt': int(time.time())
        }
        self._db.update_config(config)

    def export_create_balance_config(self, chain_id, timestamp):
        export_data = {
            "_id": f"create_balance_table_{chain_id}",
            "timestamp": timestamp
        }
        self._db.update_config(export_data)

    def removed_docs(self, collection_name, keys):
        self._db.remove_docs_by_keys(collection_name, keys)

    def get_config(self, keys):
        return self._db.get_config(keys)
