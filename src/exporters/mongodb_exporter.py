import time
from typing import List

from src.constants.mongo_constants import MongoDBCollections
from src.databases.mongodb_klg import MongoDB
from src.models.loader import Loader
from src.utils.format_utils import filter_string
from src.utils.logger_utils import get_logger

logger = get_logger("MongoDB Exporter")


class MongoDBExporter:
    def __init__(self, db: MongoDB):
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

    def export_contracts(self, data: List[dict], keep_none=True, merge=True):
        for contract in data:
            contract['_id'] = f'{contract["chainId"]}_{contract["address"]}'
        if keep_none:
            self._db.update_contracts(data)
        else:
            self._db.update_docs(MongoDBCollections.smart_contracts, data, merge=merge)

    def export_call_contracts(self, data: List[dict]):
        for relationship in data:
            _from = relationship["_from"].split('/')[-1]
            _to = relationship['_to'].split('/')[-1]
            relationship['_id'] = f'{relationship["type"]}_{_from}_{_to}'
        self._db.update_call_contracts(data)

    def export_wallets(self, data: List[dict], metadata: dict = None):
        """Update newly-appeared wallets"""
        for wallet in data:
            wallet['_id'] = f"{wallet['chainId']}_{wallet['address']}"
            if metadata is not None:
                wallet.update(metadata)

        self._db.update_docs(MongoDBCollections.wallets, data=data, shard_key='address')

    def export_multichain_wallets(self, data: List[dict]):
        for wallet in data:
            wallet['_id'] = wallet['address']
        self._db.update_multichain_wallets(data)

    def export_updated_wallets(self, data: List[dict]):
        """Update existed wallets. Mainly for flagging wallets as 'newElite' or 'elite'"""
        for wallet in data:
            wallet['_id'] = f"{wallet['chainId']}_{wallet['address']}"
        self._db.update_docs(collection_name=MongoDBCollections.wallets, data=data, shard_key='address')

    def export_dex_nfts(self, data: List[dict]):
        for nft in data:
            nft['_id'] = f"{nft['chainId']}_{nft['nftManagerAddress']}_{nft['tokenId']}"
        self._db.update_docs(collection_name=MongoDBCollections.dex_nfts, data=data)

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

    def export_abi(self, data: List[dict]):
        for abi in data:
            abi['_id'] = abi.get('name')
        self._db.update_abi(data)

    ######################
    #      Get data      #
    ######################

    def get_contracts(self, tags: list = None, chain_id: str = None, keys: list = None, projection: list = None):
        return self._db.get_contracts(tags, chain_id, keys, projection=projection)

    def get_wallets(self, addresses: list = None, chain_id: str = None, keys: list = None, projection: list = None):
        if keys is None:
            keys = [f'{chain_id}_{address}' for address in addresses]
        return self._db.get_wallets_by_keys(keys, projection=projection)

    def get_abi(self, abi_names: list):
        return self._db.get_abi(abi_names)

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
