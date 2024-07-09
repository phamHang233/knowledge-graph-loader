import sys
from typing import List

import pymongo
from pymongo import MongoClient, UpdateOne

from config import DexNFTManagerDBConfig
from src.constants.mongo_constants import DexNFTManagerCollections
from src.utils.dict_utils import flatten_dict, delete_none
from src.utils.logger_utils import get_logger
from src.utils.retry_handler import retry_handler
from src.utils.time_execute_decorator import sync_log_time_exe, TimeExeTag

logger = get_logger('NFTMongoDB')


class NFTMongoDB:
    def __init__(self, connection_url=None, database=DexNFTManagerDBConfig.DATABASE, db_prefix=""):
        if not connection_url:
            connection_url = DexNFTManagerDBConfig.CONNECTION_URL

        self.connection_url = connection_url.split('@')[-1]
        try:
            self.connection = MongoClient(connection_url)
            self.mongo_db = self.connection[database]
        except Exception as e:
            logger.exception(f"Failed to connect to ArangoDB: {connection_url}: {e}")
            sys.exit(1)

        self._wallets_col = self.mongo_db[DexNFTManagerCollections.wallets]
        self._nft_col = self.mongo_db[DexNFTManagerCollections.dex_nfts]
        self.collector_collection = self.mongo_db[DexNFTManagerCollections.collectors]
        self._configs_col = self.mongo_db[DexNFTManagerCollections.configs]
        self._pair_col = self.mongo_db[DexNFTManagerCollections.pairs]
        if db_prefix:
            dex_event_col = db_prefix + "_" + DexNFTManagerCollections.dex_events_etl
        else:
            dex_event_col = DexNFTManagerCollections.dex_events_etl
        self.dex_events_collection = self.mongo_db[dex_event_col]
        # self._create_index()

    #######################
    #       Index         #
    #######################

    def _create_index(self):
        # Wallet index
        wallets_col_indexes = self._wallets_col.index_information()
        if 'wallets_flagged_chainId_index' not in wallets_col_indexes:
            self._wallets_col.create_index(
                [('flagged', pymongo.ASCENDING), ('chainId', pymongo.ASCENDING)],
                name='wallets_flagged_chainId_index', background=True
            )
        if 'wallets_tags_index' not in wallets_col_indexes:
            self._wallets_col.create_index(
                [('tags', pymongo.ASCENDING)],
                name='wallets_tags_index', background=True, sparse=True
            )
        if 'wallets_newElite_chainId_index' not in wallets_col_indexes:
            self._wallets_col.create_index(
                [('newElite', pymongo.ASCENDING), ('chainId', pymongo.ASCENDING)],
                name='wallets_newElite_chainId_index', background=True, sparse=True
            )
        if 'wallets_newTarget_chainId_index' not in wallets_col_indexes:
            self._wallets_col.create_index(
                [('newTarget', pymongo.ASCENDING), ('chainId', pymongo.ASCENDING)],
                name='wallets_newTarget_chainId_index', background=True, sparse=True
            )
        if 'wallets_elite_chainId_index' not in wallets_col_indexes:
            self._wallets_col.create_index(
                [('elite', pymongo.ASCENDING), ('chainId', pymongo.ASCENDING)],
                name='wallets_elite_chainId_index', background=True, sparse=True
            )
        if 'wallets_selective_index' not in wallets_col_indexes:
            self._wallets_col.create_index(
                [('selective', pymongo.ASCENDING)],
                name='wallets_selective_index', background=True, sparse=True
            )

    #######################
    #       Wallet        #
    #######################
    def count_all_wallets(self, filter_):
        return self._wallets_col.count_documents(filter_)

    def get_elite_wallets(self, chain_id: str):
        filter_statement = {
            'elite': True,
            'chainId': chain_id
        }
        projection_statement = {
            'address': 1,
            'chainId': 1
        }
        cursor = self._wallets_col.find(filter=filter_statement, projection=projection_statement, batch_size=1000)
        return cursor

    def get_wallets(self, chain_id=None, batch_idx=None, projection=None, batch_size=50000):
        filter_statement = {}
        if chain_id:
            filter_statement['chainId'] = chain_id
        if batch_idx:
            filter_statement['flagged'] = batch_idx

        projection_statement = self.get_projection_statement(projection)
        if projection_statement:
            cursor = self._wallets_col.find(filter=filter_statement, projection=projection_statement,
                                            batch_size=batch_size)
        else:
            cursor = self._wallets_col.find(filter=filter_statement, batch_size=batch_size)
        return cursor

    def get_wallets_by_keys(self, keys, projection=None):
        projection_statement = self.get_projection_statement(projection)
        filter_statement = {
            "_id": {"$in": keys}
        }
        if projection_statement:
            cursor = self._wallets_col.find(
                filter=filter_statement, projection=projection_statement, batch_size=1000)
        else:
            cursor = self._wallets_col.find(
                filter=filter_statement, batch_size=1000)
        return cursor

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_wallets_by_addresses(self, addresses, chain_id=None, projection=None):
        projection_statement = self.get_projection_statement(projection)
        try:
            if not chain_id:
                filter_statement = {"address": {"$in": addresses}}
            else:
                filter_statement = {"_id": {"$in": [f'{chain_id}_{address}' for address in addresses]}}

            cursor = self._wallets_col.find(filter=filter_statement, projection=projection_statement, batch_size=1000)
            return list(cursor)
        except Exception as ex:
            logger.exception(ex)
        return None

    def get_wallet_by_address(self, address, chain_id=None, projection=None):
        projection_statement = self.get_projection_statement(projection)
        try:
            if not chain_id:
                filter_statement = {"address": address}
            else:
                filter_statement = {"_id": f'{chain_id}_{address}'}

            cursor = self._wallets_col.find(filter=filter_statement, projection=projection_statement, batch_size=1000)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None

    def get_wallets_addresses(self, label: str, chain_id: str = '0x38',
                              batch_size: int = 1000):  # return cursor of obj not str
        """Get addresses with specific label (elite, target, new_elite, new_target)
        """
        try:
            filter_statement = {
                label: True,
                'chainId': chain_id
            }
            projection_statement = {
                'address': 1
            }
            cursor = self._wallets_col.find(
                filter=filter_statement, projection=projection_statement, batch_size=batch_size)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_wallets_addresses_with_flags(
            self, label: str = None, chain_id: str = '0x38', batch_size: int = 1000,
            flag: int = 1, projection=None):  # return cursor of obj not str
        """
        Get addresses with specific label (elite, target, new_elite, new_target)
        """

        filter_statement = {
            'chainId': chain_id,
            'flagged': flag
        }
        if label:
            filter_statement[label] = True
        projection_statement = {'address': 1}
        projection_statement.update(self.get_projection_statement(projection))

        cursor = self._wallets_col.find(
            filter=filter_statement, projection=projection_statement, batch_size=batch_size)
        return cursor

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_wallet_data_with_flags(
            self, chain_id: str = '0x38', batch_size: int = 1000,
            flag: int = 1):  # return cursor of obj not str
        """
        Get addresses with specific label (elite, target, new_elite, new_target)
        """

        filter_statement = {
            'chainId': chain_id,
            'flagged': flag
        }

        cursor = self._wallets_col.find(
            filter=filter_statement, batch_size=batch_size)
        return cursor

    def get_fix_wallets_addresses_with_limit(self, label: str, chain_id: str = '0x38',
                                             limit: int = 1000):  # return cursor of obj not str
        """Get addresses with specific label (elite, target, new_elite, new_target)
        """
        try:
            filter_statement = {
                'chainId': chain_id,
                label: False
            }
            projection_statement = {
                'address': 1
            }
            cursor = self._wallets_col.find(
                filter=filter_statement, projection=projection_statement).limit(limit)
            return list(cursor)
        except Exception as ex:
            logger.exception(ex)
        return None

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_balance_change_log_wallets_addresses(self, label: str, chain_id: str = '0x38', flag: int = 1):
        """Get addresses with specific label (elite, target, new_elite, new_target)
        """
        try:
            filter_statement = {
                label: True,
                'chainId': chain_id,
                'flagged': flag
            }
            projection_statement = {
                'address': 1,
                'balanceChangeLogs': 1
            }
            cursor = self._wallets_col.find(
                filter=filter_statement, projection=projection_statement, batch_size=1000)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_wallets_addresses_with_limit(self, label: str, chain_id: str = '0x38', limit: int = 1000):
        """Get addresses with specific label (elite, target, new_elite, new_target)
        """
        try:
            filter_statement = {
                label: True,
                'chainId': chain_id
            }
            projection_statement = {
                'address': 1
            }
            cursor = self._wallets_col.find(
                filter=filter_statement, projection=projection_statement).limit(limit)

            return [item["address"] for item in cursor]
        except Exception as ex:
            logger.exception(ex)
        return None

    def get_wallets_by_list_keys(self, keys: list):
        """Get addresses with list key
        """
        try:
            filter_statement = {
                "_id": {"$in": keys}
            }
            cursor = self._wallets_col.find(filter=filter_statement, batch_size=1000)
            return list(cursor)
        except Exception as ex:
            logger.exception(ex)
        return None

    def get_wallets_token_balance(self, wallet_addresses: list, chain_id: str = '0x38'):
        """Get addresses with specific label (elite, target, new_elite, new_target)
        """
        try:
            list_keys = [f"{chain_id}_{wallet_address}" for wallet_address in wallet_addresses]
            filter_statement = {
                "_id": {"$in": list_keys}
            }
            projection_statement = {
                'address': 1,
                "createdAt": 1,
                "tokenChangeLogs": 1,
                "_id": 0
            }
            cursor = self._wallets_col.find(
                filter=filter_statement, projection=projection_statement)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None

    def get_wallets_addresses_with_timestamp(self, label: str, chain_id: str = '0x38', timestamp: int = 0):
        """Get addresses with timestamp in balanceChangeLogs
        """
        try:
            filter_statement = {
                label: True,
                'chainId': chain_id,
                f'balanceChangeLogs.{timestamp}': {"$exists": True}
            }
            projection_statement = {
                'address': 1,
            }
            cursor = self._wallets_col.find(
                filter=filter_statement, projection=projection_statement)
            return [item["address"] for item in cursor]
        except Exception as ex:
            logger.exception(ex)
        return None

    def get_wallet_with_filter(self, filter_=None, projection=None):
        if filter_ is None:
            filter_ = {}
        if not projection:
            return self._wallets_col.find(filter_)
        return self._wallets_col.find(filter_, projection)

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_lending_wallets(self, chain_id: str, deposit_threshold: float = 0, projection=None, batch_size: int = 1000):
        _filter = {
            'chainId': chain_id,
            'depositInUSD': {'$gt': deposit_threshold}
        }

        cursor = self._wallets_col.find(_filter, projection).batch_size(batch_size=batch_size)
        return cursor

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_wallets_by_tags(self, tags, chain_id=None, projection=None, batch_size=10000):
        _filter = {
            'tags': {'$in': tags}
        }
        if chain_id is not None:
            _filter['chainId'] = chain_id

        cursor = self._wallets_col.find(_filter, projection=projection).batch_size(batch_size=batch_size)
        return cursor

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_error_wallets_with_flags(self, chain_id, flag_idx, projection=None, batch_size=10000):
        try:
            filter_statement = {
                "flagged": flag_idx,
                "chainId": chain_id,
                "balanceChangeLogs": {"$exists": True}
            }
            projection_statement = self.get_projection_statement(projection)
            cursor = self._wallets_col.find(
                filter_statement,
                projection=projection_statement,
                batch_size=batch_size
            )
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None

    @retry_handler
    def update_wallets(self, data):
        try:
            bulk_operations = [UpdateOne(
                {"_id": item["_id"], 'address': item['address']},
                {"$set": flatten_dict(item)},
                upsert=True
            ) for item in data]
            self._wallets_col.bulk_write(bulk_operations)
        except Exception as e:
            logger.error(f"Err: {e}")
            logger.info("Export each feature!")
            for item in data:
                bulk_operations = []
                flatten_wallet = flatten_dict(item)
                for key in item:
                    flatten_data = {flatten_key: value for flatten_key, value in flatten_wallet.items()
                                    if key in flatten_key}
                    if not flatten_data:
                        continue
                    bulk_operations.append(
                        UpdateOne({"_id": item["_id"], "address": item["address"]},
                                  {"$set": flatten_data}, upsert=True)
                    )
                self._wallets_col.bulk_write(bulk_operations)

    @retry_handler
    def update_wallets_without_flatten(self, data):
        bulk_operations = [UpdateOne(
            {"_id": item["_id"], 'address': item['address']},
            {"$set": item},
            upsert=True
        ) for item in data]
        self._wallets_col.bulk_write(bulk_operations)

    @retry_handler
    def update_wallets_transaction(self, data, chain_id):
        bulk_operations = []

        for key, value in data.items():
            bulk_operations.append(
                UpdateOne({"_id": f'{chain_id}_{key}', 'address': key}, {"$set": value}, upsert=True)
            )

        self._wallets_col.bulk_write(bulk_operations)

    @sync_log_time_exe(tag=TimeExeTag.database)
    def quick_update_wallets(self, data: list):
        bulk_operations = [UpdateOne({"_id": item["$set"]["_id"], 'address': item['$set']['address']}, item, upsert=True) for item in data]
        self._wallets_col.bulk_write(bulk_operations)

    def count_wallet_by_filter(self, filter_):
        return self._wallets_col.count_documents(filter_)

    ######################
    #      Configs       #
    ######################

    def get_config(self, key):
        try:
            config = self._configs_col.find_one({'_id': key})
            return config
        except Exception as ex:
            logger.exception(ex)
        return None

    def get_configs(self, filter_=None):
        if filter_ is None:
            filter_ = {}
        try:
            config = self._configs_col.find(filter_)
            return config
        except Exception as ex:
            logger.exception(ex)
        return None

    def update_config(self, config, merge=True):
        try:
            if merge:
                bulk_operations = [UpdateOne({"_id": config["_id"]}, {"$set": flatten_dict(config)}, upsert=True)]
            else:
                bulk_operations = [UpdateOne({"_id": config["_id"]}, {"$set": config}, upsert=True)]
            self._configs_col.bulk_write(bulk_operations)
        except Exception as ex:
            logger.exception(ex)

    def update_configs(self, configs, merge=True):
        try:
            if merge:
                bulk_operations = [UpdateOne({"_id": config["_id"]}, {"$set": flatten_dict(config)}, upsert=True) for config in configs]
            else:
                bulk_operations = [UpdateOne({"_id": config["_id"]}, {"$set": config}, upsert=True) for config in configs]
            self._configs_col.bulk_write(bulk_operations)
        except Exception as ex:
            logger.exception(ex)

    def get_wallet_flagged_state(self, chain_id=None):
        if chain_id is None:
            key = 'multichain_wallets_flagged_state'
        else:
            key = f'wallets_flagged_state_{chain_id}'
        filter_statement = {
            "_id": key
        }
        config = self._configs_col.find_one(filter_statement)
        if not config:
            return None
        return config

    def get_nft_flagged_state(self, chain_id=None):
        key = f'nfts_flagged_state_{chain_id}'
        filter_statement = {
            "_id": key
        }
        config = self._configs_col.find_one(filter_statement)
        if not config:
            return None
        return config

    def get_nfts_by_filter(self, _filter, projection=None):
        cursor = self._nft_col.find(_filter, projection=projection)
        return cursor

    def get_top_nfts_of_pair(self, pair_address, projection=None):
        try:
            cursor = (self._nft_col.find({'poolAddress': pair_address, 'aprInMonth':{"$gt": 0}}, projection=projection)
                      .sort("aprInMonth", pymongo.DESCENDING).limit(1))
            return cursor[0]
        except:
            return None

    def get_nfts_by_keys(self, keys):
        cursor = self._nft_col.find({"_id": {"$in": keys}})
        return cursor
    def get_new_wallet_by_flags_config(self, chain_id):
        filter_statement = {
            "_id": f'wallet_flags_{chain_id}'
        }
        projection_statement = {
            "newElite": 1,
            "newTarget": 1,
            "_id": 0
        }
        doc = self._configs_col.find_one(filter_statement, projection_statement)
        if not doc:
            return None
        return doc

    def get_new_flag_wallet_in_flags_config(self, chain_id, flag):
        filter_statement = {
            "_id": f'wallet_flags_{chain_id}'
        }
        projection_statement = {
            flag: 1,
            "_id": 0
        }
        doc = self._configs_col.find_one(filter_statement, projection_statement)
        if not doc:
            return None
        return doc

    @staticmethod
    def get_projection_statement(projection: list = None):
        if projection is None:
            return {}

        projection_statements = {}
        for field in projection:
            projection_statements[field] = True

        return projection_statements

    #######################
    #       Common        #
    #######################
    def get_docs(self, collection, keys: list = None, filter_: dict = None, batch_size=1000,
                 projection=None):  # change filter_ to obj

        filter_statement = {}
        if keys:
            filter_statement["_id"] = {"$in": keys}
        if filter_ is not None:
            filter_statement.update(filter_)
        if projection:
            projection_statement = self.get_projection_statement(projection)
            cursor = self.mongo_db[collection].find(
                filter=filter_statement, projection=projection_statement, batch_size=batch_size)
        else:
            cursor = self.mongo_db[collection].find(
                filter=filter_statement, batch_size=batch_size)
        return cursor

    def get_doc(self, collection, key: str = None, filter_: dict = None, batch_size=1000, projection=None):  # change filter_ to obj
        filter_statement = {}
        if key:
            filter_statement["_id"] = key
        if filter_ is not None:
            filter_statement.update(filter_)
        if projection:
            projection_statement = self.get_projection_statement(projection)
            cursor = self.mongo_db[collection].find_one(
                filter=filter_statement, projection=projection_statement, batch_size=batch_size)
        else:
            cursor = self.mongo_db[collection].find_one(
                filter=filter_statement, batch_size=batch_size)
        return cursor

    def get_docs_with_db(self, db, collection, keys: list = None, filter_: dict = None, batch_size=1000,
                         projection=None):  # change filter_ to obj
        projection_statement = self.get_projection_statement(projection)

        filter_statement = {}
        if keys:
            filter_statement["_id"] = {"$in": keys}
        if filter_ is not None:
            filter_statement.update(filter_)

        cursor = self.connection[db][collection].find(
            filter=filter_statement, projection=projection_statement, batch_size=batch_size)
        return cursor

    def update_docs(self, collection_name, data, keep_none=False, merge=True, shard_key=None, flatten=True):
        """If merge is set to True => sub-dictionaries are merged instead of overwritten"""
        col = self.mongo_db[collection_name]
        # col.insert_many(data, overwrite=True, overwrite_mode='update', keep_none=keep_none, merge=merge)
        bulk_operations = []
        if not flatten:
            if not shard_key:
                bulk_operations = [UpdateOne({"_id": item["_id"]}, {"$set": item}, upsert=True) for item in data]
            else:
                bulk_operations = [UpdateOne({"_id": item["_id"], shard_key: item[shard_key]}, {"$set": item}, upsert=True) for item in data]
            col.bulk_write(bulk_operations)
            return

        for document in data:
            unset, set_, add_to_set = self.create_update_doc(document, keep_none, merge, shard_key)
            if not shard_key:
                bulk_operations += [
                    UpdateOne({"_id": item["_id"]},
                              {"$unset": {key: value for key, value in item.items() if key != "_id"}}, upsert=True)
                    for item in unset]
                bulk_operations += [
                    UpdateOne({"_id": item["_id"]},
                              {"$set": {key: value for key, value in item.items() if key != "_id"}}, upsert=True)
                    for item in set_]
                bulk_operations += [
                    UpdateOne({"_id": item["_id"]},
                              {"$addToSet": {key: value for key, value in item.items() if key != "_id"}},
                              upsert=True)
                    for item in add_to_set]
            if shard_key:
                keys = ["_id", shard_key]
                bulk_operations += [
                    UpdateOne({"_id": item["_id"], shard_key: item[shard_key]},
                              {"$unset": {key: value for key, value in item.items() if key not in keys}},
                              upsert=True)
                    for item in unset]
                bulk_operations += [
                    UpdateOne({"_id": item["_id"], shard_key: item[shard_key]},
                              {"$set": {key: value for key, value in item.items() if key not in keys}}, upsert=True)
                    for item in set_]
                bulk_operations += [
                    UpdateOne({"_id": item["_id"], shard_key: item[shard_key]},
                              {"$addToSet": {key: value for key, value in item.items() if key not in keys}},
                              upsert=True)
                    for item in add_to_set]
        col.bulk_write(bulk_operations)

    def remove_out_date_docs(self, collection_name, timestamp, filter_: dict = None):  # change filter to dict
        filter_statement = {
            "lastUpdatedAt": {"$lt": timestamp}
        }
        if filter_ is not None:
            filter_statement.update(filter_)

        self.mongo_db[collection_name].delete_many(filter_statement)

    def remove_docs_by_keys(self, collection_name, keys):
        filter_statement = {
            "_id": {"$in": keys}
        }
        self.mongo_db[collection_name].delete_many(filter_statement)

    @staticmethod
    def create_update_doc(document, keep_none=False, merge=True, shard_key=None):
        unset, set_, add_to_set = [], [], []
        if not keep_none:
            doc = flatten_dict(document)
            for key, value in doc.items():
                if value is None:
                    tmp = {
                        "_id": document["_id"],
                        key: ""
                    }
                    if shard_key:
                        tmp[shard_key] = document[shard_key]
                    unset.append(tmp)
                    continue
                if not merge:
                    continue
                if isinstance(value, list):
                    tmp = {
                        "_id": document["_id"],
                        key: {"$each": [i for i in value if i]}
                    }
                    if shard_key:
                        tmp[shard_key] = document[shard_key]
                    add_to_set.append(tmp)
                else:
                    tmp = {
                        "_id": document["_id"],
                        key: value
                    }
                    if shard_key:
                        tmp[shard_key] = document[shard_key]
                    set_.append(tmp)

        if not merge:
            if keep_none:
                set_.append(document)
            else:
                set_.append(delete_none(document))

        return unset, set_, add_to_set

    def delete_documents(self, collection, filter_):
        self.mongo_db[collection].delete_many(filter_)

    @staticmethod
    def set_flatten_operation(bulk_operations, item, flatten_item, shard_key=None):
        for key in item:
            data = {flatten_key: value for flatten_key, value in flatten_item.items() if key in flatten_key}
            if not data:
                continue

            filter_statement = {"_id": item["_id"]}
            if shard_key is not None:
                filter_statement.update({shard_key: item[shard_key]})

            bulk_operations.append(UpdateOne(filter_statement, {"$set": data}, upsert=True))

    @staticmethod
    def unset_flatten_operation(bulk_operations, item, flatten_item, shard_key=None):
        for key in item:
            data = {flatten_key: value for flatten_key, value in flatten_item.items() if key in flatten_key}
            if not data:
                continue

            if key in ["_id", shard_key]:
                continue

            filter_statement = {"_id": item["_id"]}
            if shard_key is not None:
                filter_statement.update({shard_key: item[shard_key]})

            bulk_operations.append(UpdateOne(filter_statement, {"$unset": data}, upsert=True))

    #######################
    #       NFT        #
    #######################

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_all_nfts(self, _filter):
        return self._nft_col.find(_filter).batch_size(10000)

    def get_nft_info(self, nfts):
        return self._nft_col.find({"_id": {"$in": nfts}}).batch_size(1000)

    def get_last_block_number(self, collector_id="streaming_collector"):
        """Get the last block number collected by collector"""
        last_block_number = self.collector_collection.find_one({"_id": collector_id})
        return last_block_number["last_updated_at_block_number"]

    #######################
    #       Events        #
    #######################

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_dex_events_in_block_range(self, start_block, end_block, event_types=None, projection=None):
        filter_ = {
            'block_number': {'$gte': start_block, '$lte': end_block}
        }
        if event_types is not None:
            filter_["event_type"] = {"$in": event_types}

        try:
            cursor = self.dex_events_collection.find(filter_, projection=projection)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    def get_nft_with_flagged(self, batch_size=50000, chain_id=None, reset=False):
        if not reset:
            filter_statement = {'flagged': {'$exists': False}}
        else:
            filter_statement = {}
        if chain_id:
            filter_statement['chainId'] = chain_id

        cursor = self._nft_col.find(filter_statement, batch_size=batch_size)

        return cursor

    @sync_log_time_exe(tag=TimeExeTag.database)
    @retry_handler
    def update_nfts(self, nfts: List[dict]):
        for nft in nfts:
            nft["_id"] = f'{nft.get("chainId")}_{nft.get("nftManagerAddress")}_{nft.get("tokenId")}'

        bulk_operations = [UpdateOne(
            {"_id": item["_id"]},
            {"$set": flatten_dict(item)},
            upsert=True
        ) for item in nfts]
        self._nft_col.bulk_write(bulk_operations)

    ######################
    #        PAIR        #
    ######################

    def get_pairs(self):
        cursor = self._pair_col.find({'bestAPR': {"$exists": True}})
        return cursor

    def replace_pairs(self, data):
        for item in data:
            self._pair_col.replace_one({"_id": item["_id"]}, item, upsert=True)


    def replace_wallets(self, data):
        for item in data:
            self._wallets_col.replace_one({"_id": item["_id"]}, item, upsert=True)
