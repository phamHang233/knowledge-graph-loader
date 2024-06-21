import itertools
import time

from query_state_lib.base.mappers.eth_call_balance_of_mapper import EthCallBalanceOf
from query_state_lib.base.mappers.get_balance_mapper import GetBalance
from query_state_lib.client.client_querier import ClientQuerier
from web3 import Web3, HTTPProvider
from web3.middleware import geth_poa_middleware

from artifacts.abis.dexes.uniswap_v3_nft_manage_abi import UNISWAP_V3_NFT_MANAGER_ABI
from artifacts.abis.dexes.uniswap_v3_pool_abi import UNISWAP_V3_POOL_ABI
from artifacts.abis.erc20_abi import ERC20_ABI
from artifacts.abis.dexes.uniswap_v3_factory_abi import UNISWAP_V3_FACTORY_ABI
from src.constants.network_constants import NATIVE_TOKEN
from src.services.blockchain.batch_queries_service import add_rpc_call, decode_data_response_ignore_error, \
    decode_data_response
from src.services.blockchain.multicall import W3Multicall, add_rpc_multicall, decode_multical_response
from src.utils.logger_utils import get_logger

logger = get_logger('State Query Service')


class StateQueryService:
    def __init__(self, provider_uri):
        self._w3 = Web3(HTTPProvider(provider_uri))
        self._w3.middleware_onion.inject(geth_poa_middleware, layer=0)

        self.client_querier = ClientQuerier(provider_url=provider_uri)

    def to_checksum(self, address):
        return self._w3.to_checksum_address(address)

    def get_contract(self, address, abi):
        try:
            return self._w3.eth.contract(self.to_checksum(address), abi=abi)
        except TypeError:
            print('a')

    def decode_tx(self, address, abi, tx_input):
        contract = self.get_contract(address, abi)
        func_obj, func_params = contract.decode_function_input(tx_input)
        return func_obj, func_params

    def is_address(self, address):
        return self._w3.is_address(address)

    def balance_of(self, address, token, abi=ERC20_ABI, block_number='latest'):
        token_contract = self._w3.eth.contract(self._w3.to_checksum_address(token), abi=abi)
        decimals = token_contract.functions.decimals().call()
        balance = token_contract.functions.balanceOf(self._w3.to_checksum_address(address)).call(
            block_identifier=block_number)
        return balance / 10 ** decimals

    def batch_balance_of(self, address, tokens, block_number='latest', batch_size=100):
        balances = {}
        tokens = {token['address']: token['decimals'] or 18 for token in tokens}
        rpc_requests = []
        for token in tokens:
            if token != "0x" and token != NATIVE_TOKEN:
                query_id = f'{address}_{token}'
                call_balance_of = EthCallBalanceOf(
                    contract_address=Web3.to_checksum_address(token),
                    address=Web3.to_checksum_address(address),
                    block_number=block_number,
                    id=query_id
                )
            else:
                query_id = f"{address}_{NATIVE_TOKEN}"
                call_balance_of = GetBalance(
                    address=Web3.to_checksum_address(address),
                    block_number=block_number,
                    id=query_id
                )
            rpc_requests.append(call_balance_of)
        rpc_responses = self.client_querier.sent_batch_to_provider(rpc_requests, batch_size=batch_size)
        for token, decimals in tokens.items():
            if token == '0x':
                token = NATIVE_TOKEN
            balance = rpc_responses.get(f'{address}_{token}').result or 0
            balance = balance / 10 ** decimals
            balances[token] = balance

        return balances

    def batch_balance_of_multiple_addresses(self, addresses, tokens, block_number='latest', batch_size=100):
        tokens_decimals = {token['address']: token.get('decimals') or 18 for token in tokens}

        rpc_requests = []
        for address, interacted_tokens in addresses.items():
            for token_address in interacted_tokens:
                if token_address != "0x" and token_address != NATIVE_TOKEN:
                    query_id = f'{address}_{token_address}'
                    call_balance_of = EthCallBalanceOf(
                        contract_address=Web3.to_checksum_address(token_address),
                        address=Web3.to_checksum_address(address),
                        block_number=block_number,
                        id=query_id
                    )
                else:
                    query_id = f"{address}_{NATIVE_TOKEN}"
                    call_balance_of = GetBalance(
                        address=Web3.to_checksum_address(address),
                        block_number=block_number,
                        id=query_id
                    )
                rpc_requests.append(call_balance_of)

        rpc_responses = self.client_querier.sent_batch_to_provider(rpc_requests, batch_size=batch_size)

        results = {}
        for address, interacted_tokens in addresses.items():
            results[address] = {}
            for token_address in interacted_tokens:
                balance = rpc_responses.get(f'{address}_{token_address}').result or 0

                decimals = tokens_decimals.get(token_address) or 18
                balance = balance / 10 ** decimals
                results[address][token_address] = balance

        return results, len(rpc_requests)

    def batch_native_balance_of_wallets(self, addresses, blocks, decimals=18, batch_size=100):
        rpc_requests = []
        for address in addresses:
            for block_number in blocks:
                query_id = f"{address}_{block_number}"
                call_balance_of = GetBalance(
                    address=Web3.to_checksum_address(address),
                    block_number=block_number,
                    id=query_id
                )
                rpc_requests.append(call_balance_of)

        rpc_responses = self.client_querier.sent_batch_to_provider(rpc_requests, batch_size=batch_size)

        data = {}
        for address in addresses:
            data[address] = {}
            for block_number in blocks:
                balance = rpc_responses.get(f'{address}_{block_number}').result or 0
                balance = balance / 10 ** decimals
                data[address][block_number] = balance

        return data

    def batch_balance_of_wallets(self, addresses, tokens, blocks, batch_size=100):
        rpc_requests = []

        for address, token, block_number in itertools.product(addresses, tokens, blocks):
            token_address = token['address']
            query_id = f'{address}_{token_address}_{block_number}'
            if token_address != "0x" and token_address != NATIVE_TOKEN:
                call_balance_of = EthCallBalanceOf(
                    contract_address=Web3.to_checksum_address(token_address),
                    address=Web3.to_checksum_address(address),
                    block_number=block_number,
                    id=query_id
                )
            else:
                call_balance_of = GetBalance(
                    address=Web3.to_checksum_address(address),
                    block_number=block_number,
                    id=query_id
                )
            rpc_requests.append(call_balance_of)

        rpc_responses = self.client_querier.sent_batch_to_provider(rpc_requests, batch_size=batch_size)

        data = {}
        for address, token, block_number in itertools.product(addresses, tokens, blocks):
            token_address = token['address']
            query_id = f'{address}_{token_address}_{block_number}'
            balance = rpc_responses.get(query_id).result or 0

            decimals = token.get('decimals') or 18

            if address not in data:
                data[address] = {}
            if token_address not in data[address]:
                data[address][token_address] = {}
            data[address][token_address][block_number] = balance / 10 ** decimals

        return data

    def batch_balance_of_wallets_block(self, addresses, tokens, block_number='latest', batch_size=100):
        rpc_requests = []
        for address, token in itertools.product(addresses, tokens):
            token_address = token['address']
            query_id = f'{address}_{token_address}_{block_number}'
            if token_address != "0x" and token_address != NATIVE_TOKEN:
                call_balance_of = EthCallBalanceOf(
                    contract_address=Web3.to_checksum_address(token_address),
                    address=Web3.to_checksum_address(address),
                    block_number=block_number,
                    id=query_id
                )
            else:
                call_balance_of = GetBalance(
                    address=Web3.to_checksum_address(address),
                    block_number=block_number,
                    id=query_id
                )
            rpc_requests.append(call_balance_of)

        rpc_responses = self.client_querier.sent_batch_to_provider(rpc_requests, batch_size=batch_size)

        data = {}
        for address, token in itertools.product(addresses, tokens):
            token_address = token['address']
            query_id = f'{address}_{token_address}_{block_number}'
            balance = rpc_responses.get(query_id).result or 0
            decimals = token.get('decimals') or 18
            if address not in data:
                data[address] = {}
            if token_address not in data[address]:
                data[address][token_address] = {}
            data[address][token_address] = balance / 10 ** decimals

        return data

    def batch_balance_of_wallet_info(self, wallet_info: dict, block_number='latest', batch_size=100):
        rpc_requests = []
        for address, tokens in wallet_info.items():
            for token in tokens:
                token_address = token['address']
                query_id = f'{address}_{token_address}_{block_number}'
                if token_address != "0x" and token_address != NATIVE_TOKEN:
                    call_balance_of = EthCallBalanceOf(
                        contract_address=Web3.to_checksum_address(token_address),
                        address=Web3.to_checksum_address(address),
                        block_number=block_number,
                        id=query_id
                    )
                else:
                    call_balance_of = GetBalance(
                        address=Web3.to_checksum_address(address),
                        block_number=block_number,
                        id=query_id
                    )
                rpc_requests.append(call_balance_of)

        rpc_responses = self.client_querier.sent_batch_to_provider(rpc_requests, batch_size=batch_size)

        data = {}
        for address, tokens in wallet_info.items():
            for token in tokens:
                token_address = token['address']
                query_id = f'{address}_{token_address}_{block_number}'
                balance = rpc_responses.get(query_id).result or 0
                decimals = token.get('decimals') or 18
                if address not in data:
                    data[address] = {}
                if token_address not in data[address]:
                    data[address][token_address] = {}
                data[address][token_address] = balance / 10 ** decimals

        return data

    def batch_balance_of_wallet_info_with_block_number(self, wallets_info: list, batch_size=100):
        rpc_requests = []
        for wallet_info in wallets_info:
            address = wallet_info['address']
            tokens = wallet_info.get('tokens', [])
            block_number = wallet_info.get('block_number', 'latest')

            for token in tokens:
                token_address = token['address']
                query_id = f'{address}_{token_address}_{block_number}'
                if token_address != "0x" and token_address != NATIVE_TOKEN:
                    call_balance_of = EthCallBalanceOf(
                        contract_address=Web3.to_checksum_address(token_address),
                        address=Web3.to_checksum_address(address),
                        block_number=block_number,
                        id=query_id
                    )
                else:
                    call_balance_of = GetBalance(
                        address=Web3.to_checksum_address(address),
                        block_number=block_number,
                        id=query_id
                    )
                rpc_requests.append(call_balance_of)

        rpc_responses = self.client_querier.sent_batch_to_provider(rpc_requests, batch_size=batch_size)

        data = {}
        for wallet_info in wallets_info:
            address = wallet_info['address']
            tokens = wallet_info.get('tokens', [])
            block_number = wallet_info.get('block_number', 'latest')

            for token in tokens:
                token_address = token['address']
                query_id = f'{address}_{token_address}_{block_number}'
                balance = rpc_responses.get(query_id).result or 0
                decimals = token.get('decimals')
                if decimals is None:
                    continue

                if address not in data:
                    data[address] = {}
                if token_address not in data[address]:
                    data[address][token_address] = {}
                data[address][token_address] = balance / 10 ** decimals

        return data

    def batch_liquidity_pools_info(self, liquidity_pools, batch_size=100):
        list_rpc_call = []
        list_call_id = []
        for liquidity_pool in liquidity_pools:
            address = liquidity_pool['address']
            block_number = liquidity_pool['block_number']

            for token in liquidity_pool.get('tokens', []):
                add_rpc_call(
                    abi=ERC20_ABI, contract_address=Web3.to_checksum_address(token['address']),
                    fn_name="decimals", block_number=block_number,
                    list_call_id=list_call_id, list_rpc_call=list_rpc_call
                )
                add_rpc_call(
                    abi=ERC20_ABI, contract_address=Web3.to_checksum_address(token['address']),
                    fn_name="symbol", block_number=block_number,
                    list_call_id=list_call_id, list_rpc_call=list_rpc_call
                )
                add_rpc_call(
                    abi=ERC20_ABI, contract_address=Web3.to_checksum_address(token['address']),
                    fn_name="balanceOf", fn_paras=address, block_number=block_number,
                    list_call_id=list_call_id, list_rpc_call=list_rpc_call
                )

        try:
            responses = self.client_querier.sent_batch_to_provider(list_rpc_call, batch_size=batch_size)
            decoded_data = decode_data_response_ignore_error(data_responses=responses, list_call_id=list_call_id)
        except Exception as ex:
            err_detail = str(ex)
            if err_detail.strip().startswith('Response data err'):
                return
            raise ex

        for liquidity_pool in liquidity_pools:
            address = liquidity_pool['address']
            block_number = liquidity_pool['block_number']

            try:
                for token in liquidity_pool.get('tokens', []):
                    symbol = decoded_data.get(f'symbol_{token["address"]}_{block_number}'.lower())
                    symbol = symbol.upper()
                    decimals = decoded_data.get(f'decimals_{token["address"]}_{block_number}'.lower())

                    liquidity_amount = decoded_data.get(
                        f'balanceOf_{token["address"]}_{address}_{block_number}'.lower())
                    token['liquidityAmount'] = liquidity_amount / 10 ** decimals
                    token['symbol'] = symbol
                    token['decimals'] = decimals

                liquidity_pool['symbol'] = ' - '.join([t['symbol'] for t in liquidity_pool.get('tokens', [])])
            except Exception as ex:
                logger.exception(ex)

                # Add to ivalid liquidity pools
                liquidity_pool.pop('tokens')

        return liquidity_pools

    def get_batch_nft_info_with_block_number(self, missing_nfts, factory_nft_contracts, new_pools, batch_size=100):
        result = {}
        decoded_data = {}
        list_rpc_call = []
        list_call_id = []
        for nft in missing_nfts:
            token_id = nft['token_id']
            # block_number = nft['block_number']
            address = nft['contract_address']
            add_rpc_call(
                abi=UNISWAP_V3_NFT_MANAGER_ABI, contract_address=Web3.to_checksum_address(address),
                fn_name="positions", block_number='latest', fn_paras=int(token_id),
                list_call_id=list_call_id, list_rpc_call=list_rpc_call
            )
            add_rpc_call(
                abi=UNISWAP_V3_NFT_MANAGER_ABI, contract_address=Web3.to_checksum_address(address),
                fn_name="ownerOf", block_number='latest', fn_paras=int(token_id),
                list_call_id=list_call_id, list_rpc_call=list_rpc_call
            )
            factory = factory_nft_contracts.get(address)
            if factory is None:
                add_rpc_call(
                    abi=UNISWAP_V3_NFT_MANAGER_ABI, contract_address=Web3.to_checksum_address(address),
                    fn_name="factory", block_number="latest",
                    list_call_id=list_call_id, list_rpc_call=list_rpc_call
                )

        try:
            responses = self.client_querier.sent_batch_to_provider(list_rpc_call, batch_size=batch_size)
            filtered_call_id = []
            for call_id in list_call_id:
                if responses[call_id].result:
                    filtered_call_id.append(call_id)
                # else:
                #     if responses[call_id].error.get('message') == 'execution reverted: revert: Invalid token ID':
                #         token_id = call_id.split("_")[2]
                #         contract = call_id.split("_")[1]
                #         invalid_tokens.append(f"{contract}_{token_id}")
            if filtered_call_id:
                decoded_data.update(
                    decode_data_response_ignore_error(data_responses=responses, list_call_id=filtered_call_id))
        except Exception as ex:
            logger.exception(f"Exception {ex} when query provider")
        list_rpc_call = []
        list_call_id = []
        for nft in missing_nfts:
            token_id = nft['token_id']
            block_number = nft['block_number']
            address = nft['contract_address']
            position = decoded_data.get(f"positions_{address}_{token_id}_latest".lower())
            wallet = decoded_data.get(f"ownerOf_{address}_{token_id}_latest".lower())
            if not position:
                continue
            if factory_nft_contracts.get(address) is None:
                factory = decoded_data.get(f"factory_{address}_latest".lower())
                factory_nft_contracts[address] = factory
            factory = factory_nft_contracts[address]
            token0 = position[2]
            token1 = position[3]
            fee = position[4]
            tick_lower = position[5]
            tick_upper = position[6]
            liquidity = position[7]
            # fee_growth_inside0 = position[8]
            # fee_growth_inside1 = position[9]
            result[token_id] = {
                'tick_lower': tick_lower,
                'tick_upper': tick_upper,
                'liquidity': float(liquidity),
                # 'fee_growth_inside0': fee_growth_inside0,
                # 'fee_growth_inside1': fee_growth_inside1,
                'last_called_at': block_number,
                'wallet': wallet
            }
            add_rpc_call(
                abi=UNISWAP_V3_FACTORY_ABI, contract_address=Web3.to_checksum_address(factory),
                fn_name="getPool", block_number="latest",
                fn_paras=[Web3.to_checksum_address(token0), Web3.to_checksum_address(token1), fee],
                list_call_id=list_call_id, list_rpc_call=list_rpc_call
            )
        try:
            responses = self.client_querier.sent_batch_to_provider(list_rpc_call, batch_size=batch_size)
            # filtered_call_id = []
            # for call_id in list_call_id:
            #     if decoded_data[call_id].result:
            #         filtered_call_id.append(call_id)
            decoded_data.update(decode_data_response(data_responses=responses, list_call_id=list_call_id))

            for nft in missing_nfts:
                token_id = nft['token_id']
                # block_number = nft['block_number']
                address = nft['contract_address']
                position = decoded_data.get(f"positions_{address}_{token_id}_latest".lower())
                if not position:
                    continue

                factory = factory_nft_contracts[address]
                token0 = position[2]
                token1 = position[3]
                fee = position[4]
                pool = decoded_data.get(f"getPool_{factory}_{[token0, token1, fee]}_latest".lower())
                result[token_id].update({'pool_address': pool})
                new_pools.add(pool)
            return result

        except Exception as ex:
            return {}

    def get_batch_nft_fee_with_current_block(self, nfts, pools, w3_multicall: W3Multicall, block_number='latest', batch_size=2000):
        list_nfts = []
        important_nfts = []
        pools_in_batch = set()
        for nft in nfts:
            w3_multicall.add(
                W3Multicall.Call(address=Web3.to_checksum_address(nft['nftManagerAddress']), block_number=block_number,
                                 abi=UNISWAP_V3_NFT_MANAGER_ABI, fn_name='positions', fn_paras=int(nft['tokenId'])
                                 ))
            list_nfts.append(nft)
        list_call_id, list_rpc_call = [], []
        add_rpc_multicall(w3_multicall, list_rpc_call=list_rpc_call, list_call_id=list_call_id,
                          batch_size=batch_size)
        responses = self.client_querier.sent_batch_to_provider(list_rpc_call, batch_size=1)
        decoded_data = decode_multical_response(
            w3_multicall=w3_multicall, data_responses=responses,
            list_call_id=list_call_id, ignore_error=True, batch_size=batch_size
        )
        w3_multicall.calls = {}
        for idx, nft in enumerate(list_nfts):
            pool_address = nft['poolAddress']
            position = decoded_data.get(f'positions_{nft["nftManagerAddress"]}_{nft["tokenId"]}_{block_number}'.lower())
            if not position or position[7] == 0:
                continue
            pools_in_batch.add(pool_address)
            important_nfts.append(nft)
            if pool_address not in pools:
                w3_multicall.add(
                    W3Multicall.Call(address=Web3.to_checksum_address(pool_address), block_number=block_number,
                                     abi=UNISWAP_V3_POOL_ABI, fn_name='feeGrowthGlobal0X128'
                                     ))
                w3_multicall.add(
                    W3Multicall.Call(address=Web3.to_checksum_address(pool_address), block_number=block_number,
                                     abi=UNISWAP_V3_POOL_ABI, fn_name='feeGrowthGlobal1X128'
                                     ))

            w3_multicall.add(
                W3Multicall.Call(address=Web3.to_checksum_address(pool_address), block_number=block_number,
                                 abi=UNISWAP_V3_POOL_ABI, fn_name='ticks', fn_paras=nft['tickLower']
                                 ))
            w3_multicall.add(
                W3Multicall.Call(address=Web3.to_checksum_address(pool_address), block_number=block_number,
                                 abi=UNISWAP_V3_POOL_ABI, fn_name='ticks', fn_paras=nft['tickUpper']
                                 ))

        list_call_id, list_rpc_call = [], []
        add_rpc_multicall(w3_multicall, list_rpc_call=list_rpc_call, list_call_id=list_call_id, batch_size=batch_size)
        try:
            responses = self.client_querier.sent_batch_to_provider(list_rpc_call, batch_size=1)
            decoded_data.update(decode_multical_response(
                w3_multicall=w3_multicall, data_responses=responses,
                list_call_id=list_call_id, ignore_error=True, batch_size=batch_size
            ))
            return decoded_data, list_nfts, important_nfts, pools_in_batch

        except Exception as e:
            logger.error(f"Error while send batch to provider: {e}")
            return {}, [], [], {}

    def get_batch_nft_fee_with_block_number(self, nfts, pools, w3_multicall, block_number='latest', batch_size=2000):
        for nft in nfts:
            w3_multicall.add(
                W3Multicall.Call(address=Web3.to_checksum_address(nft['nftManagerAddress']), block_number=block_number,
                                 abi=UNISWAP_V3_NFT_MANAGER_ABI, fn_name='positions', fn_paras=int(nft['tokenId'])
                                 ))
        list_call_id, list_rpc_call = [], []
        add_rpc_multicall(w3_multicall, list_rpc_call=list_rpc_call, list_call_id=list_call_id,
                          batch_size=batch_size)
        responses = self.client_querier.sent_batch_to_provider(list_rpc_call, batch_size=1)
        decoded_data = decode_multical_response(
            w3_multicall=w3_multicall, data_responses=responses,
            list_call_id=list_call_id, ignore_error=True, batch_size=batch_size
        )
        w3_multicall.calls = {}

        for idx, nft in enumerate(nfts):
            position = decoded_data.get(f'positions_{nft["nftManagerAddress"]}_{nft["tokenId"]}_{block_number}'.lower())
            if not position:
                continue
            pool_address = nft['poolAddress']
            if pool_address not in pools:
                w3_multicall.add(
                    W3Multicall.Call(address=Web3.to_checksum_address(pool_address), block_number=block_number,
                                     abi=UNISWAP_V3_POOL_ABI, fn_name='feeGrowthGlobal0X128'
                                     ))
                w3_multicall.add(
                    W3Multicall.Call(address=Web3.to_checksum_address(pool_address), block_number=block_number,
                                     abi=UNISWAP_V3_POOL_ABI, fn_name='feeGrowthGlobal1X128'
                                     ))
                w3_multicall.add(
                    W3Multicall.Call(address=Web3.to_checksum_address(pool_address), block_number=block_number,
                                     abi=UNISWAP_V3_POOL_ABI, fn_name='slot0'
                                     ))

            w3_multicall.add(
                W3Multicall.Call(address=Web3.to_checksum_address(pool_address), block_number=block_number,
                                 abi=UNISWAP_V3_POOL_ABI, fn_name='ticks', fn_paras=nft['tickLower']
                                 ))
            w3_multicall.add(
                W3Multicall.Call(address=Web3.to_checksum_address(pool_address), block_number=block_number,
                                 abi=UNISWAP_V3_POOL_ABI, fn_name='ticks', fn_paras=nft['tickUpper']
                                 ))

        list_call_id, list_rpc_call = [], []
        add_rpc_multicall(w3_multicall, list_rpc_call=list_rpc_call, list_call_id=list_call_id, batch_size=batch_size)
        try:
            responses = self.client_querier.sent_batch_to_provider(list_rpc_call, batch_size=1)
            decoded_data.update(decode_multical_response(
                w3_multicall=w3_multicall, data_responses=responses,
                list_call_id=list_call_id, ignore_error=True, batch_size=batch_size
            ))
            return decoded_data

        except Exception as e:
            logger.error(f"Error while send batch to provider: {e}")
            time.sleep(120)
            return {}



    # def get_batch_nft_fee_with_block_number(self, nfts, pools, list_rpc_call, list_call_id, start_block=None, latest = False):
    #     for idx, nft in enumerate(nfts):
    #         block_number = start_block if start_block else nft['blockNumber']
    #         if not block_number:
    #             continue
    #         pool_address = nft['poolAddress']
    #         if pool_address not in pools:
    #             _w3 = Web3(Web3.HTTPProvider('https://rpc.ankr.com/eth'))
    #             w3_multicall = W3Multicall(_w3, address='0xcA11bde05977b3631167028862bE2a173976CA11')
    #             w3_multicall.add(
    #                 W3Multicall.Call(address=Web3.to_checksum_address(pool_address),
    #                 abi=UNISWAP_V3_POOL_ABI,fn_name='feeGrowthGlobal0X128'
    #             ))
    #             w3_multicall.add(
    #                 W3Multicall.Call(address=Web3.to_checksum_address(pool_address),
    #                 abi=UNISWAP_V3_POOL_ABI,fn_name='feeGrowthGlobal1X128'
    #             ))
    #             # add_rpc_call(
    #             #     abi=UNISWAP_V3_POOL_ABI, contract_address=pool_address,
    #             #     fn_name="feeGrowthGlobal0X128", block_number=block_number,
    #             #     list_call_id=list_call_id, list_rpc_call=list_rpc_call
    #             # )
    #             #
    #             # add_rpc_call(
    #             #     abi=UNISWAP_V3_POOL_ABI, contract_address=pool_address,
    #             #     fn_name="feeGrowthGlobal1X128", block_number=block_number,
    #             #     list_call_id=list_call_id, list_rpc_call=list_rpc_call
    #             # )
    #             if not latest:
    #                 w3_multicall.add(
    #                     W3Multicall.Call(address=Web3.to_checksum_address(pool_address),
    #                                      abi=UNISWAP_V3_POOL_ABI, fn_name='slot0'
    #                                      ))
    #                 add_rpc_call(
    #                     abi=UNISWAP_V3_POOL_ABI, contract_address=pool_address,
    #                     fn_name="slot0", block_number=block_number,
    #                     list_call_id=list_call_id, list_rpc_call=list_rpc_call
    #                 )
    #
    #         w3_multicall.add(
    #             W3Multicall.Call(address=Web3.to_checksum_address(pool_address),
    #                              abi=UNISWAP_V3_POOL_ABI, fn_name='ticks', fn_paras=nft['tickLower']
    #                              ))
    #         w3_multicall.add(
    #             W3Multicall.Call(address=Web3.to_checksum_address(pool_address),
    #                              abi=UNISWAP_V3_POOL_ABI, fn_name='ticks', fn_paras=nft['tickUpper']
    #                              ))
    #         w3_multicall.add(
    #             W3Multicall.Call(address=Web3.to_checksum_address(nft['nftManagerAddress']),
    #                              abi=UNISWAP_V3_NFT_MANAGER_ABI, fn_name='positions', fn_paras=int(nft['tokenId'])
    #                              ))
    #         # add_rpc_call(
    #         #     abi=UNISWAP_V3_POOL_ABI, contract_address=pool_address,
    #         #     fn_name="ticks", block_number=block_number, fn_paras=nft['tickLower'],
    #         #     list_call_id=list_call_id, list_rpc_call=list_rpc_call
    #         # )
    #         # add_rpc_call(
    #         #     abi=UNISWAP_V3_POOL_ABI, contract_address=pool_address,
    #         #     fn_name="ticks", block_number=block_number, fn_paras=nft['tickUpper'],
    #         #     list_call_id=list_call_id, list_rpc_call=list_rpc_call
    #         # )
    #         # add_rpc_call(
    #         #     abi=UNISWAP_V3_NFT_MANAGER_ABI, contract_address=nft['nftManagerAddress'],
    #         #     fn_name="positions", block_number=block_number, fn_paras=int(nft['tokenId']),
    #         #     list_call_id=list_call_id, list_rpc_call=list_rpc_call
    #         # )
    #
    #     return list_rpc_call, list_call_id
