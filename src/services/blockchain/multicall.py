import json
import time
from typing import Tuple, List, Union, Any

from query_state_lib.base.utils.decoder import decode_eth_call_data
from web3 import Web3, contract

from artifacts.abis.erc20_abi import ERC20_ABI
from artifacts.abis.multicall_abi import MULTICALL_ABI

w3 = Web3()


def get_fn_abi(abi, fn_name):
    for abi_ in abi:
        if abi_.get('type') == 'function' and abi_.get('name') == fn_name:
            return abi_

    return None


def _unpack_aggregate_outputs(outputs: Any) -> Tuple[Tuple[Union[None, bool], bytes], ...]:
    return tuple((None, output) for output in outputs)


class W3Multicall:
    """
    Interface for multicall3.sol contract
    """

    class Call:

        def __init__(self, address, abi, fn_name, fn_paras=None):
            self.address = address
            self.fn_name = fn_name
            self.fn_abi = get_fn_abi(abi=abi, fn_name=fn_name)

            args = []
            if fn_paras is not None:
                if type(fn_paras) is list:
                    args = fn_paras
                else:
                    if Web3.is_address(fn_paras):
                        fn_paras = Web3.to_checksum_address(fn_paras)
                    args = [fn_paras]
                self.call_id =  f"{fn_name}_{address}_{fn_paras}".lower()
            else:
                self.call_id =  f"{fn_name}_{address}".lower()

            c = contract.Contract
            c.w3 = w3
            c.abi = abi

            self.data = c.encodeABI(fn_name=fn_name, args=args)

    def __init__(self, web3, address='0xcA11bde05977b3631167028862bE2a173976CA11', calls: List['W3Multicall.Call'] = None):
        """
        :param web3: Web3 instance
        :param address: (optional) address of the multicall3.sol contract
        :param calls: (optional) list of W3Multicall.Call to perform
        """
        self.web3 = web3
        self.address = address
        self.calls: List['W3Multicall.Call'] = [] if calls is None else calls.copy()
        self.require_success = True

    def add(self, call: 'W3Multicall.Call'):
        self.calls.append(call)

    def get_params(self):
        args = self._get_args()
        return args

    def decode(self, aggregated):
        unpacked = _unpack_aggregate_outputs(aggregated[1])

        outputs = {}
        for call, (success, output) in zip(self.calls, unpacked):
            call_output = '0x' + output.hex()
            decoded_data = decode_eth_call_data([call.fn_abi], call.fn_name, call_output)

            if len(decoded_data) == 1:
                decoded_data = decoded_data[0]

            outputs[call.call_id] = decoded_data

        return outputs

    def _get_args(self) -> List[Union[bool, List[List[Any]]]]:
        if self.require_success is True:
            return [[[call.address, call.data] for call in self.calls]]
        return [self.require_success, [[call.address, call.data] for call in self.calls]]


def _test_multicall(wallet, tokens):
    start_time = time.time()

    # Test on Ethereum
    _w3 = Web3(Web3.HTTPProvider('https://rpc.ankr.com/eth'))
    w3_multicall = W3Multicall(_w3, address='0xcA11bde05977b3631167028862bE2a173976CA11')

    for token in tokens:
        if token['address'] == '0x0000000000000000000000000000000000000000':
            continue

        w3_multicall.add(W3Multicall.Call(
            Web3.to_checksum_address(token['address']),  # contract address
            ERC20_ABI,  # method signature to call
            'balanceOf',
            wallet
        ))

        w3_multicall.add(W3Multicall.Call(
            Web3.to_checksum_address(token['address']),  # contract address
            ERC20_ABI,  # method signature to call
            'decimals'
        ))

    print(f'There are {len(w3_multicall.calls)} calls')
    print(f'Encode took {round(time.time() - start_time, 3)}s')

    # For single call
    contract_ = _w3.eth.contract(Web3.to_checksum_address(w3_multicall.address), abi=MULTICALL_ABI)
    inputs = w3_multicall.get_params()
    response = contract_.functions.aggregate(*inputs).call(block_identifier='latest')
    results = w3_multicall.decode(response)

    data = {}
    idx = 0
    for token in tokens:
        if token['address'] == '0x0000000000000000000000000000000000000000':
            continue

        balance = results[idx] / 10 ** results[idx + 1]
        data[token['address']] = {
            'name': token['name'],
            'symbol': token['symbol'],
            'balance': balance,
            'decimals': results[idx + 1]
        }

        idx += 2

    with open('results_multicall.json', 'w') as f:
        json.dump(data, f, indent=2)

#
# if __name__ == '__main__':
#     wallet_ = '0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045'
#     tokens_ = [
#         {
#             "address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
#             "name": "Tether",
#             "symbol": "usdt"
#         },
#         {
#             "address": "0xb8c77482e45f1f44de1745f52c74426c631bdd52",
#             "name": "BNB",
#             "symbol": "bnb"
#         }
#     ]
#
#     start_time_ = time.time()
#     _test_multicall(wallet_, tokens_)
#     print(f'Done after {round(time.time() - start_time_, 3)}s')
