MULTICALL_ABI = [
    {
        "constant": True,
        "inputs": [
            {
                "components": [
                    {
                        "name": "target",
                        "type": "address"
                    },
                    {
                        "name": "callData",
                        "type": "bytes"
                    }
                ],
                "name": "calls",
                "type": "tuple[]"
            }
        ],
        "name": "aggregate",
        "outputs": [
            {
                "name": "blockNumber",
                "type": "uint256"
            },
            {
                "name": "returnData",
                "type": "bytes[]"
            }
        ],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [
            {
                "name": "addr",
                "type": "address"
            }
        ],
        "name": "getEthBalance",
        "outputs": [
            {
                "name": "balance",
                "type": "uint256"
            }
        ],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    }
]
