import time

from job.constants.network_constant import URL_PROTOCOL
import requests


def pool_by_id(id, protocol):
    url = URL_PROTOCOL.mapping.get(protocol)
    pool_query_fields = """{
    id
    feeTier
    totalValueLockedUSD
    totalValueLockedETH
    token0Price
    token1Price
    token0 {
      id
      symbol
      name
      decimals
    }
    token1 {
      id
      symbol
      name
      decimals
    }
    poolDayData(orderBy: date, orderDirection:desc,first:1)
    {
      date
      volumeUSD
      tvlUSD
      feesUSD
      liquidity
      high
      low
      volumeToken0
      volumeToken1
      close
      open
    }
  }"""
    query = "query Pools($id: ID!) { id: pools(where: { id: $id } orderBy:totalValueLockedETH, orderDirection:desc) " + pool_query_fields + "}"
    try:
        response = requests.post(url, json={'query': query, 'variables': {'id': id}})
        data = response.json()

        if data and 'data' in data:
            pools = data['data']

            if 'id' in pools and len(pools['id']) and len(pools['id']) == 1:
                return pools['id'][0]
        else:
            return None

    except Exception as error:
        return {'error': str(error)}


def get_pool_hour_data(pool, from_date, to_date, protocol):
    url = URL_PROTOCOL.mapping.get(protocol)
    query = """query
    PoolHourDatas($pool: ID!, $fromdate: Int!, $todate: Int!) {
        poolHourDatas(where: {pool:$pool, periodStartUnix_gt:$fromdate periodStartUnix_lt:$todate close_gt: 0}, orderBy: periodStartUnix, orderDirection: desc, first: 1000) {
    periodStartUnix
    liquidity
    high
    low
    volumeUSD
    pool {
      id
      totalValueLockedUSD
      totalValueLockedToken1
      totalValueLockedToken0
      token0
        {decimals}
      token1
        {decimals}
    }
    close
    feeGrowthGlobal0X128
    feeGrowthGlobal1X128
    }
    }
    """
    try:
        response = requests.post(url, json={'query': query,
                                            'variables': {"pool": pool, "fromdate": from_date, "todate": to_date}})
        data = response.json()
        if data and data.get('data') and data.get('data')['poolHourDatas']:
            return data['data']['poolHourDatas']
        else:
            print("nothing returned from getPoolHourData")
            return None
    except Exception as error:
        return error


def get_pool_day_datas(pool, protocol, from_date, to_date):
    url = URL_PROTOCOL.mapping.get(protocol)
    query = """
    query PoolDayDatas($id: ID!, $fromdate: Int!, $todate: Int!) {

        poolDayDatas(where: {pool: $id, date_gte: $fromdate, date_lte: $todate},orderBy: date )
        {
        high
        low
        close
        date
        volumeUSD
          pool {
      token0
        {decimals}
      token1
        {decimals}
    }
        }
      }"""

    try:
        response = requests.post(url, json={'query': query,
                                            'variables': {"id": pool, "fromdate": from_date, "todate": to_date}})

        data = response.json()

        if data and data.get('data') and data.get('data')['poolDayDatas']:
            return data['data']['poolDayDatas']

        else:
            print("nothing returned from PoolDayDatas")
            return None

    except Exception as error:
        return {'error': str(error)}


def convert_datetime_to_timestamp(date_time):
    """Converts a datetime object to a Unix timestamp (seconds since epoch)."""
    return int(time.mktime(date_time.timetuple()))

# print(
#     pool_day_data("0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640", 'ethereum',
#                   convert_datetime_to_timestamp(datetime.datetime.fromtimestamp(1710032101)),
#                   convert_datetime_to_timestamp(datetime.datetime.fromtimestamp(1712710501))))
# print(pool_by_id('0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640', 'ethereum'))
0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2