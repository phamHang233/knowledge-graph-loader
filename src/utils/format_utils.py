import datetime
import re

import base58

from src.utils.logger_utils import get_logger

logger = get_logger('Format Utils')


def convert_tvl(tvl_str: str, type_=int):
    if not tvl_str:
        return None

    try:
        tvl_str = tvl_str.replace(',', '').lower().strip()
        if tvl_str.startswith('$'):
            tvl_str = tvl_str[1:]

        if tvl_str.startswith('<'):
            return 0

        decimals_map = {
            'b': 9,
            'm': 6,
            'k': 3
        }
        decimals = 0
        for k, decimals_ in decimals_map.items():
            if tvl_str.endswith(k):
                tvl_str = tvl_str[:-1]
                decimals = decimals_
                break

        tvl_str = float(tvl_str) * round(10 ** decimals)
        return type_(tvl_str)
    except Exception as ex:
        logger.exception(ex)
        return None


def convert_tx_timestamp(time_str):
    time_format = '%b-%d-%Y %I:%M:%S %p'
    time = datetime.datetime.strptime(time_str, time_format)
    return int(time.timestamp())


def convert_coingecko_exchange_ticker_timestamp(time_str):
    if isinstance(time_str, str):
        if time_str.isdecimal():
            timestamp = int(time_str)
        elif time_str.isnumeric():
            timestamp = float(time_str)
        else:
            datetime_object = datetime.datetime.fromisoformat(time_str)
            timestamp = datetime_object.timestamp()

    elif (not isinstance(time_str, float)) and (not isinstance(time_str, int)):
        raise ValueError(f'Invalid type of time string {time_str}')

    else:
        timestamp = time_str

    timestamp = int(timestamp)
    if len(str(timestamp)) > 13:
        timestamp = int(timestamp / 1000)

    return timestamp


def remove_special_characters(string: str, specific_characters: str = None):
    if specific_characters is None:
        specific_characters = '!@#$%^&*()+=[]\\|{}:;\'"<>?/.,~`'
    return string.translate(str.maketrans('', '', specific_characters))


def filter_string(string: str, allowable_characters_regex: str = None):
    if allowable_characters_regex is None:
        allowable_characters_regex = '[a-zA-Z0-9_\-:.@()+,=;$!*\'%]'
    chars = re.findall(allowable_characters_regex, string)
    return ''.join(chars)


def convert_percentage(p_str: str, exception='-', default_value=None):
    if p_str == exception:
        return default_value

    p_str = p_str.strip().replace(',', '')
    try:
        if p_str.endswith('%'):
            p_str = p_str.replace('%', '')
            p = round(float(p_str) / 100, 4)
        else:
            p = float(p_str)
    except (TypeError, ValueError):
        logger.warning(f'Cannot format percentage of {p_str}')
        p = 0
    except Exception as ex:
        logger.exception(ex)
        p = 0
    return p


def format_cmc_number_data(text, handler_func=int, exception='--', default_value=None):
    if text != exception:
        try:
            return handler_func(text)
        except ValueError:
            return default_value

    return default_value


def format_cmc_launched_at(text):
    return int(datetime.datetime.strptime(text, '%b %Y').timestamp())


def format_opensea_volume(volume_text, eth_price=1, handler_func=float, exception='—', default_value=None):
    if not volume_text:
        return default_value

    if isinstance(volume_text, str):
        if '<' in volume_text:
            return default_value

        volume_text = volume_text.strip().lower()
        volume_text = volume_text.split(' ')[0]
        volume_text = volume_text.replace(',', '').strip()

    if volume_text != exception:
        try:
            volume = handler_func(volume_text)
            return volume * eth_price
        except Exception as ex:
            logger.exception(ex)
    return default_value


def get_domain(url):
    try:
        domain = url.split('/')[2]
        domain = domain.replace('app.', '').replace('www.', '')
    except Exception as ex:
        logger.exception(ex)
        logger.warning(f'Error with url {url}')
        domain = url

    return domain


def hex_to_base58(hex_string):
    if hex_string[:2] in ["0x", "0X"]:
        hex_string = "41" + hex_string[2:]
    bytes_str = bytes.fromhex(hex_string)
    base58_str = base58.b58encode_check(bytes_str)
    return base58_str.decode("UTF-8")


def base58_to_hex(base58_string):
    asc_string = base58.b58decode_check(base58_string)
    hex_str = asc_string.hex().upper()
    return '0x' + hex_str[2:].lower()
