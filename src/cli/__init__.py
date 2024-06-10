import click

from src.cli.calculate_optimized_range import calculate_optimized_price_range
from src.cli.flagged_nft import nft_flagged
from src.cli.update_nft_stream import update_nft_info_stream
from src.cli.nft_info_enricher import dex_nft_info_enricher
from src.cli.update_wallet_info import dex_wallet_info_enricher


@click.group()
@click.version_option(version='1.0.0')
@click.pass_context
def cli(ctx):
    # Command line
    pass


cli.add_command(dex_nft_info_enricher, 'dex_nft_info_enricher')
cli.add_command(update_nft_info_stream, "update_nft_info_stream")
cli.add_command(nft_flagged, "nft_flagged")
cli.add_command(calculate_optimized_price_range, "calculate_optimized_price_range")
cli.add_command(dex_wallet_info_enricher, "dex_wallet_info_enricher")
