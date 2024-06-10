import numpy as np
import matplotlib.pyplot as plt

from src.databases.dex_nft_manager_db import NFTMongoDB
from src.databases.mongodb_dex import MongoDBDex

# set width of bar
bar_width = 0.25
fig = plt.subplots(figsize =(12, 8))

# set height of bar
IT = [12, 30, 1, 8, 22]
ECE = [28, 6, 16, 5, 10]
CSE = [29, 3, 24, 25, 17]
y_ticks = []
real_nft = []
best_apr = []
klg = NFTMongoDB()
pairs = list(klg.get_pairs())
pair_addresses = [doc['address'] for doc in pairs]
dex_db = MongoDBDex()
cursor = dex_db.get_pairs_with_addresses(addresses=pair_addresses, chain_id = '0x1')
pair_symbol = {doc['address']: doc['symbol'] for doc in cursor}
cnt = 0
for pair in pairs:
	pair_address = pair.get('address')
	nft = klg.get_top_nfts_of_pair(pair_address)
	if nft:
		apr_of_nft = nft['aprInMonth']
		if apr_of_nft >2 or apr_of_nft < 0.1:
			continue
		y_ticks.append(pair_symbol[pair_address])
		real_nft.append(round(apr_of_nft, 2))
		best_apr.append(round(pair['bestAPR']['apr'], 2))
		cnt +=1
		if cnt == 20:
			break

width = 0.1  # the width of the bars
multiplier = 0

fig, ax = plt.subplots(layout='constrained')
x = np.arange(len(y_ticks))
# for attribute, measurement in penguin_means.items():
offset = width * multiplier
real = ax.bar(x + offset, real_nft, width, label='real')
ax.bar_label(real, padding=3)

multiplier = 1
offset = width * multiplier
optimize = ax.bar(x + offset, best_apr, width, label='best apr')
ax.bar_label(optimize, padding=3)
# multiplier =2

# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_ylabel('Length (mm)')
ax.set_title('Penguin attributes by species')
ax.set_xticks(x + width, y_ticks)
ax.set_xticklabels(ax.get_xticklabels(), rotation=90)
ax.legend(loc='upper left', ncols=3)
# ax.set_ylim(0, 2)

plt.show()