script = {"name": "elysium-trades", "version": "2", "lookback": 100}

# Use a market contract address, not the ERC-20 token address.
# Volume counts trades since connection; current_price is the confirmed block-end price.
MARKET = "0x59675174f1700677e608f86016f1cea764e1abfa@trades@elysium"


def on_data(ctx, history):
    trade = history.source(MARKET, 0)
    market = trade["onchain"]
    
    return {"metrics": {
        "side": market["side"],
        "execution_price": trade["price"],
        "current_price": market["current_price"],
        "quote_volume": market["volume"]["quote"],
        "fee": market["fee"]["amount"],
    }}
