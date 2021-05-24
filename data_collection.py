from orm import engine, Valuta, Coin, session
import requests
import pandas as pd
import numpy as np


def get_past_day(coin_name):
    """
    Fetch minutely data from the last 24 hours using the CoinGecko API
    :param coin_name: name of the coin as they accept
    :return: DataFrame containing price and total volume moved for each time step
    """
    url = f"https://api.coingecko.com/api/v3/coins/{coin_name}/market_chart"
    params = {'vs_currency': 'eur', 'days': '1'}
    f = requests.get(url, params=params)
    if 'prices' not in f.json():
        raise ValueError(
            "Wrong response for %s, check if it exists. Returned keys: %s" % (coin_name, str(f.json().keys())))
    num_data = np.array(f.json()['prices'])
    dates = num_data[:, 0]
    prices = np.array(f.json()['prices'])[:, 1]
    volumes = np.array(f.json()['total_volumes'])[:, 1]
    data = {'time': dates, 'coin': coin_name, 'price': prices, 'volume': volumes}
    df = pd.DataFrame(data)
    return df


def _get_last_entry(session, coin):
    """
    Find the last entry for a specific coin. Required so we don't overwrite old data
    :param session: SQLAlchemy session
    :param coin: ORM coin object
    :return: Last record as a Valuta
    """
    obj = session.query(Valuta).filter(Valuta.coin == coin.name).order_by(Valuta.time.desc()).first()
    return obj


def fetch(coin_name):
    """
    Main method of this module. Fetches new data for a specific coin and stores it in the database
    :param coin_name: Name of the coin as available on CoinGecko
    :return: None
    """
    coin = session.query(Coin).filter(Coin.name == coin_name).first()
    if not coin:
        coin = Coin(name=coin_name)
        session.add(coin)
        session.commit()
    new_data = get_past_day(coin_name)
    last_entry = _get_last_entry(session, coin)
    if last_entry:
        new_data = new_data[new_data.time > last_entry.time]
    if new_data:
        new_data.to_sql("valuta", engine, index=False, if_exists='append')
        print("Added %d new records" % len(new_data))


if __name__ == '__main__':
    fetch('bitcoin')
