'''ITCH data analysis module.

The functions in the module extract the midpoint price and trade signs from the
order book in the ITCH data.

This script requires the following modules:
    * numpy
    * pandas
    * itch_data_tools_data_extract

The module contains the following functions:
    * itch_midpoint_millisecond_data - extracts the midpoint price of a day in
     milliseconds.
    * itch_midpoint_second_data - extracts the midpoint price of a day in
     seconds.
    * itch_trade_signs_millisecond_data - extracts the trade signs of a day.
    * main - the main function of the script.

.. moduleauthor:: Juan Camilo Henao Londono <www.github.com/juanhenao21>
'''

# -----------------------------------------------------------------------------
# Modules

import gzip
import numpy as np
import os
import pandas as pd
import pickle

import itch_data_tools_data_extraction

# -----------------------------------------------------------------------------


def itch_midpoint_millisecond_data(ticker, date):
    """Extracts the midpoint price data for a day in milliseconds.

    Extracts the midpoint price from the TotalView-ITCH data for a day. The
    data is obtained for the open market time (9h40 to 15h50). To fill the time
    spaces when nothing happens we replicate the last value calculated until a
    change in the price happens.

    :param ticker: string of the abbreviation of the stock to be analized
     (i.e. 'AAPL').
    :param date: string with the date of the data to be extracted
     (i.e. '2008-01-02').
    :return: tuple -- The function returns a tuple with numpy arrays.
    """

    date_sep = date.split('-')
    year = date_sep[0]
    month = date_sep[1]
    day = date_sep[2]

    try:

        # Load data
        data = np.genfromtxt(gzip.open(
            f'../../itch_data/original_data_{year}/{year}{month}{day}'
            + f'_{ticker}.csv.gz'), dtype='str', skip_header=1, delimiter=',')

        # Lists of times, ids, types, volumes and prices
        # List of all the available information in the data excluding the last
        # two columns
        times_ = np.array([int(mytime) for mytime in data[:, 0]])
        ids_ = np.array([int(myid) for myid in data[:, 2]])

        # List of order types:
        # "B" = 1 - > Add buy order
        # "S" = 2 - > Add sell order
        # "E" = 3 - > Execute outstanding order in part
        # "C" = 4 - > Cancel outstanding order in part
        # "F" = 5 - > Execute outstanding order in full
        # "D" = 6 - > Delete outstanding order in full
        # "X" = 7 - > Bulk volume for the cross event
        # "T" = 8 - > Execute non-displayed order

        types_ = np.array([1 * (mytype == 'B') +
                           2 * (mytype == 'S') +
                           3 * (mytype == 'E') +
                           4 * (mytype == 'C') +
                           5 * (mytype == 'F') +
                           6 * (mytype == 'D') +
                           7 * (mytype == 'X') +
                           8 * (mytype == 'T') for mytype in data[:, 3]])
        prices_ = np.array([int(myprice) for myprice in data[:, 5]])

        ids = ids_[types_ < 7]
        times = times_[types_ < 7]
        types = types_[types_ < 7]
        prices = prices_[types_ < 7]

        # Reference lists
        # Reference lists using the original values or the length of the
        # original lists
        prices_ref = 1 * prices
        types_ref = 0 * types
        times_ref = 0 * times
        index_ref = 0 * types
        newids = {}
        insertnr = {}
        hv = 0

        # Help lists with the data of the buy orders and sell orders
        hv_prices = prices[types < 3]
        hv_types = types[types < 3]
        hv_times = times[types < 3]

        # Fill the reference lists where the values of 'T' are 'E', 'C', 'F',
        # 'D'
        # For the data in the length of the ids list (all data)
        for iii in range(len(ids)):

            # If the data is a sell or buy order
            if (types[iii] < 3):

                # Insert in the dictionary newids a key with the valor of the
                # id and the value of hv (a counter)
                newids[ids[iii]] = hv

                # Insert in the dictionary insertnr a key with the valor of the
                # id and the value of the for counter
                insertnr[ids[iii]] = iii

                # Increase the value of hv
                hv += 1

            # If the data is not a sell or buy order
            else:

                # Fill the values of prices_ref with no prices ('E', 'C', 'F',
                # 'D') with the price of the order
                prices_ref[iii] = hv_prices[newids[ids[iii]]]
                # Fill the values of types_ref with no  prices ('E', 'C', 'F',
                # 'D') with the type of the order
                types_ref[iii] = hv_types[newids[ids[iii]]]
                # Fill the values of time_ref with no  prices ('E', 'C', 'F',
                # 'D') with the time of the order
                times_ref[iii] = hv_times[newids[ids[iii]]]
                # Fill the values of index_ref with no  prices ('E', 'C', 'F',
                # 'D') with the position of the sell or buy order
                index_ref[iii] = insertnr[ids[iii]]

        # Minimum and maximum trade price

        # The minimum price allowed is 0.9 times the price of
        # the minimum value of all full executed orders.
        minP = round(0.9 * (1. * prices_ref[types == 5] / 10000).min(), 2)
        # The maximum price allowed is 1.1 times the price of
        # the maximum value of all full executed orders.
        maxP = round(1.1 * (1. * prices_ref[types == 5] / 10000).max(), 2)
        # Values between maxP and minP with step of 0.01 cents
        valuesP = minP + 0.01 * np.arange(int((maxP - minP) / 0.01))
        maxP = valuesP.max()

        # Construct quotes and spread
        # Sell values started at 0
        nAsk = 0 * valuesP
        # Last value of nAsk set to 1
        nAsk[-1] = 1
        # Buy values starte at 0
        nBid = 0 * valuesP
        # First value of nBid set to 1
        nBid[0] = 1
        # Set bestAsk and bestAskOld to a high value
        bestAsk = 10000000.
        bestAskOld = 10000000.
        # Set bestBid and bestBidOld a low value
        bestBid = 0.
        bestBidOld = 0.
        # Create lists for best asks, bids and times
        bestAsks = []
        bestBids = []
        bestTimes = []

        # Finding the best asks and best bids

        # For the data in the length of the ids list (all data)
        for iii in range(len(ids)):

            # Incoming limit orders

            myPriceIndex = int(round(1. * (1. * prices_ref[iii] / 10000 - minP)
                               / 0.01))

            # Initializing bestAksOld and bestBidOld
            bestAskOld = 1 * bestAsk
            bestBidOld = 1 * bestBid

            # The price is greater than the minP
            if (myPriceIndex >= 0 and
                    myPriceIndex < len(valuesP)):

                # If the order is a sell
                if (types[iii] == 2):

                    if (nAsk[myPriceIndex] == 0):

                        # The bestAsk is the minimum value between the previous
                        # bestAsk and the value in valuesP with id myPriceIndex
                        bestAsk = min(bestAsk, valuesP[myPriceIndex])

                    # Increase the value of nAsk to 1 (value arrived the book)
                    nAsk[myPriceIndex] += 1

                # If the order is a buy
                if (types[iii] == 1):

                    if (nBid[myPriceIndex] == 0):

                        # The bestBid is the maximum value between the previous
                        # bestBid and the value in valuesP with id myPriceIndex
                        bestBid = max(bestBid, valuesP[myPriceIndex])

                    # Increase the value of nBid to 1 (value arrived the book)
                    nBid[myPriceIndex] += 1

                # limit orders completely leaving

                # If the order is a full executed order or if the order is a
                # full delete order
                if (types[iii] == 5
                        or types[iii] == 6):

                    # If the order is a sell
                    if (types_ref[iii] == 2):

                        # Reduce the value in nAsk to 0 (value left the book)
                        nAsk[myPriceIndex] -= 1

                        # If the value is not in the book and if the value is
                        # the best ask
                        if (nAsk[myPriceIndex] == 0 and
                                valuesP[myPriceIndex] == bestAsk):

                            # The best ask is the minimum value of the prices
                            # that are currently in the order book
                            bestAsk = valuesP[nAsk > 0].min()

                    else:

                        # Reduce the value in nBid to 0 (value left the book)
                        nBid[myPriceIndex] -= 1

                        # If the value is not in the book and if the value is
                        # the best bid
                        if (nBid[myPriceIndex] == 0
                                and valuesP[myPriceIndex] == bestBid):

                            # The best bid is the maximum value of the prices
                            # that are currently in the order book
                            bestBid = valuesP[nBid > 0].max()

            # If the bestAsk changes or and if the bestBid changes
            if (bestAsk != bestAskOld
                    or bestBid != bestBidOld):

                # Append the values of bestTimes, bestAsks and bestBids
                bestTimes.append(times[iii])
                bestAsks.append(bestAsk)
                bestBids.append(bestBid)
                bestAskOld = bestAsk
                bestBidOld = bestBid

        # Calculating the spread, midpoint and time

        # Calculating the spread
        spread_ = np.array(bestAsks) - np.array(bestBids)
        # Transforming bestTimes in an array
        timesS = np.array(bestTimes)
        midpoint_ = 1. * (np.array(bestAsks) + np.array(bestBids)) / 2

        # Setting the values in the open market time

        # This line behaves as an or the two arrays must achieve a condition,
        # in this case, be in the market trade hours
        day_times_ind = (1. * timesS / 3600 / 1000 > 9.5) * \
                        (1. * timesS / 3600 / 1000 < 16) > 0

        # Midpoint in the market trade hours
        midpoint = 1. * midpoint_[day_times_ind]
        # Time converted to hours in the market trade hours
        times_spread = 1. * timesS[day_times_ind]
        bestAsks = np.array(bestAsks)[day_times_ind]
        bestBids = np.array(bestBids)[day_times_ind]
        # Spread in the market trade hours
        spread = spread_[day_times_ind]

        return (times_spread, midpoint, bestAsks, bestBids, spread)

    except FileNotFoundError as e:
        print('No data')
        print(e)
        print()
        return None

# -----------------------------------------------------------------------------


def itch_midpoint_second_data(ticker, date):
    """Reduces the midpoint price data from milliseconds to seconds for a day.

    :param ticker: string of the abbreviation of the stock to be analized
     (i.e. 'AAPL').
    :param date: string with the date of the data to be extracted
     (i.e. '2008-01-02').
    :return: tuple -- The function returns a tuple with numpy arrays.
    """

    date_sep = date.split('-')
    year = date_sep[0]
    month = date_sep[1]
    day = date_sep[2]

    function_name = itch_midpoint_second_data.__name__
    itch_data_tools_data_extraction \
        .itch_function_header_print_data(function_name, ticker, ticker, year,
                                         month, day)

    # Extract data
    (time_ms, midpoint_ms,
        _, _, _) = itch_midpoint_millisecond_data(ticker, date)

    # Market time in seconds
    full_time = np.array(range(34800, 57000))
    midpoint_s = np.zeros(len(full_time))

    for t_idx, t_val in enumerate(full_time):

        # Select the last midpoint price of every second
        condition = (time_ms >= t_val * 1000) * (time_ms < (t_val + 1) * 1000)

        if (np.sum(condition)):
            midpoint_s[t_idx] = midpoint_ms[condition][-1]
        else:
            midpoint_s[t_idx] = midpoint_s[t_idx - 1]

    assert not np.sum(midpoint_s == 0)

    # Saving data
    itch_data_tools_data_extraction \
        .itch_save_data(function_name, (full_time, midpoint_s), ticker, ticker,
                        year, month, day)

    return (full_time, midpoint_s)

# -----------------------------------------------------------------------------


def itch_trade_signs_millisecond_data(ticker, date):
    """Obtain the trade signs data for a day in milliseconds.

    Extracts the trade signs from the TotalView-ITCH data for a day. The
    data is obtained for the open market time (9h40 to 15h50). To fill the time
    spaces when nothing happens we just fill with zeros indicating that there
    were neither a buy nor a sell.

    :param ticker: string of the abbreviation of the stock to be analized
     (i.e. 'AAPL').
    :param date: string with the date of the data to be extracted
     (i.e. '2008-01-02').
    :return: tuple -- The function returns a tuple with numpy arrays.
    """

    date_sep = date.split('-')
    year = date_sep[0]
    month = date_sep[1]
    day = date_sep[2]

    try:

        # Load full data using cols with values time, order, type, shares and
        # price
        data = pd.read_csv(gzip.open(
            f'../../itch_data/original_data_{year}/{year}{month}{day}_{ticker}'
            + f'.csv.gz', 'rt'), usecols=(0, 2, 3, 4, 5),
            dtype={'Time': 'uint32', 'Order': 'uint64', 'T': str,
                   'Shares': 'uint16', 'Price': 'float64'})

        data['Price'] = data['Price'] / 10000

        # Select only trade orders. Visible ('E' and 'F') and hidden ('T')
        trade_pos = np.array(data['T'] == 'E') + np.array(data['T'] == 'F') \
            + np.array(data['T'] == 'T')
        trade_data = data[trade_pos]

        # Converting the data in numpy arrays
        trade_data_time = trade_data['Time'].values
        trade_data_order = trade_data['Order'].values
        trade_data_types = 3 * np.array(trade_data['T'] == 'E') \
            + 4 * np.array(trade_data['T'] == 'F') \
            + 5 * np.array(trade_data['T'] == 'T')
        trade_data_volume = trade_data['Shares'].values
        trade_data_price = trade_data['Price'].values

        # Select only limit orders
        limit_pos = np.array(data['T'] == 'B') + np.array(data['T'] == 'S')
        limit_data = data[limit_pos]

        # Reduce the values to only the ones that have the same order number as
        # trade orders
        limit_data = limit_data[limit_data.Order.isin(trade_data['Order'])]

        # Converting the data in numpy arrays
        limit_data_order = limit_data['Order'].values
        limit_data_types = 1 * np.array(limit_data['T'] == 'S') \
            - 1 * np.array(limit_data['T'] == 'B')
        limit_data_volume = limit_data['Shares'].values
        limit_data_price = limit_data['Price'].values

        # Arrays to store the info of the identified trades
        length_trades = len(trade_data)
        trade_times = 1 * trade_data_time
        trade_signs = np.zeros(length_trades)
        trade_volumes = np.zeros(length_trades, dtype='uint16')
        trade_price = np.zeros(length_trades)

        # In the for loop is assigned the price, trade sign and volume of each
        # trade.
        for t_idx in range(length_trades):

            try:

                # limit orders that have the same order as the trade order
                l_idx = np.where(limit_data_order
                                 == trade_data_order[t_idx])[0][0]

                # Save values that are independent of the type
                # Price of the trade (Limit data)
                trade_price[t_idx] = limit_data_price[l_idx]

                # Trade sign identification
                trade = limit_data_types[l_idx]

                if (trade == 1):
                    trade_signs[t_idx] = 1.
                else:
                    trade_signs[t_idx] = -1.

                # The volume depends on the trade type. If it is 4 the
                # value is taken from the limit data and the order number
                # is deleted from the data. If it is 3 the
                # value is taken from the trade data and then the
                # value of the volume in the limit data must be
                # reduced with the value of the trade data
                volume_type = trade_data_types[t_idx]

                if (volume_type == 4):

                    trade_volumes[t_idx] = limit_data_volume[l_idx]
                    limit_data_order[l_idx] = 0

                else:

                    trade_volumes[t_idx] = trade_data_volume[t_idx]
                    diff_volumes = limit_data_volume[l_idx] \
                        - trade_data_volume[t_idx]

                    assert diff_volumes > 0

                    limit_data_volume[l_idx] = diff_volumes

            except IndexError:

                pass

        assert len(trade_signs != 0) == len(trade_data_types != 5)

        # To use the hidden trades, I change the values in the computed arrays
        # with # the information of visible trades to have the hidden
        # information.
        hidden_pos = trade_data_types == 5
        trade_volumes[hidden_pos] = trade_data_volume[hidden_pos]
        trade_price[hidden_pos] = trade_data_price[hidden_pos]

        # Open market time 9h40 - 15h50
        market_time = (trade_times / 3600 / 1000 >= 9.5) & \
            (trade_times / 3600 / 1000 < 16)

        trade_times_market = trade_times[market_time]
        trade_signs_market = trade_signs[market_time]
        trade_volumes_market = trade_volumes[market_time]
        trade_price_market = trade_price[market_time]

        return (trade_times_market, trade_signs_market, trade_volumes_market,
                trade_price_market)

    except FileNotFoundError as e:
        print('No data')
        print(e)
        print()
        return None

# -----------------------------------------------------------------------------


def itch_trade_signs_second_data(ticker, date):
    """Reduces the trade signs data from milliseconds to seconds for a day.

    :param ticker: string of the abbreviation of the stock to be analized
     (i.e. 'AAPL').
    :param date: string with the date of the data to be extracted
     (i.e. '2008-01-02').
    :return: tuple -- The function returns a tuple with numpy arrays.
    """

    date_sep = date.split('-')
    year = date_sep[0]
    month = date_sep[1]
    day = date_sep[2]

    function_name = itch_trade_signs_second_data.__name__
    itch_data_tools_data_extraction \
        .itch_function_header_print_data(function_name, ticker, ticker, year,
                                         month, day)

    # Extract data
    (time_ms, trade_signs_ms,
        _, _) = itch_trade_signs_millisecond_data(ticker, date)

    # Market time in seconds
    full_time = np.array(range(34800, 57000))
    trade_signs_s = np.zeros(len(full_time))

    for t_idx, t_val in enumerate(full_time):

        # take the sign function of the sum of every second
        condition = (time_ms >= t_val * 1000) * (time_ms < (t_val + 1) * 1000)
        trade_sum = np.sum(trade_signs_ms[condition])
        trade_signs_s[t_idx] = np.sign(trade_sum)

    # Saving data
    itch_data_tools_data_extraction \
        .itch_save_data(function_name, (full_time, trade_signs_s), ticker,
                        ticker, year, month, day)

    return (full_time, trade_signs_s)

# -----------------------------------------------------------------------------


def main():
    """The main function of the script.

    The main function is used to test the functions in the script.

    :return: None.
    """

    pass

# -----------------------------------------------------------------------------


if __name__ == "__main__":
    main()
