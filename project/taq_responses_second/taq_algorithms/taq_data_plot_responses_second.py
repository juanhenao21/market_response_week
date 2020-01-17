'''TAQ data plot module.

The functions in the module plot the data obtained in the
taq_data_analysis_responses_second module.

This script requires the following modules:
    * matplotlib
    * pickle
    * taq_data_tools_responses_second

The module contains the following functions:
    * taq_self_response_week_avg_responses_second_plot - plots the self-
     response average for a week.
    * main - the main function of the script.

.. moduleauthor:: Juan Camilo Henao Londono <www.github.com/juanhenao21>
'''

# ----------------------------------------------------------------------------
# Modules

from matplotlib import pyplot as plt
import pickle

import taq_data_tools_responses_second

# ----------------------------------------------------------------------------


def taq_midpoint_second_plot(ticker, dates):
    """Plots the midpoint price in second scale for a day.

    :param ticker: string of the abbreviation of the stock to be analized
     (i.e. 'AAPL').
    :param dates: list of strings with the date of the data to be extracted
     (i.e. ['2008-01-02', '2008-01-03]).
    :return: None -- The function saves the plot in a file and does not return
     a value.
    """

    year = dates[0].split('-')[0]
    month = dates[0].split('-')[1]
    day_ini_ = dates[0].split('-')[2]
    day_fin_ = dates[-1].split('-')[2]

    try:
        function_name = taq_midpoint_second_plot.__name__
        taq_data_tools_responses_second \
            .taq_function_header_print_plot(function_name, ticker, ticker,
                                            year, '', '')

        figure = plt.figure(figsize=(16, 9))

        for date in dates:

            date_sep = date.split('-')
            year = date_sep[0]
            month = date_sep[1]
            day = date_sep[2]

            # Load data
            midpoint = pickle.load(open(
                f'../../taq_data/responses_second_data_{year}/taq_midpoint'
                + f'_second_data/taq_midpoint_second_data_midpoint'
                + f'_{year}{month}{day}_{ticker}.pickle', 'rb'))
            time = pickle.load(open(
                f'../../taq_data/responses_second_data_{year}/taq_midpoint'
                + f'_second_data/taq_midpoint_second_data_time.pickle', 'rb'))

            plt.plot(time, midpoint, linewidth=5, label=f'{date}')
            plt.legend(loc='best', fontsize=25)
            plt.xlabel(r'Time $[s]$', fontsize=35)
            plt.ylabel(r'$m(t)$', fontsize=35)
            plt.xticks(fontsize=25)
            plt.yticks(fontsize=25)

        plt.title(f'TAQ Midpoint price - {ticker} - {year}.{month}'
                  + f'.{day_ini_}/{day_fin_}', fontsize=40)
        plt.grid(True)
        plt.tight_layout()

        # Plotting
        taq_data_tools_responses_second \
            .taq_save_plot(f'{function_name}_{month}', figure, ticker, ticker,
                           year, '')

        return None

    except FileNotFoundError as e:
        print('No data')
        print(e)
        print()
        return None

# ----------------------------------------------------------------------------


def taq_self_response_week_avg_responses_second_plot(ticker, dates):
    """Plots the self-response average for a year.

    :param ticker: string of the abbreviation of the stock to be analized
     (i.e. 'AAPL').
    :param dates: list of strings with the date of the data to be extracted
     (i.e. ['2008-01-02', '2008-01-03]).
    :return: None -- The function saves the plot in a file and does not return
     a value.
    """

    year = dates[0].split('-')[0]
    month = dates[0].split('-')[1]
    day_ini_ = dates[0].split('-')[2]
    day_fin_ = dates[-1].split('-')[2]

    try:
        function_name = taq_self_response_week_avg_responses_second_plot \
            .__name__
        taq_data_tools_responses_second \
            .taq_function_header_print_plot(function_name, ticker, ticker,
                                            year, '', '')

        # Load data
        self_ = pickle.load(open(
                        f'../../taq_data/responses_second_data_{year}/taq_self'
                        + f'_response_week_responses_second_data_{month}/taq'
                        + f'_self_response_week_responses_second_data_{month}'
                        + f'_{year}_{ticker}.pickle', 'rb'))

        figure = plt.figure(figsize=(16, 9))
        plt.semilogx(self_, linewidth=5, label=f'{ticker}')
        plt.legend(loc='best', fontsize=25)
        plt.title(f'TAQ Self-response - {ticker} - {year}.{month}'
                  + f'.{day_ini_}/{day_fin_}', fontsize=40)
        plt.xlabel(r'$\tau \, [s]$', fontsize=35)
        plt.ylabel(r'$R_{ii}(\tau)$', fontsize=35)
        plt.xticks(fontsize=25)
        plt.yticks(fontsize=25)
        plt.xlim(1, 1000)
        # plt.ylim(13 * 10 ** -5, 16 * 10 ** -5)
        plt.ticklabel_format(style='sci', axis='y', scilimits=(0, 0))
        plt.grid(True)
        plt.tight_layout()

        # Plotting
        taq_data_tools_responses_second \
            .taq_save_plot(f'{function_name}_{month}', figure, ticker, ticker,
                           year, '')

        return None

    except FileNotFoundError as e:
        print('No data')
        print(e)
        print()
        return None

# ----------------------------------------------------------------------------


def main():
    """The main function of the script.

    The main function is used to test the functions in the script.

    :return: None.
    """

    pass

    return None

# -----------------------------------------------------------------------------


if __name__ == '__main__':
    main()
