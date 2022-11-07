import argparse
from argparse import Namespace
import csv
from datetime import datetime
import os
import warnings


from pandas import DataFrame

from nordea_analytics import get_nordea_analytics_client


parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers()


def get_time_series(args: Namespace) -> None:
    """Retrieves time series."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        na_service = get_nordea_analytics_client(client_id=args.client_id,
                                                 client_secret=args.client_secret)

        current_path = os.getcwd()
        with open(f'{current_path}/output_time_series.csv', 'w', newline='') as file:
            try:
                df: DataFrame = na_service.get_time_series(symbols=args.symbols.split(','),
                                                           keyfigures=args.keyfigures.split(','),
                                                           from_date=datetime.strptime(args.from_date, '%Y%m%d').date(),
                                                           to_date=datetime.strptime(args.to_date, '%Y%m%d').date(),
                                                           as_df=True)

                no_rows = df.shape[0]
                if no_rows == 0:
                    raise Exception('No data was returned')

                writer = csv.writer(file)
                writer.writerow(df)
                for x in range(no_rows):
                    writer.writerow(df.iloc[x])
            except Exception as ex:
                ex_msg = ex.args[0] if len(ex.args) > 0 else ''
                with open(f'{current_path}/diagnostics.txt', 'w') as diagnostics_file:
                    diagnostic: str = na_service.dump_diagnostic()
                    diagnostics_file.write(f'Request failed\n{ex_msg}\nSee diagnostics below\n')
                    diagnostics_file.write(diagnostic)

        if len(w) > 0:
            with open(f'{current_path}/warnings.txt', 'w') as warning_file:
                for x in range(len(w)):
                    warning_file.write(w[x].message.args[0] + '\n')


time_series_parser = subparsers.add_parser('get_time_series')
time_series_parser.add_argument('client_id')
time_series_parser.add_argument('client_secret')
time_series_parser.add_argument('symbols')
time_series_parser.add_argument('keyfigures')
time_series_parser.add_argument('from_date')
time_series_parser.add_argument('to_date')
time_series_parser.set_defaults(func=get_time_series)


def get_bond_key_figures(args: Namespace) -> None:
    """Retrieves bond key figures."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        na_service = get_nordea_analytics_client(client_id=args.client_id,
                                                 client_secret=args.client_secret)

        calc_date = datetime.strptime(args.calc_date, '%Y%m%d').date()
        current_path = os.getcwd()
        with open(f'{current_path}/output_bond_key_figures.csv', 'w', newline='') as file:
            try:
                df: DataFrame = na_service.get_bond_key_figures(symbols=args.symbols.split(','),
                                                                keyfigures=args.keyfigures.split(','),
                                                                calc_date=calc_date,
                                                                as_df=True)

                no_rows = df.shape[0]
                if no_rows == 0:
                    raise Exception('No data was returned')

                row_header: list = ['Symbols', 'Date']
                row_header.extend(df.columns)
                writer = csv.writer(file)
                writer.writerow(row_header)
                for x in range(no_rows):
                    row_data: list = [df.index[x], calc_date]
                    row_data.extend(df.iloc[x])
                    writer.writerow(row_data)
            except Exception as ex:
                ex_msg = ex.args[0] if len(ex.args) > 0 else ''
                with open(f'{current_path}/diagnostics.txt', 'w') as diagnostics_file:
                    diagnostic: str = na_service.dump_diagnostic()
                    diagnostics_file.write(f'Request failed\n{ex_msg}\nSee diagnostics below\n')
                    diagnostics_file.write(diagnostic)

        if len(w) > 0:
            with open(f'{current_path}/warnings.txt', 'w') as warning_file:
                for x in range(len(w)):
                    warning_file.write(w[x].message.args[0] + '\n')


bond_key_figure_parser = subparsers.add_parser('get_bond_key_figures')
bond_key_figure_parser.add_argument('client_id')
bond_key_figure_parser.add_argument('client_secret')
bond_key_figure_parser.add_argument('symbols')
bond_key_figure_parser.add_argument('keyfigures')
bond_key_figure_parser.add_argument('calc_date')
bond_key_figure_parser.set_defaults(func=get_bond_key_figures)


def get_index_composition(args: Namespace) -> None:
    """Retrieves index constituents."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        na_service = get_nordea_analytics_client(client_id=args.client_id,
                                                 client_secret=args.client_secret)

        calc_date = datetime.strptime(args.calc_date, '%Y%m%d').date()
        current_path = os.getcwd()
        with open(f'{current_path}/output_index_composition.csv', 'w', newline='') as file:
            try:
                df: DataFrame = na_service.get_index_composition(indices=args.indices.split(','),
                                                                 calc_date=calc_date,
                                                                 as_df=True)

                no_rows = df.shape[0]
                if no_rows == 0:
                    raise Exception('No data was returned')

                writer = csv.writer(file)
                writer.writerow(df)
                for x in range(no_rows):
                    writer.writerow(df.iloc[x])
            except Exception as ex:
                ex_msg = ex.args[0] if len(ex.args) > 0 else ''
                with open(f'{current_path}/diagnostics.txt', 'w') as diagnostics_file:
                    diagnostic: str = na_service.dump_diagnostic()
                    diagnostics_file.write(f'Request failed\n{ex_msg}\nSee diagnostics below\n')
                    diagnostics_file.write(diagnostic)

        if len(w) > 0:
            with open(f'{current_path}/warnings.txt', 'w') as warning_file:
                for x in range(len(w)):
                    warning_file.write(w[x].message.args[0] + '\n')


index_composition_parser = subparsers.add_parser('get_index_composition')
index_composition_parser.add_argument('client_id')
index_composition_parser.add_argument('client_secret')
index_composition_parser.add_argument('indices')
index_composition_parser.add_argument('calc_date')
index_composition_parser.set_defaults(func=get_index_composition)


if __name__ == '__main__':
    args = parser.parse_args()
    args.func(args)
