import pandas as pd
import pytest


def load_and_compare_dfs(
    df: pd.DataFrame,
    expected_df_file_path: str,
    index_col: float = None,
    check_order: bool = True,
    header: str = "infer",
    tolerance: float = 1e-7,
    dump_data: bool = False,
    reset_index: bool = False,
) -> pytest:
    """Load the expected DataFrame and compares to expected results.

    Args:
        df: DataFrame from the test.
        expected_df_file_path: Path where the expected DataFrame is saved.
        index_col: column number for the index.
        check_order: boolean if should check order of columns.
        header: DataFrame format.
        tolerance: Difference tolerance in values.
        dump_data: Boolean if the expected DataFrame should.
            be dumped and "df" should be saved as the expected DataFrame instead.
        reset_index: Boolean if index should be reset. If the index has repeated
            values it can cause problems in the assertion.

    Returns: Several assert statements which check if the returned
            DataFrame from the service matches the expected result DataFrame.

    """
    if dump_data:
        df.to_csv(expected_df_file_path)  # used to generate new files
    expected_df = pd.read_csv(
        expected_df_file_path,
        index_col=index_col,
        header=header,
        float_precision="high",
    )
    if reset_index:
        df = df.reset_index(drop=True)
        expected_df = expected_df.reset_index(drop=True)
    # need to set some of the attributes to that of the
    # generated one for the comparision to work
    if len(df.index):
        df.index = df.index.astype(str)
        expected_df.index = expected_df.index.astype(str)

    if not isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.astype(str)

    # Check columns
    # First check sorted
    col_1 = expected_df.columns.tolist()
    col_2 = df.columns.tolist()
    col_1.sort(), col_2.sort()
    assert col_1 == col_2, "Columns doesn't contain the same elements."

    # Check unsorted
    if check_order:
        col_1 = expected_df.columns.tolist()
        col_2 = df.columns.tolist()
        assert col_1 == col_2, "Columns not in order."

    # Chceck rows
    row_1 = expected_df.index.tolist()
    row_2 = df.index.tolist()
    row_1.sort(), row_2.sort()
    assert row_1 == row_2, "Index doesn't contain the same elements."

    # Check unsorted
    if check_order:
        row_1 = expected_df.index.tolist()
        row_2 = df.index.tolist()
        assert row_1 == row_2, "Index not in order"

    # check values
    df = df.fillna(0.0)
    expected_df = expected_df.fillna(0.0)
    df = df.fillna(0.0)
    for r in row_1:
        for c in col_1:
            assert df.loc[r, c] == pytest.approx(
                expected_df.loc[r, c], abs=tolerance
            ), (r, c, f"Expected: {expected_df.loc[r, c]}, Actual: {df.loc[r, c]}")
