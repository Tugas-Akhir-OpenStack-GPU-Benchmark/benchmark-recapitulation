# Taken from https://towardsdatascience.com/make-your-tables-look-glorious-2a5ddbfcc0e5
import pandas as pd
import dataframe_image as dfi
from matplotlib import pyplot as plt, image as mpimg


def highlight_product(series, colour ='yellow'):
    r = pd.Series(data = False, index = series.index)

    return [f'background-color: {colour}' if series.name[-1] % 2 == 0 else '' for v in r]

def format_value(value):
    if pd.isna(value):
        return ""
    elif isinstance(value, str):
        return value
    elif isinstance(value, (int, float)) and value == int(value):
        return f"{int(value)}"  # No decimal places for integers
    else:
        return f"{value:.3f}"  # Two decimal places for floats


def export_pandas_to_png(df: pd.DataFrame, filename: str, title:str, hide_index=False, border=False, alternating_row_colors=True):
    df.insert(0, 'index', range(len(df)))
    df.set_index('index', inplace=True, drop=True, append=True)
    d_styled = (
        df.style
        .format(format_value)
        .set_table_styles([
            # left-align the first column, and right-align the remaining one. Include column header as well
            {'selector': 'th.col0', 'props': [('text-align', 'left')]},  # Left-align the first column header
            {'selector': 'td.col0', 'props': [('text-align', 'left')]},  # Left-align the first column cells
            {'selector': 'th:not(.col0)', 'props': [('text-align', 'right')]},  # Right-align the remaining column headers
            {'selector': 'td:not(.col0)', 'props': [('text-align', 'right')]}   # Right-align the remaining column cells
        ])
    )
    if alternating_row_colors:
        d_styled = d_styled.apply(highlight_product, colour = '#DDEBF7', axis = 1)
    d_styled = d_styled.hide(subset=None, level=['index'] if not hide_index else None, names=not hide_index)  # hide "index" index from png output
    if border:
        d_styled = (
            d_styled.pipe(add_horizontal_borders_for_index)
                    .apply(lambda x: add_horizontal_borders(df), axis=None)  # Apply horizontal borders
                    # .apply(lambda x: add_vertical_borders(df), axis=None)
        )
    dfi.export(
        d_styled,
        filename,
    )
    add_title_to_existing_file(filename, title)


def add_title_to_existing_file(filename, title):
    img = mpimg.imread(filename)
    fig, ax = plt.subplots()
    ax.imshow(img)
    ax.axis('off')

    plt.title(title, fontsize=14, pad=10)
    plt.savefig(filename, transparent=True, bbox_inches='tight', pad_inches=0.05)
    plt.close()


def add_horizontal_borders_for_index(styler):
    styler.applymap_index(lambda v: 'border-top: 2px solid black;', level=0, axis=0)
    return styler


def add_horizontal_borders(df):
    styles = pd.DataFrame("", index=df.index, columns=df.columns)
    for i in range(1, len(df)):
        if df.index[i][0] != df.index[i - 1][0]:
            styles.iloc[i, :] = 'border-top: 2px solid black;'
    return styles

def add_vertical_borders(df):
    styles = pd.DataFrame("", index=df.index, columns=df.columns)
    for i in range(len(df)):
        for j in range(len(df.columns)):
            if j == 0:
                styles.iloc[i, j] += 'border-left: 2px solid black;'
            if df.columns[j] == df.columns[-1]:
                styles.iloc[i, j] += 'border-right: 2px solid black;'
            if i == 0:
                styles.iloc[i, j] += 'border-top: 2px solid black;'
    return styles
