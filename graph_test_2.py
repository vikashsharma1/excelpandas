import matplotlib.axes as ax
import matplotlib.pyplot as plt
import pandas as pd

import os


def create_and_save_graph(df):
    # Filling NaN with 0
    df = df.fillna(0)
    # Selecting 11th row and 1st column
    df1_label = df.iloc[10:, :1]
    # Converting dataframe into list
    df1_label = df1_label.values.tolist()

    plt.gcf().subplots_adjust(bottom=0.15)
    trend_graph_path = os.path.join(os.path.dirname(__file__), 'shivam.png')
    # Select data for Grand Total Plot
    df1 = df.iloc[10:, 1:9]
    df1 = df1.iloc[0:, 1:]
    # Plot first graph
    ax = df1.T.plot(kind='line', marker='s', x_compat=True, rot=0, alpha=0.75, grid=True)
    # Data for second graph
    df = df.iloc[:10, :9]
    df = df.drop(["Category"], axis=1)
    df_label = df.iloc[:, 0]
    df_label = df_label.values.tolist()
    df = df.iloc[0:, 1:]
    ax = df.T.plot(kind='bar', width=0.9, legend=False, figsize=(60, 4),
                   ax=ax, rot=0, alpha=0.75, grid=True, colormap='Paired')

    x_offset = -0.02
    y_offset = 70.02

    for p in ax.patches:
        b = p.get_bbox()
        val = int(b.y1 + b.y0)
        ax.annotate(val, ((b.x0 + b.x1) / 2 + x_offset, b.y1 + y_offset), rotation=90)

    patches, labels = ax.get_legend_handles_labels()
    labels = df1_label + df_label
    ax.legend(patches, labels, loc='upper center', bbox_to_anchor=(
        0.5, -0.05), ncol=11, fancybox=True, shadow=True, prop={'size': 10})

    plt.savefig(trend_graph_path, bbox_inches='tight', dpi=200)


# dash_board_csv = "config/dash_board_data.csv"

# df_dash_board = pd.read_csv(dash_board_csv)


dash_board_csv = "graph_df.xlsx"
df_dash_board = pd.read_excel('graph_df.xlsx', sheet_name='GRAPH')

create_and_save_graph(df_dash_board)
