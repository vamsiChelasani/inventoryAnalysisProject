import plotly.express as px
import plotly.graph_objs as go
import pandas as pd
import glob
from plotly.subplots import make_subplots
from dash import Dash, html, dcc, callback, Output, Input

directory = 'final_data/monthly_values_companywise'

# Use glob to find the single CSV file in the directory
csv_files = glob.glob(f'{directory}/*.csv')

# Check if the CSV file is found and read it
if csv_files:
    monthly_totals_companywise_df = pd.read_csv(csv_files[0])

directory = 'final_data/monthly_values_vs_growth_rate'
csv_files = glob.glob(f'{directory}/*.csv')
if csv_files:
    monthly_totals_vs_growth_rate_df = pd.read_csv(csv_files[0])

directory = 'final_data/monthly_values_vs_moving_avg'
csv_files = glob.glob(f'{directory}/*.csv')
if csv_files:
    monthly_values_vs_moving_avg_df = pd.read_csv(csv_files[0])

directory = 'final_data/monthly_company_total_vs_moving_avg'
csv_files = glob.glob(f'{directory}/*.csv')
if csv_files:
    monthly_company_total_vs_moving_avg_df = pd.read_csv(csv_files[0])

directory = 'final_data/monthly_inventory_turnover_ratio'
csv_files = glob.glob(f'{directory}/*.csv')
if csv_files:
    monthly_inventory_turnover_ratio_df = pd.read_csv(csv_files[0])

directory = 'final_data/inventory_turnover_ratio_vs_avg_ratio'
csv_files = glob.glob(f'{directory}/*.csv')
if csv_files:
    inventory_ratio_vs_avg_df = pd.read_csv(csv_files[0])

directory = 'final_data/days_sales_inventory'
csv_files = glob.glob(f'{directory}/*.csv')
if csv_files:
    dsi_df = pd.read_csv(csv_files[0])


'''
***********************************************************************
*                      DAYS SALES OF INVENTORY                        *
***********************************************************************
'''

min_dsi = dsi_df['days_sales_of_inventory'].min()
if min_dsi <= 0:
    dsi_df['days_sales_of_inventory_adjusted'] = dsi_df['days_sales_of_inventory'] + abs(min_dsi) + 1
else:
    dsi_df['days_sales_of_inventory_adjusted'] = dsi_df['days_sales_of_inventory']


# Create scatter plot
dsi_scatter_fig = px.scatter(
    dsi_df,
    x='item',
    y='days_sales_of_inventory',
    color='company_name',
    size='days_sales_of_inventory_adjusted',  # Use adjusted DSI for size
    hover_name='company_name',
    title='Days Sales of Inventory by Item and Company',
    labels={'days_sales_of_inventory': 'Days Sales of Inventory', 'item': 'Item'}
)

# Update layout for better readability
dsi_scatter_fig.update_layout(
    xaxis_title='Item',
    yaxis_title='Days Sales of Inventory',
    xaxis_tickangle=-45,
    height=600
)

'''
***********************************************************************
*     INVENTORY TURNOVER RATIO vs AVERAGE INVENTORY TURNOVER RATIO    *
***********************************************************************
'''

inventory_vs_avg_fig = make_subplots(specs=[[{"secondary_y": True}]])

# Add traces for each company
companies = inventory_ratio_vs_avg_df['company_name'].unique()
for company in companies:
    company_data = inventory_ratio_vs_avg_df[inventory_ratio_vs_avg_df['company_name'] == company]
    
    inventory_vs_avg_fig.add_trace(
        go.Scatter(
            x=company_data['inventory_month'],
            y=company_data['inventory_turnover_ratio'],
            name=f'Inventory Turnover Ratio - {company}',
            mode='lines+markers',
            line=dict(color='blue'),
            visible=company == companies[0]
        ),
        secondary_y=False,
    )
    
    inventory_vs_avg_fig.add_trace(
        go.Scatter(
            x=company_data['inventory_month'],
            y=company_data['avg_inventory_turnover_ratio'],
            name=f'Avg Inventory Turnover Ratio - {company}',
            mode='lines+markers',
            line=dict(color='green'),
            visible=company == companies[0]
        ),
        secondary_y=True,
    )

# Update layout for buttons
buttons = []
for company in companies:
    button_visibility = [company in trace.name for trace in inventory_vs_avg_fig.data]
    
    buttons.append(
        dict(
            label=company,
            method="update",
            args=[{"visible": button_visibility}]
        )
    )

inventory_vs_avg_fig.update_layout(
    updatemenus=[dict(
        active=0,
        buttons=buttons,
        x=1.15,
        y=1.15
    )],
    title_text="Inventory Turnover Ratios Over Time by Company",
    xaxis_title="Inventory Month",
    yaxis_title="Inventory Turnover Ratio",
    yaxis2_title="Average Inventory Turnover Ratio"
)

'''
***********************************************************************
*          MONTHLY INVENTORY COMPANY WISE vs MOVING AVERAGES          *
***********************************************************************
'''

monthly_inv_fig = make_subplots(specs=[[{"secondary_y": True}]])

# Add traces for each company
companies = monthly_company_total_vs_moving_avg_df['company_name'].unique()
for company in companies:
    company_data = monthly_company_total_vs_moving_avg_df[monthly_company_total_vs_moving_avg_df['company_name'] == company]
    
    monthly_inv_fig.add_trace(
        go.Bar(
            x=company_data['inventory_month'],
            y=company_data['total_opening_value'],
            name='Total Opening Value',
            marker_color='blue',
            visible=(company == companies[0])
        ),
        secondary_y=False,
    )
    
    monthly_inv_fig.add_trace(
        go.Scatter(
            x=company_data['inventory_month'],
            y=company_data['avg_opening_value'],
            name='Avg Opening Value',
            mode='lines+markers',
            line=dict(color='blue'),
            visible=(company == companies[0])
        ),
        secondary_y=True,
    )
    
    monthly_inv_fig.add_trace(
        go.Bar(
            x=company_data['inventory_month'],
            y=company_data['total_receipt_value'],
            name='Total Receipt Value',
            marker_color='green',
            visible=(company == companies[0])
        ),
        secondary_y=False,
    )
    
    monthly_inv_fig.add_trace(
        go.Scatter(
            x=company_data['inventory_month'],
            y=company_data['avg_receipt_value'],
            name='Avg Receipt Value',
            mode='lines+markers',
            line=dict(color='green'),
            visible=(company == companies[0])
        ),
        secondary_y=True,
    )
    
    monthly_inv_fig.add_trace(
        go.Bar(
            x=company_data['inventory_month'],
            y=company_data['total_issue_value'],
            name='Total Issue Value',
            marker_color='red',
            visible=(company == companies[0])
        ),
        secondary_y=False,
    )
    
    monthly_inv_fig.add_trace(
        go.Scatter(
            x=company_data['inventory_month'],
            y=company_data['avg_issue_value'],
            name='Avg Issue Value',
            mode='lines+markers',
            line=dict(color='red'),
            visible=(company == companies[0])
        ),
        secondary_y=True,
    )
    
    monthly_inv_fig.add_trace(
        go.Bar(
            x=company_data['inventory_month'],
            y=company_data['total_closing_value'],
            name='Total Closing Value',
            marker_color='purple',
            visible=(company == companies[0])
        ),
        secondary_y=False,
    )
    
    monthly_inv_fig.add_trace(
        go.Scatter(
            x=company_data['inventory_month'],
            y=company_data['avg_closing_value'],
            name='Avg Closing Value',
            mode='lines+markers',
            line=dict(color='purple'),
            visible=(company == companies[0])
        ),
        secondary_y=True,
    )

# Update layout for buttons
buttons = []
for company in companies:
    button_visibility = [False] * len(monthly_inv_fig.data)
    for i in range(len(companies)):
        button_visibility[i * 8:(i + 1) * 8] = [company == companies[i]] * 8
    
    buttons.append(
        dict(
            label=company,
            method="update",
            args=[{"visible": button_visibility}]
        )
    )

monthly_inv_fig.update_layout(
    updatemenus=[dict(
        active=0,
        buttons=buttons,
        x=1.15,
        y=1.15
    )],
    title_text="Inventory Values Over Time by Company",
    xaxis_title="Inventory Month",
    yaxis_title="Total Value",
    yaxis2_title="Average Value"
)

'''
***********************************************************************
*                   MONTHLY INVENTORY TURNOVER RATIO                  *
***********************************************************************
'''

inv_ratio_fig = go.Figure()

inv_ratio_fig.add_trace(go.Scatter(
    x=monthly_inventory_turnover_ratio_df['inventory_month'],
    y=monthly_inventory_turnover_ratio_df['inventory_turnover_ratio'],
    name='Inventory Turnover Ratio',
    mode='lines+markers',
    line=dict(color='#9d4edd')
))

# Add a horizontal line at y=1
inv_ratio_fig.add_shape(
    type='line',
    x0=monthly_inventory_turnover_ratio_df['inventory_month'].min(),
    y0=1,
    x1=monthly_inventory_turnover_ratio_df['inventory_month'].max(),
    y1=1,
    line=dict(color='red', width=2, dash='dash'),
    name='Threshold Line'
)

# Add titles and labels
inv_ratio_fig.update_layout(
    title='Inventory Turnover Ratio Over Time',
    xaxis_title='Inventory Month',
    yaxis_title='Inventory Turnover Ratio'
)

'''
***********************************************************************
*                    MONTHLY INVENTORY COMPANY WISE                   *
***********************************************************************
'''

df_opening = monthly_totals_companywise_df[['company_name', 'inventory_month', 'opening_value']].copy()
df_receipt = monthly_totals_companywise_df[['company_name', 'inventory_month', 'receipt_value']].copy()
df_issue = monthly_totals_companywise_df[['company_name', 'inventory_month', 'issue_value']].copy()
df_closing = monthly_totals_companywise_df[['company_name', 'inventory_month', 'closing_value']].copy()

# Function to create bubble chart
def create_bubble_chart(df, value_column, title):
    monthly_totals_companywise_df['value_size'] = monthly_totals_companywise_df[value_column].abs()
    monthly_totals_companywise_df['value_size'] = monthly_totals_companywise_df['value_size'] / monthly_totals_companywise_df['value_size'].max() * 100
    fig = px.scatter(monthly_totals_companywise_df, x='inventory_month', y=value_column, size='value_size', color='company_name',
                     title=title, labels={value_column: 'Value', 'inventory_month': 'Inventory Month'},
                     hover_name='company_name', height=600)
    fig.update_layout(
        xaxis_title='Inventory Month',
        yaxis_title='Value',
        legend_title='Company',
        hovermode='closest'
    )
    return fig

# Create bubble charts
fig_opening = create_bubble_chart(df_opening, 'opening_value', 'Opening Values Over Time by Company')
fig_receipt = create_bubble_chart(df_receipt, 'receipt_value', 'Receipt Values Over Time by Company')
fig_issue = create_bubble_chart(df_issue, 'issue_value', 'Issue Values Over Time by Company')
fig_closing = create_bubble_chart(df_closing, 'closing_value', 'Closing Values Over Time by Company')

'''
***********************************************************************
*                    MONTHLY TOTALS vs GROWTH RATE                    *
***********************************************************************
'''

# Create bar charts for total values
bar_total_opening = go.Bar(
    x=monthly_totals_vs_growth_rate_df['inventory_month'],
    y=monthly_totals_vs_growth_rate_df['total_opening_value'],
    name='Total Opening Value',
    marker_color='#f7d6e0'
)
bar_total_receipt = go.Bar(
    x=monthly_totals_vs_growth_rate_df['inventory_month'],
    y=monthly_totals_vs_growth_rate_df['total_receipt_value'],
    name='Total Receipt Value',
    marker_color='#89c2d9'
)
bar_total_issue = go.Bar(
    x=monthly_totals_vs_growth_rate_df['inventory_month'],
    y=monthly_totals_vs_growth_rate_df['total_issue_value'],
    name='Total Issue Value',
    marker_color='#c7f9cc'
)
bar_total_closing = go.Bar(
    x=monthly_totals_vs_growth_rate_df['inventory_month'],
    y=monthly_totals_vs_growth_rate_df['total_closing_value'],
    name='Total Closing Value',
    marker_color='#f7c59f'
)

# Create line charts for MoM growth percentages
line_growth_ov = go.Scatter(
    x=monthly_totals_vs_growth_rate_df['inventory_month'],
    y=monthly_totals_vs_growth_rate_df['MoM_growth_percent_ov'],
    name='MoM Growth Opening Value',
    mode='lines+markers',
    yaxis='y2',
    line=dict(color='#d62728')
)
line_growth_rv = go.Scatter(
    x=monthly_totals_vs_growth_rate_df['inventory_month'],
    y=monthly_totals_vs_growth_rate_df['MoM_growth_percent_rv'],
    name='MoM Growth Receipt Value',
    mode='lines+markers',
    yaxis='y2',
    line=dict(color='#3a2e39')
)
line_growth_iv = go.Scatter(
    x=monthly_totals_vs_growth_rate_df['inventory_month'],
    y=monthly_totals_vs_growth_rate_df['MoM_growth_percent_iv'],
    name='MoM Growth Issue Value',
    mode='lines+markers',
    yaxis='y2',
    line=dict(color='#7316ba')
)
line_growth_cv = go.Scatter(
    x=monthly_totals_vs_growth_rate_df['inventory_month'],
    y=monthly_totals_vs_growth_rate_df['MoM_growth_percent_cv'],
    name='MoM Growth Closing Value',
    mode='lines+markers',
    yaxis='y2',
    line=dict(color='#227c9d')
)

# Combine the charts
growth_rate_fig = go.Figure(data=[
    bar_total_opening, bar_total_receipt, bar_total_issue, bar_total_closing,
    line_growth_ov, line_growth_rv, line_growth_iv, line_growth_cv
])

# Update layout
growth_rate_fig.update_layout(
    title='Total Values and MoM Growth Percentages Over Time',
    xaxis=dict(title='Inventory Month'),
    yaxis=dict(title='Total Value', tickformat='digits'),
    yaxis2=dict(
        title='MoM Growth Percentage',
        overlaying='y',
        side='right',
        tickformat='.2f',
        title_standoff=30  # Increase this value to move the secondary y-axis title further from the plot
    ),
    barmode='group',
    legend=dict(
        x=0.5, y=-0.2,  # Adjust position to place the legend below the chart
        xanchor='center',
        orientation='h',  # Make the legend horizontal
        bordercolor="Black",
        borderwidth=1
    )
)

'''
***********************************************************************
*                 MONTHLY INVENTORY vs MOVING AVERAGES                *
***********************************************************************
'''
bar_total_opening = go.Bar(
    x=monthly_values_vs_moving_avg_df['inventory_month'],
    y=monthly_values_vs_moving_avg_df['total_opening_value'],
    name='Total Opening Value',
    marker_color='#219ebc'
)
bar_total_receipt = go.Bar(
    x=monthly_values_vs_moving_avg_df['inventory_month'],
    y=monthly_values_vs_moving_avg_df['total_receipt_value'],
    name='Total Receipt Value',
    marker_color='#023047'
)
bar_total_issue = go.Bar(
    x=monthly_values_vs_moving_avg_df['inventory_month'],
    y=monthly_values_vs_moving_avg_df['total_issue_value'],
    name='Total Issue Value',
    marker_color='#ffb703'
)
bar_total_closing = go.Bar(
    x=monthly_values_vs_moving_avg_df['inventory_month'],
    y=monthly_values_vs_moving_avg_df['total_closing_value'],
    name='Total Closing Value',
    marker_color='#fb8500'
)

# Create line charts for average values
line_avg_opening = go.Scatter(
    x=monthly_values_vs_moving_avg_df['inventory_month'],
    y=monthly_values_vs_moving_avg_df['avg_opening_value'],
    name='Avg Opening Value',
    mode='lines+markers',
    line=dict(color='#e63946')
)
line_avg_receipt = go.Scatter(
    x=monthly_values_vs_moving_avg_df['inventory_month'],
    y=monthly_values_vs_moving_avg_df['avg_receipt_value'],
    name='Avg Receipt Value',
    mode='lines+markers',
    line=dict(color='#378c37')
)
line_avg_issue = go.Scatter(
    x=monthly_values_vs_moving_avg_df['inventory_month'],
    y=monthly_values_vs_moving_avg_df['avg_issue_value'],
    name='Avg Issue Value',
    mode='lines+markers',
    line=dict(color='#9d4edd')
)
line_avg_closing = go.Scatter(
    x=monthly_values_vs_moving_avg_df['inventory_month'],
    y=monthly_values_vs_moving_avg_df['avg_closing_value'],
    name='Avg Closing Value',
    mode='lines+markers',
    line=dict(color='purple')
)

# Combine the charts
monthly_totals_vs_avg_fig = go.Figure(data=[
    bar_total_opening, bar_total_receipt, bar_total_issue, bar_total_closing,
    line_avg_opening, line_avg_receipt, line_avg_issue, line_avg_closing
])

# Update layout
monthly_totals_vs_avg_fig.update_layout(
    title='Total value and Average Values Over Time',
    xaxis=dict(title='Inventory Month'),
    yaxis=dict(title='Value', tickformat='digits'),
    barmode='group'
)

# Set up the Dash application
app = Dash(__name__)
server=app.server

app.layout = html.Div([
    html.H1("Inventory Dashboard"),

    dcc.Graph(id='dsi-scatter-plot', figure=dsi_scatter_fig),
    dcc.Graph(id='inventory-vs-avg-line-plot', figure=inventory_vs_avg_fig),
    dcc.Graph(id='monthly-inv-line-plot', figure=monthly_inv_fig),
    dcc.Graph(id='inv-line-plot', figure=inv_ratio_fig),
    dcc.Graph(id='growth-rate-line-plot', figure=growth_rate_fig),
    dcc.Graph(id='monthly-bar-plot', figure=monthly_totals_vs_avg_fig),
    dcc.Graph(id='opening-bubble-plot', figure=fig_opening),
    dcc.Graph(id='receipt-bubble-plot', figure=fig_receipt),
    dcc.Graph(id='issue-bubble-plot', figure=fig_issue),
    dcc.Graph(id='closing-bubble-plot', figure=fig_closing),

])

if __name__ == '__main__':
    app.run_server(debug=True)
