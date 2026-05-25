import plotly.graph_objects as go


def empty_fig(message: str) -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        template="plotly_dark",
        annotations=[{
            "text": message,
            "showarrow": False,
            "font": {"size": 16},
            "xref": "paper", "yref": "paper",
            "x": 0.5, "y": 0.5,
        }],
    )
    return fig
